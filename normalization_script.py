"""
Automate SQL queries on the "gp_02" MySQL database.

Instructions:
1. Ensure you have a .env file with the variable `PASSWORD` set to your MySQL password.
2. Install any dependencies (e.g. `pip install python-dotenv mysql-connector-python`) if necessary.
3. Run this Python script. It will connect to localhost with user "root" and database "gp_02",
   then execute the SQL queries in sequence.
4. Note: The `CREATE TABLE company_raw` is intentionally omitted (already assumed to exist).
"""

import os
import time
import mysql.connector
from dotenv import load_dotenv


def log_step(message):
    """Log migration steps with timestamp"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"[{timestamp}] {message}")


def run_migration():
    # Load environment variables from .env file
    load_dotenv()

    # Read the password from an environment variable named PASSWORD
    db_password = os.getenv("PASSWORD")
    if not db_password:
        raise ValueError("Database password not found in environment variables.")

    log_step("Starting database migration")

    # Connect to MySQL
    try:
        connection = mysql.connector.connect(
            host="localhost", user="root", password=db_password, database="gp_02"
        )
        cursor = connection.cursor()
        log_step("Database connection established")
    except mysql.connector.Error as err:
        log_step(f"ERROR: Database connection failed: {err}")
        return

    try:
        # Phase 1: Initial Schema Setup
        log_step("PHASE 1: Initial Schema Setup")

        # Rename staging table to final table name
        log_step("Renaming raw table to company")
        cursor.execute("ALTER TABLE company_raw RENAME TO company")

        # Add primary key and columns for company size
        log_step("Adding primary key and company size columns")
        cursor.execute(
            "ALTER TABLE company ADD COLUMN company_id INT AUTO_INCREMENT PRIMARY KEY FIRST"
        )
        cursor.execute(
            """
            ALTER TABLE company
            ADD COLUMN company_size_min VARCHAR(50) AFTER company_size,
            ADD COLUMN company_size_max VARCHAR(50) AFTER company_size_min
        """
        )
        connection.commit()

        # Phase 2: Create Dimension and Junction Tables
        log_step("PHASE 2: Creating dimension and junction tables")

        # Specialty tables
        log_step("Creating specialty tables")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS specialty (
                specialty_name_id INT AUTO_INCREMENT PRIMARY KEY,
                specialty_name VARCHAR(255) NOT NULL
            ) ENGINE=INNODB
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS company_specialty (
                unique_id INT AUTO_INCREMENT PRIMARY KEY,
                company_id INT NOT NULL,
                specialty_name_id INT NOT NULL,
                FOREIGN KEY (company_id) REFERENCES company(company_id),
                FOREIGN KEY (specialty_name_id) REFERENCES specialty(specialty_name_id),
                UNIQUE (company_id, specialty_name_id)
            ) ENGINE=INNODB
        """
        )

        # Type tables
        log_step("Creating company type tables")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS type (
                company_type_id INT AUTO_INCREMENT PRIMARY KEY,
                company_type_name VARCHAR(255) NOT NULL
            ) ENGINE=INNODB
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS company_type (
                unique_id INT AUTO_INCREMENT PRIMARY KEY,
                company_id INT NOT NULL,
                company_type_id INT NOT NULL,
                FOREIGN KEY (company_id) REFERENCES company(company_id),
                FOREIGN KEY (company_type_id) REFERENCES type(company_type_id),
                UNIQUE (company_id, company_type_id)
            ) ENGINE=INNODB
        """
        )

        # Industry tables
        log_step("Creating industry tables")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS industry (
                industry_id INT AUTO_INCREMENT PRIMARY KEY,
                industry_name VARCHAR(255) NOT NULL
            ) ENGINE=INNODB
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS industry_type (
                unique_id INT AUTO_INCREMENT PRIMARY KEY,
                company_id INT NOT NULL,
                industry_id INT NOT NULL,
                FOREIGN KEY (company_id) REFERENCES company(company_id),
                FOREIGN KEY (industry_id) REFERENCES industry(industry_id),
                UNIQUE (company_id, industry_id)
            ) ENGINE=INNODB
        """
        )

        # Locations table
        log_step("Creating locations table")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS locations (
                locations_id INT AUTO_INCREMENT PRIMARY KEY,
                company_id INT NOT NULL,
                country VARCHAR(255),
                city VARCHAR(255),
                postal_code VARCHAR(50),
                address_line1 VARCHAR(500),
                is_hq BOOLEAN DEFAULT FALSE,
                state VARCHAR(255),
                FOREIGN KEY (company_id) REFERENCES company(company_id)
            ) ENGINE=INNODB
        """
        )

        # Company updates and relationships tables
        log_step("Creating tables for updates and relationships")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS company_updates (
                update_id INT AUTO_INCREMENT PRIMARY KEY,
                company_id INT NOT NULL,
                article_link VARCHAR(500) NOT NULL,
                image VARCHAR(500) NOT NULL, 
                posted_on DATE NOT NULL,
                update_text TEXT NOT NULL,
                total_likes INT NOT NULL,
                FOREIGN KEY (company_id) REFERENCES company(company_id),
                UNIQUE (update_id, company_id)
            ) ENGINE=INNODB
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS affiliated_companies (
                affiliated_companies_id INT AUTO_INCREMENT PRIMARY KEY,
                company_id INT NOT NULL,
                name VARCHAR(500) NOT NULL, 
                linkedin_url VARCHAR(500) NOT NULL,
                industry VARCHAR(500) NOT NULL,
                location VARCHAR(500) NOT NULL,
                FOREIGN KEY (company_id) REFERENCES company(company_id),
                UNIQUE (affiliated_companies_id, company_id)
            ) ENGINE=INNODB
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS similar_companies (
                similar_companies_id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(500) NOT NULL, 
                linkedin_url VARCHAR(500) NOT NULL,
                industry VARCHAR(500) NOT NULL,
                location VARCHAR(500) NOT NULL
            ) ENGINE=INNODB
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS similar_companies_junction (
                unique_id INT AUTO_INCREMENT PRIMARY KEY,
                similar_companies_id INT NOT NULL,
                company_id INT NOT NULL,
                FOREIGN KEY (company_id) REFERENCES company(company_id),
                FOREIGN KEY (similar_companies_id) REFERENCES similar_companies(similar_companies_id),
                UNIQUE (company_id, similar_companies_id)
            ) ENGINE=INNODB
        """
        )
        connection.commit()

        # Phase 3: Data Transformation and Loading
        log_step("PHASE 3: Data transformation and loading")

        # Parse company size min/max
        log_step("Parsing company size min/max values")
        cursor.execute(
            """
            INSERT INTO company (company_size_min, company_size_max)
            SELECT 
                JSON_EXTRACT(company_size, '$[0]') AS min_value,
                JSON_EXTRACT(company_size, '$[1]') AS max_value
            FROM company
        """
        )

        # Extract specialty data
        log_step("Extracting and loading specialty data")
        cursor.execute(
            """
            INSERT INTO specialty (specialty_name)
            SELECT DISTINCT TRIM(jt.specialty)
            FROM company cr
            JOIN JSON_TABLE(
                cr.specialities,
                '$[*]'
                COLUMNS (
                   specialty VARCHAR(255) PATH '$'
                )
            ) AS jt
            WHERE cr.specialities IS NOT NULL
              AND TRIM(jt.specialty) <> ''
            ON DUPLICATE KEY UPDATE specialty_name = specialty_name
        """
        )

        cursor.execute(
            """
            INSERT INTO company_specialty (company_id, specialty_name_id)
            SELECT cr.company_id, s.specialty_name_id
            FROM company cr
            JOIN JSON_TABLE(
                cr.specialities,
                '$[*]'
                COLUMNS (
                   specialty VARCHAR(255) PATH '$'
                )
            ) AS jt
            JOIN specialty s ON s.specialty_name = TRIM(jt.specialty)
            WHERE cr.specialities IS NOT NULL
              AND TRIM(jt.specialty) <> ''
            ON DUPLICATE KEY UPDATE specialty_name_id = s.specialty_name_id
        """
        )

        # Extract company type data
        log_step("Extracting and loading company type data")
        cursor.execute(
            """
            INSERT INTO type (company_type_name)
            SELECT DISTINCT TRIM(company_type) AS company_type_name
            FROM company
            WHERE company_type IS NOT NULL
              AND TRIM(company_type) <> ''
            ON DUPLICATE KEY UPDATE company_type_name = company_type_name
        """
        )

        cursor.execute(
            """
            INSERT INTO company_type (company_id, company_type_id)
            SELECT cr.company_id, t.company_type_id
            FROM company cr
            JOIN type t ON t.company_type_name = TRIM(cr.company_type)
            WHERE cr.company_type IS NOT NULL
              AND TRIM(cr.company_type) <> ''
            ON DUPLICATE KEY UPDATE company_type_id = t.company_type_id
        """
        )

        # Extract industry data
        log_step("Extracting and loading industry data")
        cursor.execute(
            """
            INSERT INTO industry (industry_name)
            SELECT DISTINCT TRIM(industry) AS industry_name
            FROM company
            WHERE industry IS NOT NULL
              AND TRIM(industry) <> ''
            ON DUPLICATE KEY UPDATE industry_name = industry_name
        """
        )

        cursor.execute(
            """
            INSERT INTO industry_type (company_id, industry_id)
            SELECT cr.company_id, i.industry_id
            FROM company cr
            JOIN industry i ON i.industry_name = TRIM(cr.industry)
            WHERE cr.industry IS NOT NULL
              AND TRIM(cr.industry) <> ''
            ON DUPLICATE KEY UPDATE industry_id = i.industry_id
        """
        )

        # Extract location data
        log_step("Extracting and loading location data")
        cursor.execute(
            """
            INSERT INTO locations (company_id, country, city, postal_code, address_line1, is_hq, state)
            SELECT 
                cr.company_id,
                TRIM(jt.country) AS country,
                TRIM(jt.city) AS city,
                TRIM(jt.postal_code) AS postal_code,
                TRIM(jt.line_1) AS address_line1,
                CASE 
                    WHEN TRIM(jt.is_hq) IN ('true', '1') THEN TRUE 
                    ELSE FALSE 
                END AS is_hq,
                TRIM(jt.state) AS state
            FROM company cr
            JOIN JSON_TABLE(
                cr.locations,
                '$[*]' 
                COLUMNS (
                   country     VARCHAR(255) PATH '$.country',
                   city        VARCHAR(255) PATH '$.city',
                   postal_code VARCHAR(50)  PATH '$.postal_code',
                   line_1      VARCHAR(500) PATH '$.line_1',
                   is_hq       VARCHAR(10)  PATH '$.is_hq',
                   state       VARCHAR(255) PATH '$.state'
                )
            ) AS jt
            WHERE cr.locations IS NOT NULL
              AND TRIM(jt.country) <> ''
        """
        )

        # Extract company update data
        log_step("Extracting and loading company update data")
        cursor.execute(
            """
            INSERT INTO company_updates (company_id, article_link, image, posted_on, update_text, total_likes)
            SELECT 
                cr.company_id,
                COALESCE(jt.article_link, 'No Link Provided') AS article_link,
                COALESCE(jt.image, '') AS image,
                STR_TO_DATE(
                  CONCAT(
                      COALESCE(jt.year, 1900), '-', 
                      LPAD(COALESCE(jt.month, 1), 2, '0'), '-', 
                      LPAD(COALESCE(jt.day, 1), 2, '0')
                  ), '%Y-%m-%d'
                ) AS posted_on,
                COALESCE(jt.text, '') AS update_text,
                COALESCE(jt.total_likes, 0) AS total_likes
            FROM company cr
            JOIN JSON_TABLE(
                cr.updates,
                '$[*]'
                COLUMNS (
                  article_link VARCHAR(500) PATH '$.article_link',
                  image        VARCHAR(500) PATH '$.image',
                  day          INT PATH '$.posted_on.day',
                  month        INT PATH '$.posted_on.month',
                  year         INT PATH '$.posted_on.year',
                  text         TEXT PATH '$.text',
                  total_likes  INT PATH '$.total_likes'
                )
            ) AS jt
            WHERE cr.updates IS NOT NULL
        """
        )

        # Extract affiliated companies data
        log_step("Extracting and loading affiliated companies data")
        cursor.execute(
            """
            INSERT INTO affiliated_companies (company_id, name, linkedin_url, industry, location)
            SELECT 
                cr.company_id,
                COALESCE(jt.name, 'No Name Provided') AS name,
                COALESCE(jt.link, 'No Link Provided') AS linkedin_url,
                COALESCE(jt.industry, 'No Industry Provided') AS industry,
                COALESCE(jt.location, 'No Location Provided') AS location
            FROM company cr
            JOIN JSON_TABLE(
                cr.affiliated_companies,
                '$[*]'
                COLUMNS (
                    name VARCHAR(500) PATH '$.name',
                    link VARCHAR(500) PATH '$.link',
                    industry VARCHAR(500) PATH '$.industry',
                    location VARCHAR(500) PATH '$.location'
                )
            ) AS jt
            WHERE cr.affiliated_companies IS NOT NULL
        """
        )

        # Extract similar companies data
        log_step("Extracting and loading similar companies data")
        cursor.execute(
            """
            INSERT INTO similar_companies (name, linkedin_url, industry, location)
            SELECT DISTINCT
                COALESCE(TRIM(jt.name), 'No Name Provided') AS name,
                COALESCE(TRIM(jt.link), 'No Link Provided') AS linkedin_url,
                COALESCE(TRIM(jt.industry), 'No Industry Provided') AS industry,
                COALESCE(TRIM(jt.location), 'No Location Provided') AS location
            FROM company cr
            JOIN JSON_TABLE(
                cr.similar_companies,
                '$[*]'
                COLUMNS (
                   name VARCHAR(500) PATH '$.name',
                   link VARCHAR(500) PATH '$.link',
                   industry VARCHAR(500) PATH '$.industry',
                   location VARCHAR(500) PATH '$.location'
                )
            ) AS jt
            WHERE cr.similar_companies IS NOT NULL
              AND TRIM(jt.name) <> ''
        """
        )

        cursor.execute(
            """
            INSERT INTO similar_companies_junction (company_id, similar_companies_id)
            SELECT DISTINCT
                cr.company_id,
                sc.similar_companies_id
            FROM company cr
            JOIN JSON_TABLE(
                cr.similar_companies,
                '$[*]'
                COLUMNS (
                   name VARCHAR(500) PATH '$.name',
                   link VARCHAR(500) PATH '$.link',
                   industry VARCHAR(500) PATH '$.industry',
                   location VARCHAR(500) PATH '$.location'
                )
            ) AS jt
            JOIN similar_companies sc 
                ON sc.name         = COALESCE(TRIM(jt.name), 'No Name Provided')
               AND sc.linkedin_url = COALESCE(TRIM(jt.link), 'No Link Provided')
               AND sc.industry     = COALESCE(TRIM(jt.industry), 'No Industry Provided')
               AND sc.location     = COALESCE(TRIM(jt.location), 'No Location Provided')
            WHERE cr.similar_companies IS NOT NULL
              AND TRIM(jt.name) <> ''
        """
        )
        connection.commit()

        # Phase 4: Cleanup
        log_step("PHASE 4: Cleanup - removing redundant columns")
        cursor.execute(
            """
            ALTER TABLE company 
            DROP COLUMN industry,
            DROP COLUMN hq,
            DROP COLUMN company_type,
            DROP COLUMN specialities,
            DROP COLUMN locations,
            DROP COLUMN similar_companies,
            DROP COLUMN affiliated_companies,
            DROP COLUMN updates,
            DROP COLUMN company_size,
            DROP COLUMN exit_data,
            DROP COLUMN acquisitions,
            DROP COLUMN extra,
            DROP COLUMN funding_data,
            DROP COLUMN categories,
            DROP COLUMN customer_list
        """
        )

        cursor.execute("ALTER TABLE affiliated_companies DROP COLUMN location")
        cursor.execute("ALTER TABLE similar_companies DROP COLUMN location")
        connection.commit()

        log_step("Migration completed successfully")

    except mysql.connector.Error as err:
        log_step(f"ERROR: {err}")
        connection.rollback()
        log_step("Migration failed - rolling back changes")
    finally:
        cursor.close()
        connection.close()
        log_step("Database connection closed")


if __name__ == "__main__":
    print(f"Starting migration script at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Current user: {os.getenv('USER', 'unknown')}")

    try:
        run_migration()
        print("Script execution completed.")
    except Exception as e:
        print(f"Script execution failed: {e}")
