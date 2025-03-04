"""
Automate SQL queries on the "gp_02" MySQL database.

Instructions:
1. Ensure you have a .env file with the variable `PASSWROD` set to your MySQL password.
2. Install any dependencies (e.g. `pip install python-dotenv mysql-connector-python`) if necessary.
3. Run this Python script. It will connect to localhost with user "root" and database "gp_02",
   then execute the SQL queries in sequence.
4. Note: The `CREATE TABLE company_raw` is intentionally omitted (already assumed to exist).
"""

import os
import mysql.connector
from dotenv import load_dotenv

# If using a .env file, uncomment the next line after installing python-dotenv:
# from dotenv import load_dotenv


def run_migration():
    # Uncomment if you use python-dotenv to load environment variables from .env
    load_dotenv()

    # Read the password from an environment variable named PASSWROD
    db_password = os.getenv("PASSWROD")

    # Connect to MySQL
    connection = mysql.connector.connect(
        host="localhost", user="root", password=db_password, database="gp_02"
    )
    cursor = connection.cursor()

    # List of SQL statements in the exact order to be run
    # (Removing the original "CREATE TABLE company_raw" portion as requested.)
    queries = [
        # ---------------------------------------------------------
        # 1) Drop unneeded columns from "company_raw"
        # ---------------------------------------------------------
        """
        ALTER TABLE company_raw
          DROP COLUMN exit_data,
          DROP COLUMN acquisitions,
          DROP COLUMN extra,
          DROP COLUMN funding_data,
          DROP COLUMN categories,
          DROP COLUMN customer_list;
        """,
        # ---------------------------------------------------------
        # 2) Add an auto-increment primary key to "company_raw"
        # ---------------------------------------------------------
        """
        ALTER TABLE company_raw
          ADD COLUMN company_id INT AUTO_INCREMENT PRIMARY KEY FIRST;
        """,
        # ---------------------------------------------------------
        # 3) Create "specialty" dimension and "company_specialty" junction
        # ---------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS specialty (
            specialty_name_id INT AUTO_INCREMENT PRIMARY KEY,
            specialty_name VARCHAR(255) NOT NULL
        ) ENGINE=INNODB;
        """,
        """
        CREATE TABLE IF NOT EXISTS company_specialty (
            unique_id INT AUTO_INCREMENT PRIMARY KEY,
            company_id INT NOT NULL,
            specialty_name_id INT NOT NULL,
            FOREIGN KEY (company_id) REFERENCES company_raw(company_id),
            FOREIGN KEY (specialty_name_id) REFERENCES specialty(specialty_name_id),
            UNIQUE (company_id, specialty_name_id)
        ) ENGINE=INNODB;
        """,
        # ---------------------------------------------------------
        # 4) Create "type" dimension and "company_type" junction
        # ---------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS type (
            company_type_id INT AUTO_INCREMENT PRIMARY KEY,
            company_type_name VARCHAR(255) NOT NULL
        ) ENGINE=INNODB;
        """,
        """
        CREATE TABLE IF NOT EXISTS company_type (
            unique_id INT AUTO_INCREMENT PRIMARY KEY,
            company_id INT NOT NULL,
            company_type_id INT NOT NULL,
            FOREIGN KEY (company_id) REFERENCES company_raw(company_id),
            FOREIGN KEY (company_type_id) REFERENCES type(company_type_id),
            UNIQUE (company_id, company_type_id)
        ) ENGINE=INNODB;
        """,
        # ---------------------------------------------------------
        # 5) Create "industry" dimension and "industry_type" junction
        # ---------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS industry (
            industry_id INT AUTO_INCREMENT PRIMARY KEY,
            industry_name VARCHAR(255) NOT NULL
        ) ENGINE=INNODB;
        """,
        """
        CREATE TABLE IF NOT EXISTS industry_type (
            unique_id INT AUTO_INCREMENT PRIMARY KEY,
            company_id INT NOT NULL,
            industry_id INT NOT NULL,
            FOREIGN KEY (company_id) REFERENCES company_raw(company_id),
            FOREIGN KEY (industry_id) REFERENCES industry(industry_id),
            UNIQUE (company_id, industry_id)
        ) ENGINE=INNODB;
        """,
        # ---------------------------------------------------------
        # 6) Create "ranges" dimension and "company_range" junction
        # ---------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS ranges (
            range_id INT AUTO_INCREMENT PRIMARY KEY,
            range_parameter VARCHAR(255) NOT NULL
        ) ENGINE=INNODB;
        """,
        """
        CREATE TABLE IF NOT EXISTS company_range (
            unique_id INT AUTO_INCREMENT PRIMARY KEY,
            range_id INT NOT NULL,
            company_id INT NOT NULL,
            FOREIGN KEY (company_id) REFERENCES company_raw(company_id),
            FOREIGN KEY (range_id) REFERENCES ranges(range_id),
            UNIQUE (company_id, range_id)
        ) ENGINE=INNODB;
        """,
        # ---------------------------------------------------------
        # 7) Create "locations" dimension and "company_location" junction
        # ---------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS locations (
            locations_id INT AUTO_INCREMENT PRIMARY KEY,
            country VARCHAR(255),
            city VARCHAR(255),
            postal_code VARCHAR(50),
            address_line1 VARCHAR(500),
            is_hq BOOLEAN DEFAULT FALSE,
            state VARCHAR(255)
        ) ENGINE=INNODB;
        """,
        """
        CREATE TABLE IF NOT EXISTS company_location (
            unique_id INT AUTO_INCREMENT PRIMARY KEY,
            company_id INT NOT NULL,
            locations_id INT NOT NULL,
            FOREIGN KEY (company_id) REFERENCES company_raw(company_id),
            FOREIGN KEY (locations_id) REFERENCES locations(locations_id),
            UNIQUE (company_id, locations_id)
        ) ENGINE=INNODB;
        """,
        # ---------------------------------------------------------
        # 8) Create tables for updates / affiliated / similar companies
        # ---------------------------------------------------------
        """
        CREATE TABLE IF NOT EXISTS company_updates (
            update_id INT AUTO_INCREMENT PRIMARY KEY,
            company_id INT NOT NULL,
            article_link VARCHAR(500) NOT NULL,
            image VARCHAR(500) NOT NULL, 
            posted_on DATE NOT NULL,
            update_text TEXT NOT NULL,
            total_likes INT NOT NULL,
            FOREIGN KEY (company_id) REFERENCES company_raw(company_id),
            UNIQUE (update_id, company_id)
        ) ENGINE=INNODB;
        """,
        """
        CREATE TABLE IF NOT EXISTS affiliated_companies (
            affiliated_companies_id INT AUTO_INCREMENT PRIMARY KEY,
            company_id INT NOT NULL,
            name VARCHAR(500) NOT NULL, 
            linkedin_url VARCHAR(500) NOT NULL,
            industry VARCHAR(500) NOT NULL,
            location VARCHAR(500) NOT NULL,
            FOREIGN KEY (company_id) REFERENCES company_raw(company_id),
            UNIQUE (affiliated_companies_id, company_id)
        ) ENGINE=INNODB;
        """,
        """
        CREATE TABLE IF NOT EXISTS similar_companies (
            similar_companies_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(500) NOT NULL, 
            linkedin_url VARCHAR(500) NOT NULL,
            industry VARCHAR(500) NOT NULL,
            location VARCHAR(500) NOT NULL
        ) ENGINE=INNODB;
        """,
        """
        CREATE TABLE IF NOT EXISTS similar_companies_junction (
            unique_id INT AUTO_INCREMENT PRIMARY KEY,
            similar_companies_id INT NOT NULL,
            company_id INT NOT NULL,
            FOREIGN KEY (company_id) REFERENCES company_raw(company_id),
            FOREIGN KEY (similar_companies_id) REFERENCES similar_companies(similar_companies_id),
            UNIQUE (company_id, similar_companies_id)
        ) ENGINE=INNODB;
        """,
        # ---------------------------------------------------------
        # 9) Insert Data from "company_raw" into Dimension/Junction tables
        # ---------------------------------------------------------
        # 9.1) Specialties
        """
        INSERT INTO specialty (specialty_name)
        SELECT DISTINCT TRIM(jt.specialty)
        FROM company_raw cr
        JOIN JSON_TABLE(
            cr.specialities,
            '$[*]'
            COLUMNS (
               specialty VARCHAR(255) PATH '$'
            )
        ) AS jt
        WHERE cr.specialities IS NOT NULL
          AND TRIM(jt.specialty) <> ''
        ON DUPLICATE KEY UPDATE specialty_name = specialty_name;
        """,
        """
        INSERT INTO company_specialty (company_id, specialty_name_id)
        SELECT cr.company_id, s.specialty_name_id
        FROM company_raw cr
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
        ON DUPLICATE KEY UPDATE specialty_name_id = s.specialty_name_id;
        """,
        # 9.2) Company Type
        """
        INSERT INTO type (company_type_name)
        SELECT DISTINCT TRIM(company_type) AS company_type_name
        FROM company_raw
        WHERE company_type IS NOT NULL
          AND TRIM(company_type) <> ''
        ON DUPLICATE KEY UPDATE company_type_name = company_type_name;
        """,
        """
        INSERT INTO company_type (company_id, company_type_id)
        SELECT cr.company_id, t.company_type_id
        FROM company_raw cr
        JOIN type t ON t.company_type_name = TRIM(cr.company_type)
        WHERE cr.company_type IS NOT NULL
          AND TRIM(cr.company_type) <> ''
        ON DUPLICATE KEY UPDATE company_type_id = t.company_type_id;
        """,
        # 9.3) Industry
        """
        INSERT INTO industry (industry_name)
        SELECT DISTINCT TRIM(industry) AS industry_name
        FROM company_raw
        WHERE industry IS NOT NULL
          AND TRIM(industry) <> ''
        ON DUPLICATE KEY UPDATE industry_name = industry_name;
        """,
        """
        INSERT INTO industry_type (company_id, industry_id)
        SELECT cr.company_id, i.industry_id
        FROM company_raw cr
        JOIN industry i ON i.industry_name = TRIM(cr.industry)
        WHERE cr.industry IS NOT NULL
          AND TRIM(cr.industry) <> ''
        ON DUPLICATE KEY UPDATE industry_id = i.industry_id;
        """,
        # 9.4) Ranges (Company Size)
        """
        INSERT INTO ranges (range_parameter)
        SELECT DISTINCT
           CASE 
              WHEN jt.low = 0    AND jt.high = 1     THEN '0-1'
              WHEN jt.low = 2    AND jt.high = 10    THEN '2-10'
              WHEN jt.low = 11   AND jt.high = 50    THEN '11-50'
              WHEN jt.low = 51   AND jt.high = 200   THEN '51-200'
              WHEN jt.low = 201  AND jt.high = 500   THEN '201-500'
              WHEN jt.low = 501  AND jt.high = 1000  THEN '501-1000'
              WHEN jt.low = 1001 AND jt.high = 5000  THEN '1001-5000'
              WHEN jt.low = 5001 AND jt.high = 10000 THEN '5001-10000'
              WHEN jt.low = 10001 AND jt.high IS NULL THEN '10001+'
              ELSE NULL
           END AS range_parameter
        FROM company_raw cr
        JOIN JSON_TABLE(
            cr.company_size,
            '$'
            COLUMNS (
               low  INT PATH '$[0]',
               high INT PATH '$[1]'
            )
        ) AS jt
        WHERE cr.company_size IS NOT NULL
          AND jt.low IS NOT NULL
          AND (
               (jt.low = 0    AND jt.high = 1)
            OR (jt.low = 2    AND jt.high = 10)
            OR (jt.low = 11   AND jt.high = 50)
            OR (jt.low = 51   AND jt.high = 200)
            OR (jt.low = 201  AND jt.high = 500)
            OR (jt.low = 501  AND jt.high = 1000)
            OR (jt.low = 1001 AND jt.high = 5000)
            OR (jt.low = 5001 AND jt.high = 10000)
            OR (jt.low = 10001 AND jt.high IS NULL)
          );
        """,
        """
        INSERT INTO company_range (company_id, range_id)
        SELECT DISTINCT
            cr.company_id,
            r.range_id
        FROM company_raw cr
        JOIN JSON_TABLE(
            cr.company_size,
            '$'
            COLUMNS (
               low  INT PATH '$[0]',
               high INT PATH '$[1]'
            )
        ) AS jt
        JOIN ranges r 
           ON r.range_parameter = CASE 
                 WHEN jt.low = 0    AND jt.high = 1     THEN '0-1'
                 WHEN jt.low = 2    AND jt.high = 10    THEN '2-10'
                 WHEN jt.low = 11   AND jt.high = 50    THEN '11-50'
                 WHEN jt.low = 51   AND jt.high = 200   THEN '51-200'
                 WHEN jt.low = 201  AND jt.high = 500   THEN '201-500'
                 WHEN jt.low = 501  AND jt.high = 1000  THEN '501-1000'
                 WHEN jt.low = 1001 AND jt.high = 5000  THEN '1001-5000'
                 WHEN jt.low = 5001 AND jt.high = 10000 THEN '5001-10000'
                 WHEN jt.low = 10001 AND jt.high IS NULL THEN '10001+'
                 ELSE NULL
              END
        WHERE cr.company_size IS NOT NULL
          AND jt.low IS NOT NULL
          AND (
               (jt.low = 0    AND jt.high = 1)
            OR (jt.low = 2    AND jt.high = 10)
            OR (jt.low = 11   AND jt.high = 50)
            OR (jt.low = 51   AND jt.high = 200)
            OR (jt.low = 201  AND jt.high = 500)
            OR (jt.low = 501  AND jt.high = 1000)
            OR (jt.low = 1001 AND jt.high = 5000)
            OR (jt.low = 5001 AND jt.high = 10000)
            OR (jt.low = 10001 AND jt.high IS NULL)
          );
        """,
        # 9.5) Locations
        """
        INSERT INTO locations (country, city, postal_code, address_line1, is_hq, state)
        SELECT DISTINCT
            TRIM(jt.country) AS country,
            TRIM(jt.city) AS city,
            TRIM(jt.postal_code) AS postal_code,
            TRIM(jt.line_1) AS address_line1,
            CASE 
                WHEN TRIM(jt.is_hq) IN ('true', '1') THEN TRUE 
                ELSE FALSE 
            END AS is_hq,
            TRIM(jt.state) AS state
        FROM company_raw cr
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
          AND TRIM(jt.country) <> '';
        """,
        """
        INSERT INTO company_location (company_id, locations_id)
        SELECT DISTINCT
            cr.company_id, 
            l.locations_id
        FROM company_raw cr
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
        JOIN locations l 
            ON l.country       = TRIM(jt.country)
           AND l.city          = TRIM(jt.city)
           AND l.postal_code   = TRIM(jt.postal_code)
           AND l.address_line1 = TRIM(jt.line_1)
           AND l.state         = TRIM(jt.state)
           AND l.is_hq         = CASE 
                                   WHEN TRIM(jt.is_hq) IN ('true', '1') THEN TRUE 
                                   ELSE FALSE 
                                 END
        WHERE cr.locations IS NOT NULL
          AND TRIM(jt.country) <> '';
        """,
        # 9.6) Company Updates
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
        FROM company_raw cr
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
        WHERE cr.updates IS NOT NULL;
        """,
        # 9.7) Affiliated Companies
        """
        INSERT INTO affiliated_companies (company_id, name, linkedin_url, industry, location)
        SELECT 
            cr.company_id,
            COALESCE(jt.name, 'No Name Provided') AS name,
            COALESCE(jt.link, 'No Link Provided') AS linkedin_url,
            COALESCE(jt.industry, 'No Industry Provided') AS industry,
            COALESCE(jt.location, 'No Location Provided') AS location
        FROM company_raw cr
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
        WHERE cr.affiliated_companies IS NOT NULL;
        """,
        # 9.8) Similar Companies
        """
        INSERT INTO similar_companies (name, linkedin_url, industry, location)
        SELECT DISTINCT
            COALESCE(TRIM(jt.name), 'No Name Provided') AS name,
            COALESCE(TRIM(jt.link), 'No Link Provided') AS linkedin_url,
            COALESCE(TRIM(jt.industry), 'No Industry Provided') AS industry,
            COALESCE(TRIM(jt.location), 'No Location Provided') AS location
        FROM company_raw cr
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
          AND TRIM(jt.name) <> '';
        """,
        """
        INSERT INTO similar_companies_junction (company_id, similar_companies_id)
        SELECT DISTINCT
            cr.company_id,
            sc.similar_companies_id
        FROM company_raw cr
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
          AND TRIM(jt.name) <> '';
        """,
        # ---------------------------------------------------------
        # 10) Cleanup: drop columns that are no longer needed
        # ---------------------------------------------------------
        """
        ALTER TABLE company_raw DROP COLUMN industry;
        """,
        """
        ALTER TABLE company_raw DROP COLUMN hq;
        """,
        """
        ALTER TABLE company_raw DROP COLUMN company_type;
        """,
        """
        ALTER TABLE company_raw DROP COLUMN specialities;
        """,
        """
        ALTER TABLE company_raw DROP COLUMN locations;
        """,
        """
        ALTER TABLE company_raw DROP COLUMN similar_companies;
        """,
        """
        ALTER TABLE company_raw DROP COLUMN affiliated_companies;
        """,
        """
        ALTER TABLE company_raw DROP COLUMN updates;
        """,
        """
        ALTER TABLE affiliated_companies DROP COLUMN location;
        """,
        """
        ALTER TABLE similar_companies DROP COLUMN location;
        """,
    ]

    # Execute each query in sequence
    for i, query in enumerate(queries, start=1):
        print(f"Executing query #{i}...")
        cursor.execute(query)
        connection.commit()

    cursor.close()
    connection.close()
    print("All queries executed successfully!")


if __name__ == "__main__":
    run_migration()
