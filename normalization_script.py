# Ensure the target database and the company_raw table with original columns are present before execution.
# load PASSWORD into .env


import mysql.connector
from mysql.connector import MySQLConnection
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()


def create_mysql_connection(
    host: str, user: str, password: str, database: str
) -> MySQLConnection:
    """
    Establishes a connection to a MySQL database.
    """
    try:
        connection = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        if connection.is_connected():
            print("Connection to MySQL database was successful.")
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None


def setup_and_transform_data(connection: MySQLConnection):
    """
    Runs all the SQL statements that mimic your original SQL script:
    1. Cleans up tables,
    2. Creates new tables and relationships,
    3. Inserts data from JSON columns into new tables,
    4. Drops old columns.
    """

    # List all your SQL steps in order:
    sql_statements = [
        # Step 2: Clean up the company_raw table by removing unnecessary columns
        """
        ALTER TABLE company_raw
          DROP COLUMN exit_data,
          DROP COLUMN acquisitions,
          DROP COLUMN extra,
          DROP COLUMN funding_data,
          DROP COLUMN categories,
          DROP COLUMN customer_list;
        """,
        # Step 3: Add a primary key to the company_raw table
        """
        ALTER TABLE company_raw
          ADD COLUMN company_id INT AUTO_INCREMENT PRIMARY KEY FIRST;
        """,
        # Step 4: Create the specialty table
        """
        CREATE TABLE IF NOT EXISTS specialty (
            specialty_name_id INT AUTO_INCREMENT PRIMARY KEY,
            specialty_name VARCHAR(255) NOT NULL
        ) ENGINE=INNODB;
        """,
        # Step 5: Create the company_specialty junction table
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
        # Step 6: Create the type table
        """
        CREATE TABLE IF NOT EXISTS type (
            company_type_id INT AUTO_INCREMENT PRIMARY KEY,
            company_type_name VARCHAR(255) NOT NULL
        ) ENGINE=INNODB;
        """,
        # Step 7: Create the company_type junction table
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
        # Step 8: Create the industry table
        """
        CREATE TABLE IF NOT EXISTS industry (
            industry_id INT AUTO_INCREMENT PRIMARY KEY,
            industry_name VARCHAR(255) NOT NULL
        ) ENGINE=INNODB;
        """,
        # Step 9: Create the industry_type junction table
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
        # Step 10: Create the locations table
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
        # Step 11: Create the company_location junction table
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
        # Step 12: Create the company_updates table
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
        # Step 13: Create the affiliated_companies table
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
        # Step 14: Create the similar_companies table
        """
        CREATE TABLE IF NOT EXISTS similar_companies (
            similar_companies_id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(500) NOT NULL, 
            linkedin_url VARCHAR(500) NOT NULL,
            industry VARCHAR(500) NOT NULL,
            location VARCHAR(500) NOT NULL
        ) ENGINE=INNODB;
        """,
        # Step 15: Create the similar_companies_junction table
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
        # Step 16: Insert distinct specialties into the specialty table
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
        # Step 17: Insert data into the company_specialty junction table
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
        # Step 18: Insert distinct company types into the type table
        """
        INSERT INTO type (company_type_name)
        SELECT DISTINCT TRIM(company_type) AS company_type_name
        FROM company_raw
        WHERE company_type IS NOT NULL
          AND TRIM(company_type) <> ''
        ON DUPLICATE KEY UPDATE company_type_name = company_type_name;
        """,
        # Step 19: Insert data into the company_type junction table
        """
        INSERT INTO company_type (company_id, company_type_id)
        SELECT cr.company_id, t.company_type_id
        FROM company_raw cr
        JOIN type t ON t.company_type_name = TRIM(cr.company_type)
        WHERE cr.company_type IS NOT NULL
          AND TRIM(cr.company_type) <> ''
        ON DUPLICATE KEY UPDATE company_type_id = t.company_type_id;
        """,
        # Step 20: Insert distinct industries into the industry table
        """
        INSERT INTO industry (industry_name)
        SELECT DISTINCT TRIM(industry) AS industry_name
        FROM company_raw
        WHERE industry IS NOT NULL
          AND TRIM(industry) <> ''
        ON DUPLICATE KEY UPDATE industry_name = industry_name;
        """,
        # Step 21: Insert data into the industry_type junction table
        """
        INSERT INTO industry_type (company_id, industry_id)
        SELECT cr.company_id, i.industry_id
        FROM company_raw cr
        JOIN industry i ON i.industry_name = TRIM(cr.industry)
        WHERE cr.industry IS NOT NULL
          AND TRIM(cr.industry) <> ''
        ON DUPLICATE KEY UPDATE industry_id = i.industry_id;
        """,
        # Step 22: Insert distinct locations into the locations table
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
        # Step 23: Insert data into the company_location junction table
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
        # Step 24: Insert updates into the company_updates table
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
        # Step 25: Insert affiliated companies
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
        # Step 26: Insert similar companies
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
        # Step 27: Insert data into the similar_companies_junction table
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
        JOIN similar_companies sc ON 
            sc.name = COALESCE(TRIM(jt.name), 'No Name Provided')
            AND sc.linkedin_url = COALESCE(TRIM(jt.link), 'No Link Provided')
            AND sc.industry = COALESCE(TRIM(jt.industry), 'No Industry Provided')
            AND sc.location = COALESCE(TRIM(jt.location), 'No Location Provided')
        WHERE cr.similar_companies IS NOT NULL
          AND TRIM(jt.name) <> '';
        """,
        # Step 28: Drop columns no longer needed
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

    cursor = connection.cursor()
    for i, statement in enumerate(sql_statements, start=1):
        try:
            cursor.execute(statement)
            connection.commit()
            print(f"[OK] Step {i} executed successfully.")
        except mysql.connector.Error as err:
            print(f"[ERROR] Step {i} failed: {err}")
            # Decide if you want to break or continue
            # break

    cursor.close()


# Example usage:
if __name__ == "__main__":
    password = os.getenv("PASSWORD")
    connection = create_mysql_connection("localhost", "root", password, "gp_02")
    if connection:
        setup_and_transform_data(connection)
        connection.close()
