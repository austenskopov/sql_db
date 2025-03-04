# LinkedIn Company Data ETL System

![Database](https://img.shields.io/badge/Database-MySQL-blue)
![ETL](https://img.shields.io/badge/ETL-Pentaho-orange)
![Python](https://img.shields.io/badge/Language-Python-green)

## Overview
This repository contains an ETL (Extract, Transform, Load) pipeline for processing LinkedIn company profile data from JSON files and storing it in a normalized MySQL database. The system covers conceptual, logical, and physical design aspects to ensure data consistency and referential integrity.

## Key Features
- **Data Modeling**  
  - Conceptual, Logical, and Physical diagrams (Crow's Foot notation, 3NF design).
  - Focus on proper normalization principles to achieve Third Normal Form (3NF).
- **Staging**  
  - Loads raw JSON data into a staging database (`staging`) using Pentaho Data Integration.
- **Transformation**  
  - Normalizes data into intermediate tables, correcting inconsistencies and handling duplicates.
- **Loading**  
  - Moves transformed data into the final `company` database with proper constraints and relationships.

## Technology Stack
- **MySQL** for relational storage
- **Pentaho Data Integration (Spoon)** for ETL workflows
- **Python 3.x** for scripting and data manipulation
- **JSON** as source format

## Repository Structure
```
sql_db/
├── README.md                     # This documentation
├── instructions.md               # Setup and execution instructions
├── normalization_script.py       # Python data processing script
├── stage-json-to-table.ktr       # Pentaho transformation definition
├── staging.sql                   # SQL schema and normalization queries
└── company-profile/              # Source JSON data files
    └── [company_*.json]          # Multiple company profile files
```

## Data Flow
1. **Extract**  
   - JSON files from the `company-profile` directory are identified and read.
2. **Staging**  
   - Using Pentaho Data Integration (Spoon), data is inserted into `staging` tables in MySQL.
3. **Transform**  
   - **Primary Method:** Run the SQL statements in `staging.sql` for normalization:
     - First execute the table creation section to establish the schema
     - Then execute the remaining transformation queries in order
   - **Optional:** Execute the `normalization_script.py` only if additional custom logic is required.
4. **Load**  
   - Final normalized data is moved into the `company` database with enforced primary/foreign keys.

## Getting Started

### Prerequisites
- MySQL Server 5.7+  
- Pentaho Data Integration 8.0+  
- Python 3.6+ (optional, only if using the Python script)  

### Quick Setup
1. **Clone** this repository.
2. **Launch Pentaho** with the `./spoon` command.
3. **Configure** database connections in Pentaho.
4. **Run** the Pentaho transformation to load staging data.
5. **Normalize** the data:
   - Run the remaining SQL transformation querie after the first one:
     ```bash
     mysql -u <user> -p <database> < staging_transformations.sql
     ```
   - Note: Python script is optional and only needed for specialized transformations.
6. **Confirm** the final structure in the `company` database.

## Example Query
```sql
-- Retrieve companies with their specialties
SELECT c.name, GROUP_CONCAT(s.specialty_name) AS specialties
FROM companies c
JOIN company_specialties cs ON c.id = cs.company_id
JOIN specialties s ON cs.specialty_id = s.id
GROUP BY c.name;
```

## Additional Documentation
See [instructions.md](instructions.md) for more details on the SQL normalization process and how the ERD was developed to achieve proper 3NF design.