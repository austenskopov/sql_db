# LinkedIn Company Data ETL Processing System

## Overview
This repository contains a complete ETL (Extract, Transform, Load) pipeline for processing LinkedIn company profile data. The system ingests JSON data files containing company information, loads them into a structured SQL database, and performs normalization to create an efficient relational schema.

## Repository Structure
```
└── sql_db/
    ├── README.md                        # This documentation file
    ├── instructions.md                  # Detailed setup instructions
    ├── normalization_script.py          # Python script for database normalization
    ├── stage-json-to-table.ktr          # Pentaho Data Integration transformation file
    ├── staging.sql                      # SQL schema and normalization queries
    └── company-profile/                 # Source JSON data files
        └── [multiple company JSON files]
```

## Data Processing Flow
1. **Extraction**: JSON company profile data is sourced from the `company-profile` directory
2. **Loading**: The Pentaho Data Integration tool (`stage-json-to-table.ktr`) loads raw data into the `company_raw` table
3. **Transformation**: Data is normalized through both SQL (`staging.sql`) and Python (`normalization_script.py`) processing
4. **Result**: A fully normalized relational database with separate tables for companies, specialties, locations, industries, etc.

## Key Features
- Processing of complex nested JSON structures including arrays and objects
- Robust SQL schema design with appropriate foreign key relationships
- Python automation for database transformation and enhancement
- Handling of real-world data challenges (NULL values, duplicates, etc.)
- Complete normalization through proper relational database design

## Technologies Used
- **MySQL**: Database engine
- **Pentaho Data Integration (Spoon)**: ETL processing tool
- **Python**: Scripting for advanced database operations
- **SQL**: Schema creation and data transformation

## Setup Instructions
See the `instructions.md` file for detailed setup and execution steps. The process involves:
1. Configuring the database connection in Pentaho
2. Running the initial data load transformation
3. Executing schema creation and normalization queries
4. Running the Python script for additional data processing

This project demonstrates proficiency in SQL database design, ETL processes, data normalization techniques, and using multiple tools to create an efficient data pipeline.