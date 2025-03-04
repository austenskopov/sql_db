# LinkedIn Company Data ETL System

![Database](https://img.shields.io/badge/Database-MySQL-blue)
![ETL](https://img.shields.io/badge/ETL-Pentaho-orange)
![Python](https://img.shields.io/badge/Language-Python-green)

## 📋 Overview
A comprehensive ETL (Extract, Transform, Load) pipeline for processing LinkedIn company profile data. This system converts raw JSON data into a normalized SQL database with proper relational schema design.

## 🚀 Features
- **Complex JSON Processing**: Handles nested structures, arrays, and objects
- **Complete Normalization**: Creates efficient relational tables with proper constraints
- **Automated Workflow**: End-to-end pipeline from extraction to final database
- **Data Quality Handling**: Manages NULL values, duplicates, and inconsistencies
- **Performance Optimized**: Designed for efficient querying and storage

## 🛠️ Technology Stack
- **Database**: MySQL
- **ETL Tool**: Pentaho Data Integration (Spoon)
- **Scripting**: Python 3.x
- **Data Format**: JSON → Relational SQL

## 📂 Repository Structure
```
sql_db/
├── README.md                       # This documentation
├── instructions.md                 # Setup and execution instructions
├── normalization_script.py         # Python data processing script
├── stage-json-to-table.ktr         # Pentaho transformation definition
├── staging.sql                     # SQL schema and normalization queries
└── company-profile/                # Source JSON data files
    └── [company_*.json]            # Multiple company profile files
```

## 📊 Data Processing Flow
1. **Extract**: Source JSON files from `company-profile` directory
2. **Load**: Pentaho loads raw data into `company_raw` staging table
3. **Transform**: SQL and Python scripts normalize the data
4. **Result**: Fully normalized database with tables for:
   - Companies (core data)
   - Specialties (many-to-many relationship)
   - Locations (geographic data)
   - Industries (categorization)
   - And other related entities

## ⚙️ Getting Started

### Prerequisites
- MySQL Server 5.7+
- Pentaho Data Integration 8.0+
- Python 3.6+ with required packages
- Source JSON data files

### Quick Setup
1. Clone this repository
2. Configure database connection in Pentaho
3. Run initial data load:
   ```
   ./run-pentaho-job.sh stage-json-to-table.ktr
   ```
4. Execute SQL normalization:
   ```
   mysql -u username -p database < staging.sql
   ```
5. Run Python normalization:
   ```
   python3 normalization_script.py
   ```

## 📝 Documentation
See [instructions.md](instructions.md) for comprehensive setup and execution steps.

## 🔍 Example Queries
```sql
-- Get companies with their specialties
SELECT c.name, GROUP_CONCAT(s.specialty_name) AS specialties
FROM companies c
JOIN company_specialties cs ON c.id = cs.company_id
JOIN specialties s ON cs.specialty_id = s.id
GROUP BY c.name;
```
