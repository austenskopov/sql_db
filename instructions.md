# Group Project 2 Setup Instructions

## Prerequisites
- Ensure the `mysql.jar` file is added to `/data-integration/lib/`.
- Launch `./spoon.sh`.

## Steps to Configure and Run the Transformation

1. **Open the Transformation File**
   - Navigate to the `File` tab and select `Open`.
   - Open the `.ktr` file.

2. **Design the Transformation**
   - In the top left corner, click on `Design`.
   - Search for `JSON` and select `JSON INPUT`. Drag it to the central dashboard.
   - Search for `TABLE` and select `TABLE OUTPUT`. Drag it to the dashboard.

3. **Connect Components**
   - Hold `Shift` and connect the `JSON INPUT` to the `TABLE OUTPUT`.

4. **Configure JSON Input**
   - Double-click on the `JSON INPUT` component.
   - In the `file/dir` field, navigate to the `week7` folder for `etl-demo-2025-02-17`.
   - Connect to the `src` folder containing company data (avoid selecting individual files). The path should end with `/source-data/company-profile`.

5. **Set Regular Expression**
   - Under "Regular expression", enter `.*\.json` to include all JSON files.
   - Check "Show filename(s)" to verify file visibility. If files are not visible, retry from step 4.

6. **Verify Fields**
   - Due to "stage-json-to-table.ktr", the `FIELDS` tab should display 27 attributes. If correct, proceed.

7. **Database Setup in TablePlus**
   - Access your local database and create a new schema named `gp_02` or similar.

8. **Create Table in Database**
   - Execute the following SQL query in the `gp_02` schema to create the `company_raw` table:

   ```sql
   CREATE TABLE company_raw (
       linkedin_internal_id VARCHAR(255),
       description TEXT,
       website VARCHAR(255),
       industry VARCHAR(255),
       company_size VARCHAR(50),
       company_size_on_linkedin VARCHAR(50),
       hq VARCHAR(255),
       company_type VARCHAR(255),
       founded_year VARCHAR(10),
       specialties TEXT,
       locations TEXT,
       name VARCHAR(255),
       tagline VARCHAR(255),
       universal_name_id VARCHAR(255),
       profile_pic_url VARCHAR(500),
       background_cover_image_url VARCHAR(500),
       search_id VARCHAR(255),
       similar_companies TEXT,
       affiliated_companies TEXT,
       updates TEXT,
       follower_count VARCHAR(50),
       acquisitions TEXT,
       exit_data TEXT,
       extra TEXT,
       funding_data TEXT,
       categories TEXT,
       customer_list TEXT
   );
   ```

   - If incorrect attributes appear, use the following to reset:

   ```sql
   DROP TABLE IF EXISTS company_raw;
   ```

9. **Configure Table Output in Spoon**
   - Return to the Spoon Pentaho dashboard and select the `TABLE OUTPUT` icon.
   - Under `Connections`, retain `cs453w25` and click `EDIT`.
   - Choose `MySQL` connection type and select `Access: Native (JDBC)`.

10. **Test Database Connection**
    - Ensure the database name (`gp_02`) and password are correct.
    - Test the connection for success.

11. **Run the Transformation**
    - If the connection is successful, return to the main dashboard.
    - Click the `RUN` button (resembles a play button) to execute the transformation.

12. **Verify in TablePlus**
    - Check TablePlus for successful execution. Ensure all 27 attributes are present with no errors.
