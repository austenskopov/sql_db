remove data types that are all null
1) Exit_data
2) acquisitions 
3) extra 
4) funding_data
5) categories
6) customer_list



1) Create company_specialty table 
* company ID  -> FK(company ID), FK(Special ID) -> special_name_ id

2) Create Company_type_junction
* Company_id -> company_type_id (fk), company_id(fk) -> company_type_id(pk)

3) Similar to Step 2 for Industry_type junction table

4) DELETE HQ table
* use 'is_hq' 
  UPDATE 'Location' Table
* add junction table for company_location
- line_1 -> address_line1

5) trasnform posted_on (MVA) to a single date and cast new type if needed

6) Update 'Update table'
* company_id (fk)
* update_id (pk)


7) Update 'Affiliated table'
* company_id (FK)
* affiliated_companies_id (FK)
* clearer attributes (link -> Linkedin_url)
(1 to many....no junction table needed)


8) Update 'similar_companies'
* create junction table this time for many to many relationship


