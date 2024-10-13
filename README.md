# Clever Data Engineer Trial Project

## Goal of the Project:

At Clever, we are turning data into personalized and localized content so our readers can make well-informed decisions about any step of their real estate journey.

Please fork this repository and let us know when you've completed the project with a link to your fork.

Using the data set decribed below, and the Airflow-based data pipeline docker configuration, perform basic analysis for the chosen in `company_profiles_google_maps.csv`. There are basic errors on the DAG that you will need to fix for the pipeline to work properly. 

## Expectations
To perform this project you will need to:
* Perform transforms on the raw data and load them into a PostgreSQL database
* Be able to join datasets together in way for an analyst to be able to rank by a certain set of criteria (you can determine this criteria)
* Be able to filter the data by city or state so analysis can be performed by locality
* Given a locality, create a ranked list according to the criteria you’ve chosen

**Bonus:**
* Interesting additional analysis based on the underlying data
* An example could be Review Sentiment, common themes among ratings, complaints, etc.

## Dataset
Moving company data set (files can be found at 'dags/scripts/data_examples' folder)
* fmcsa_companies.csv
* fmcsa_company_snapshot.csv
* fmcsa_complaints.csv
* fmcsa_safer_data.csv
* company_profiles_google_maps.csv
* customer_reviews_google.csv


## Getting started
To get started with Airflow check the [getting started](docs/getting_started.md) documentation.



# Notes on the data

## Tables general overview
From looking at the data I understood that the tables can be "divided" in two sections, google related data and FMCSA data.

### Google data

From Google we have 2 tables: 
- company_profiles_google_maps:
    - General data about each company
    - google_id is the primary key
- customer_reviews_google
    - Pretty self explanatory, holds reviews from google about each company
    - also uses google_id as key

### FMCSA data

From the FMCSA we have 4 tables, all using usdot_num as primary key
- fmcsa_companies:
    - General data about each company
- fmcsa_safer_data:
    - Holds information about safety checks on each company
- fmcsa_company_snapshot:
    - Seems to be a table of snapshots taken for each company information
- fmcsa_complaints:
    - Clients complaints about each company

## Possible joins

After understanding the general data present in each table, I looked out for possible joins that could enrich or connect the data, the joins in google and fmcsa sections are pretty obvious and easy but looking at the tables there isn't a common key to join Google companies to the FMCSA companies (Might do a fuzzy match using different informations in each dataset)

