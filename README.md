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



# Notes and project explanation

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

## DAGs Created

### Google data treatment

Here I did the main trial task, which was to build a treated table that ranked companies by a metric and let the analysts filter by city and state. The DAG is google_treatment_DAG.
I decided for the metric, to use both the number of reviews and the avg rating and make a bayesian average to account for both metrics in a single feature, this way the analysts can rank by this column and use the ranks.

### FMCSA x Google data

After understanding the general data present in each table, I looked out for possible joins that could enrich or connect the data, the joins in google and fmcsa sections are pretty obvious and easy but looking at the tables there isn't a common key to join Google companies to the FMCSA companies and decided to try a fuzzy match.
It yielded a few joins and for what I looked they seemed to be good joins, the dag is google_fmcsa_joiner_DAG

### Sentiment analysis

For the bonus I enjoyed the idea of making a score with the sentiment on each review text, I wanted to test out different approaches, did a little bit of EDA to understand languages, which can be found on EDA_Google_data.ipynb, and decided to go with polyglot and textblob, both very simple, yet useful models. Sampling a few of the results, polyglot seemed to be the best approach but it didn't work in every row, so I ended up combining both.

The result was a table with the bayesian average of the sentiment scores for each company and the dag is sentiment_analysis_DAG.

## Next steps.

Although I believe it was a good stop here, had I more time, I would work on:
- Building a more consolidated score using sentiment and ratings
- Making a wordcloud with the complaints data
- Using the matches from FMCSA and google data to add the complaints to the reviews data (It yielded few joins but thinking of a larger database it might be more interesting)
- Improving the structure of the connections with postgres to make it more reusable and standardized, it got a little scattered for different pipelines

## Conclusions

It was a really cool project, managed to fix the airflow setup, the main DAG did run, we added some packages on the docker requirements and we got some interesting results on the treated data. It was specially challenging dealing with the sentiment on airflow but because of the modules I chose that were giving me some headache to deploy haha, although it was working on the jupyter server