## HOW TO USE THIS DATA PIPELINE PROJECT


# Introdution

This project is for technical servier data project

## structure du projet
   - src
      - data
    
        - data_loader
        - data_cleaning
        - adhoc_processing
      - main.py
   - data
     - raw
       - clinical_trials.csv
       - drugs.csv
       - pubmed.csv
       - pubmed.json
     - cleaned
       - clinical_pubmed_drug.json(graph json)
   - .github
       - workflows
          - python-package.yml 
       

  - tests 
     - test_function.py
     - clinical_pubmed_drug.json file
  - Dokerfile
  - requirements.txt
  - SQl
    - test1.sql
    - test2.sql
## Deployment
- The output of this test is [this json file](data/cleaned/clinical_pubmed_drug.json)
- The ad-hoc part is printed when launching ``python -m src.main.py``
- To deploy this project in local do the following steps


1. **Create virtual environment with Python 3.9:**
   ```bash
   python3 -m venv venv
   ```

2. **Activate the virtual environment:**
   ```bash
   source venv/bin/activate
   ```

3. **Install dependencies:**
   ```bash
   pip3 install -r requirements.txt
   
4. **launch the main part:**
   ```bash
   python -m src.main
   ```` 
5. **launch the test:**
   ```bash
   pytest
   ````    
   
      Another option is to parse the paths of files as arguments
- ### Deployment of production context
    - A folder config should be created and add `dev`, `uat`, `ppd` and `prd` folder to hold the source information
  
    - add `ci/cd` with github workflow or cloud build that is trigger on push, merge operation

    - I have preference for cloud build with terraform by creating **cloud build repositoy and git connexion** 
    - For merge strategy we could use [trunked_based development](https://www.atlassian.com/continuous-delivery/continuous-integration/trunk-based-development) based on release  
    - We could trigger the pipeline based on Eventarc when the list of file is uploaded to GCS via Airflow or Cloud Workflow
    - For Airflow(composer) we should create an instance

## Strategy for huge volume of data 
  
  - The change to make is about the source of data.
Where there will be loaded
  - Update the reader_loader function to be able to read from source of data 
  - Raw data should also be loaded to table for analysis and facilitate the normalization part 
  - Use Spark , Dask for parallelism 


# SQL TEST 
  The [test 1](**) and [Test 2](SQL/Test2.sql) are on the SQL folder 
  
We can launch it on Bigquery editor 