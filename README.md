# Data-Engineering---ETL
Performing ETL on Health Data of New York State.

# Technologies used:
Python, Sqlite Database, Crontab

# Project Scope:
1) Extract data from the URL: https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD
2) Transform the JSON data to the required format according to each County of the New York State
3) Load the Data using Multi-threading concepts of OOPS.
4) Implement the Unit Tests

# Assumptions:
1) Everyday the data collected from the API is for the last few days.
2) Historical data present retrieved from the API is correct.
3) For a given county, latest Test date is retrieved and all the data upto that test date is neglected (To make sure we follow the incremental process).
4) For the unit tests, count of data from the API and count of data from the DB are checked (As we have not created stage tables).

# Requirements:
pip install urllib
pip install json
pip install sqlite3
pip install python-crontab

# Steps to setup the cron for the application:
python cron_etl.py

# Steps to run the application:
python etl.py
