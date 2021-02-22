# Data-Engineering---ETL
Performing ETL on Health Data of New York State.

# Technologies used:
Python, Sqlite Database, Crontab

# Project Scope:
1) Extract data from the URL: https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD

2) Transform the JSON data to the required format according to each County of the New York State

3) Load the Data using Multi-threading concepts of OOPS.

# Assumptions:
Everyday the data collected from the API is for the last few days.

Historical data present retrieved from the API is correct.

For a given county, latest Test date is retrieved and all the data upto that test date is neglected (To make sure we follow the incremental process).


# Requirements:

pip install urllib
pip install json
pip install sqlite3
pip install python-crontab
