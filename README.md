# ETL-Python-Multithreading
Performing ETL on Health Data of New York State.

# Technologies used:
Python, Sqlite Database, Crontab

# Problem Statement:
Imagine you are part of a data team that wants to bring in daily data for COVID-19 test occurring in New York state for analysis. Your team has to design a daily
workflow that would run at 9:00 AM and ingest the data into the system.

API : https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD

By following the ETL process, extract the data for each county in New York state from
the above API, and load them into individual tables in the database. Each county table
should contain following columns :

❖ Test Date

❖ New Positives

❖ Cumulative Number of Positives

❖ Total Number of Tests Performed

❖ Cumulative Number of Tests Performed

❖ Load date


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
