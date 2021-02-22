from crontab import CronTab
# Setup a cron process to run Daily at 9:00 AM
cron = CronTab(tab="""
  0 9 * * * python etl.py # ETL everyday at 9 am
""")
# same as 0 9 * * * python etl.py
print("Crontab list is as follows:")
for job in cron:
    print(job)