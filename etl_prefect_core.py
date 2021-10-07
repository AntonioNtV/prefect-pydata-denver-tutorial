from prefect.core.flow import cache
import requests
import json
import sqlite3
from collections import namedtuple
from contextlib import closing
from datetime import timedelta

from prefect import task, Flow
from prefect.tasks.database.sqlite import SQLiteScript
from prefect.schedules import IntervalSchedule

DATABASE_NAME='cfpbcomplaints.db'

## setup
create_table = SQLiteScript(
    db=DATABASE_NAME,
    script='CREATE TABLE IF NOT EXISTS complaint (timestamp TEXT, state TEXT, product TEXT, company TEXT, complaint_what_happened TEXT)'
)

## extract
@task(cache_for=timedelta(days=1))
def get_complaint_data():
    r = requests.get("https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/", params={'size':10})
    response_json = json.loads(r.text)
    print("I actually requested this time!")
    return response_json['hits']['hits']


## transform
@task
def parse_complaint_data(raw_complaint_data):
    complaints = []
    Complaint = namedtuple('Complaint', ['data_received', 'state', 'product', 'company', 'complaint_what_happened'])
    for row in raw_complaint_data:
        source = row.get('_source')
        this_complaint = Complaint(
            data_received=source.get('date_recieved'),
            state=source.get('state'),
            product=source.get('product'),
            company=source.get('company'),
            complaint_what_happened=source.get('complaint_what_happened')
        )
        complaints.append(this_complaint)
    return complaints

## load
@task
def store_complaints(parsed_complaint_data):
    insert_cmd = "INSERT INTO complaint VALUES (?, ?, ?, ?, ?)"

    with closing(sqlite3.connect("cfpbcomplaints.db")) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executemany(insert_cmd, parsed_complaint_data)
            conn.commit()



def build_flow(schedule=None):
    with Flow("etl flow", schedule=schedule) as flow:
        db_table = create_table()
        raw_complaint_data = get_complaint_data()
        parsed_complaint_data = parse_complaint_data(raw_complaint_data)
        populated_table = store_complaints(parsed_complaint_data)
        populated_table.set_upstream(db_table) # db_table need to happen before populated_table
    return flow

schedule = IntervalSchedule(interval=timedelta(minutes=1))
etl_flow = build_flow(schedule)
etl_flow.run()    


