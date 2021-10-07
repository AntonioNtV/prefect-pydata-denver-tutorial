import requests
import json
import sqlite3
import pathlib
import prefect
from collections import namedtuple
from contextlib import closing
from datetime import timedelta

from prefect import task, Flow
from prefect.tasks.database.sqlite import SQLiteScript
from prefect.schedules import IntervalSchedule
from prefect.engine.results import LocalResult

DATABASE_NAME='cfpbcomplaints.db'

## setup
create_table = SQLiteScript(
    db=DATABASE_NAME,
    script='CREATE TABLE IF NOT EXISTS complaint (timestamp TEXT, state TEXT, product TEXT, company TEXT, complaint_what_happened TEXT)'
)

def alert_failed(obj, old_state, new_state):
    if new_state.is_failed():
        logger = prefect.context.get('logger')
        logger.info("I actually requested this time!")


## extract
@task(cache_for=timedelta(days=1), state_handlers=[alert_failed], result=LocalResult(dir="{current_path}/results".format(current_path=pathlib.Path(__file__).parent.resolve())))
def get_complaint_data():
    r = requests.get("https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/", params={'size':10})
    response_json = json.loads(r.text)
    logger = prefect.context.get('logger')
    logger.info("I actually requested this time!")
    return response_json['hits']['hits']


## transform
@task(state_handlers=[alert_failed])
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
@task(state_handlers=[alert_failed])
def store_complaints(parsed_complaint_data):
    insert_cmd = "INSERT INTO complaint VALUES (?, ?, ?, ?, ?)"

    with closing(sqlite3.connect(DATABASE_NAME)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executemany(insert_cmd, parsed_complaint_data)
            conn.commit()



def build_flow(schedule=None):
    with Flow("etl flow", schedule=schedule, state_handlers=[alert_failed]) as flow:
        db_table = create_table()
        raw_complaint_data = get_complaint_data()
        parsed_complaint_data = parse_complaint_data(raw_complaint_data)
        populated_table = store_complaints(parsed_complaint_data)
        populated_table.set_upstream(db_table) # db_table need to happen before populated_table
    return flow

schedule = IntervalSchedule(interval=timedelta(minutes=1))
etl_flow = build_flow(schedule)
etl_flow.register(project_name='ETL FIRST PROJECT WITH PREFECT')    


