import requests
import json
from collections import namedtuple
from contextlib import closing
import sqlite3

from prefect import task, Flow


## extract
@task
def get_complaint_data():
    r = requests.get("https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/", params={'size':10})
    response_json = json.loads(r.text)
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
    create_script = 'CREATE TABLE IF NOT EXISTS complaint (timestamp TEXT, state TEXT, product TEXT, company TEXT, complaint_what_happened TEXT)'
    insert_cmd = "INSERT INTO complaint VALUES (?, ?, ?, ?, ?)"

    with closing(sqlite3.connect("cfpbcomplaints.db")) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executescript(create_script)
            cursor.executemany(insert_cmd, parsed_complaint_data)
            conn.commit()


with Flow("etl flow") as flow:
    raw_complaint_data = get_complaint_data()
    parsed_complaint_data = parse_complaint_data(raw_complaint_data)
    store_complaints(parsed_complaint_data)

flow.visualize()    
flow.run()


