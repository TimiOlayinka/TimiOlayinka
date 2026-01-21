########## begin ##########
# pylint: disable=unused-variable, import-error

########## dev / testing req ##########
import sys
sys.path.append(r"C:\github\analytics-code-library\CodeLibrary")

########## global imports ##########
import DataPlatform.API as API
import DataPlatform.GCP as GCP
import json

########################
#setup
import datetime
import pandas as pd
bigquery = GCP.bigquery

#vars
project_id = "{{lake_project_id}}"
proc_proj_id = "{{proc_project_id}}"
table_dataset = "Finance"
table_name = "EventStream"
event_id = f"retail-store-orders"
task_conn_id = 'connection_api_xxx_retailstore'

access_string = GCP.access_secret_version(proc_proj_id, task_conn_id, 'latest')
access_details = json.loads(access_string)
authorisation = access_details.get('Authorization')

date_from = datetime.datetime.today().date() - datetime.timedelta(days=2)
date_from_unix = str(int((date_from - datetime.date(1970,1,1)).total_seconds()))

base_url = "https://api140.xxx.com/v2/accounts/91528"
api_url = f"orders.json"
url = f"{base_url}/{api_url}?num=999999&orderdatefrom={date_from_unix}"

headers = {
  'Authorization': f'{authorisation}'
}

#get data
r = API.execute_api_call(api_url=url, api_headers=headers, api_method="get", api_response_parse=False)

#load data
client = bigquery.Client(project=project_id)
table_id = client.dataset(table_dataset).table(table_name)

schema = [
    bigquery.SchemaField(name="EventDateTime", field_type="TIMESTAMP"),
    bigquery.SchemaField(name="EventID", field_type="STRING"),
    bigquery.SchemaField(name="EventPayload", field_type="STRING"),
    bigquery.SchemaField(name="EventExecutionID", field_type="STRING")
]

row = [{}]
row[0]["EventDateTime"] = datetime.datetime.now()
row[0]["EventID"] = event_id
row[0]["EventPayload"] = str(r)
row[0]["EventExecutionID"] = ""

data = pd.DataFrame(row)
job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")
job = client.load_table_from_dataframe(data, table_id, job_config=job_config)

