import datetime
import time
import requests
import pandas as pd
import json
from io import StringIO
from google.cloud import bigquery
from DataPlatform import GCP

task_conn_id = "{{task_conn_id}}"
url = "{{api_url}}"
brand = "{{brand}}"
lake_proj_id = "{{lake_project_id}}"
proc_proj_id = "{{proc_project_id}}"
table_name = f"{{target_table}}_{brand}"


access_string = GCP.access_secret_version(proc_proj_id, task_conn_id, 'latest')
access_details = json.loads(access_string)
app_id = access_details.get('app_id')
access_token = access_details.get('api_key')
headers = {'access-token': access_token,'content-type': 'application/json'}
url = url + app_id

start_date = datetime.date.today() - datetime.timedelta(days=1)
end_date = datetime.date.today() - datetime.timedelta(seconds=1)
start_date = datetime.datetime.strftime(start_date, '%Y-%m-%dT%H:%M:%SZ')
end_date = datetime.datetime.strftime(end_date, '%Y-%m-%dT%H:%M:%SZ')

report_type = 'eo_commerce_event'
fields = ['timestamp_iso',
          'last_attributed_touch_data_tilde_channel',
          'last_attributed_touch_data_tilde_campaign',
          'last_attributed_touch_data_tilde_feature',
          'last_attributed_touch_data_tilde_tags',
          'last_attributed_touch_data_tilde_keyword',
          'user_data_geo_country_code',
          'event_data_transaction_id',
          'event_data_currency',
          'event_data_exchange_rate',
          'event_data_revenue',
          'event_data_revenue_in_usd',
          'name']
filters = ['eq','name','PURCHASE']
payload = '{"report_type":"' + report_type + '","limit":2000000,"fields":' + json.dumps(fields) + ',"start_date":' + '"' + start_date + '","end_date":' + '"' + end_date + '","filter":' + json.dumps(filters) + '}'

# Queue export
response = requests.post(url=url, headers=headers, data=payload)
parsed = response.json()
export_url = parsed.get('export_job_status_url')

download_status = ""
timeout = 0

# try until export available then download to string
while download_status != "complete" :
    download = requests.get(url=export_url, headers=headers)
    parsed = download.json()
    download_status = parsed.get('status')
    print(download_status)
    time.sleep(5)
    
    timeout += 1
    if timeout == 120 :
        print('timeout error')
        raise Exception
    
    
download_url = parsed.get('response_url')
file_response = requests.get(download_url, allow_redirects = True)

normalised = file_response.content.decode('utf-8')
df = pd.read_csv(StringIO(normalised), dtype=str)
df['brand'] = brand

schema = [bigquery.SchemaField(c, 'STRING') for c in df.columns.values.tolist()]

client = bigquery.Client(project=lake_proj_id)
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
job = client.load_table_from_dataframe(df, table_name, job_config=job_config)
job.result()