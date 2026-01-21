import datetime
import requests
import pandas as pd
import json
from google.cloud import bigquery
from DataPlatform import GCP
from urllib.parse import urlparse,parse_qs
import time

task_conn_id = "{{task_conn_id}}"
brand = "{{brand}}"
lake_proj_id = "{{lake_project_id}}"
proc_proj_id = "{{proc_project_id}}"
table_name = f"{{target_table}}_{brand}"

access_string = GCP.access_secret_version(proc_proj_id, task_conn_id, 'latest')
access_details = json.loads(access_string)
api_key = access_details.get('api_key')
campaign_id = access_details.get('campaign_id')
headers = {"Authorization" : f"Basic {api_key}"}

url = f"{{api_url}}/reporting/report_advertiser/campaign/{campaign_id}/conversion.json"
   
start_date = datetime.date.today() - datetime.timedelta(days=365)
end_date = datetime.date.today() - datetime.timedelta(days=0)
start_date = datetime.datetime.strftime(start_date, '%Y-%m-%dT%H:%M:%SZ')
end_date = datetime.datetime.strftime(end_date, '%Y-%m-%dT%H:%M:%SZ')
payload = {
                "start_date": start_date,
                "end_date": end_date
            }

df=pd.DataFrame()

while payload:
    try :
        response = requests.get(url=url, headers=headers, params=payload)
        parsed = response.json()
        normalised = []
        for record in parsed['conversions'] :
            normalised.append(record['conversion_data'])
        tmp = pd.json_normalize(normalised)
        df = pd.concat([df,tmp])

        nextpage=urlparse(parsed['hypermedia']['pagination']['next_page'])
        payload=parse_qs(nextpage.query)
    except Exception as e:
        print(e)
        break
    time.sleep(3)

fields = ['conversion_id',
          'conversion_reference',
          'conversion_type',
          'publisher_id',
          'publisher_name',
          'campaign_id',
          'campaign_title',
          'conversion_time',
          'last_modified',
          'currency',
          'country',
          'conversion_value.conversion_status',
          'conversion_value.value',
          'conversion_value.commission',
          'conversion_value.publisher_commission',
          'conversion_lag',
          'clickref']

df=df[fields]
df.columns = df.columns.str.replace('.', '_',regex=False)
df['brand'] = brand
df = df.astype(str)

schema = [bigquery.SchemaField(c.replace('.','_'), 'STRING') for c in df.columns.values.tolist()]

client = bigquery.Client(project=lake_proj_id)
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
job = client.load_table_from_dataframe(df, table_name, job_config=job_config)
job.result()