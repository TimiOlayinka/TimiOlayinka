import requests
import pandas as pd
import json
from datetime import date, timedelta
import time
import backoff
from google.cloud import bigquery
from DataPlatform import GCP

# pass next url calls to single function to allow for retry of single page
# via exp. backoff when hitting strict rate limits without having to 
# re-attempt whole call.
@backoff.on_exception(backoff.expo, Exception, max_tries=2)
def call_next_page(endpoint: str, data: str, headers: dict) -> dict :
    import requests
    import time

    response = requests.post(endpoint, data, headers)
    print(response.status_code)
    if response.status_code == 200 :
        response = response.json()
        return response
    else :
        if response.status_code == 429 :
            time.sleep(10) #add additional sleep if rate limit hit, because it's REALLY low.
        raise Exception

rpt_date = date.today() - timedelta(days=1)
rpt_date = rpt_date.strftime("%Y-%m-%d")
task_conn_id = "{{task_conn_id}}"
endpoint = "{{endpoint}}"
brand = "{{brand}}"
lake_proj_id = "{{lake_project_id}}"
proc_proj_id = "{{proc_project_id}}"
table_name = f"{{target_table}}_{brand}"  

access_string = GCP.access_secret_version(proc_proj_id, task_conn_id, 'latest')
access_details = json.loads(access_string)
api_key = access_details.get('api_key')
branch_secret = access_details.get('branch_secret')
granularity = 'day'
dimensions = '["last_attributed_touch_data_tilde_campaign","last_attributed_touch_data_tilde_channel","last_attributed_touch_data_tilde_feature","last_attributed_touch_data_tilde_keyword","name","user_data_geo_country_code","customer_event_alias"]'
topics = ['eo_commerce_event','eo_commerce_event','eo_click','eo_install','eo_open','eo_pageview','eo_reinstall','eo_web_session_start']

mdf = pd.DataFrame()
for index, topic in enumerate(topics) :
    data_source = topic

    if topic == 'eo_commerce_event' and index == 0 :
        aggregation = 'revenue'
        source_file = 'revenue'
    else :
        source_file = ''.join(topic.replace('eo_','').split('_')[0:2]) #consistency with existing data
        aggregation = 'total_count'
    data = '{' + '"branch_key":' + '"' + api_key + '"' + ',' + '"branch_secret":' + '"' + branch_secret + '"' + ',' + '"start_date":' + '"' + rpt_date + '"' + ',' + '"end_date":' + '"' + rpt_date + '"' + ',' + '"data_source":' + '"' + data_source + '"' + ',' + '"granularity":' + '"' + granularity + '"' + ',' + '"aggregation":' + '"' + aggregation + '"' + ',' + '"dimensions":' + dimensions + '}'
    headers = {'content-type': 'application/json'}

    response = requests.post(endpoint,data,headers)
    if response.status_code == 200 :
        response = response.json()
    else :
        continue
    
    results = response.get('results')
    df = pd.json_normalize(results)
    while 'next_url' in str(response.get('paging')) :
        time.sleep(3) #Max 20 requests per minute
        next_url = 'https://api2.branch.io' + str(response.get('paging')["next_url"])
        try :
            response = call_next_page(endpoint=next_url, data=data, headers=headers)
            results = response.get('results')
            tmp = pd.json_normalize(results)
            df = pd.concat([df,tmp])
        except Exception as e:
            print(e)
    
    if 'result.total_count' not in df.columns :
        df['result.total_count'] = None
    if 'result.revenue' not in df.columns :
        df['result.revenue'] = None
    df['brand'] = brand
    df['source_file'] = source_file
    mdf = pd.concat([mdf,df])

mdf.columns = [c.replace('.','_') for c in mdf.columns]
mdf = mdf.astype('string')
schema = [bigquery.SchemaField(c, 'STRING') for c in mdf.columns]

client = bigquery.Client(project=lake_proj_id)
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)
job = client.load_table_from_dataframe(mdf, table_name, job_config=job_config)
job.result()
