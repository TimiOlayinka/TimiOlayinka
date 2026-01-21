import logging
import pandas as pd
import openpyxl
from google.cloud import bigquery
from typing import List, Literal

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format ='*%(levelname)s* | %(message)s')



EXPECTED_COLUMNS = {
                        'Date', 
                        'Store', 
                        'Marketplace', 
                        'BNPL',
                        'Total', 
                        'Week commencing',  
                        'Web loyalty' #space at the end
                        }

class ParsingException(BaseException):
    """Handles excel parsing exceptions"""

def read_excel_sheet(path: str,sheet_name: str,**kwargs) -> pd.DataFrame:
    global pd, logger

    try:
        df = pd.read_excel(io=path, sheet_name=sheet_name, **kwargs)
    except FileNotFoundError:
        logger.error('Excel file not in the path {path}')
        raise
    except ValueError:
        logger.error(f'Sheet {sheet_name} does not exist.')
        raise
    return df



# MS Income
def parse_ms_income(df: pd.DataFrame) -> pd.DataFrame:
    global pd, logger, EXPECTED_COLUMNS, SHEET

    stripper = {col_name: col_name.strip() for col_name in df.columns}
    df = df.rename(columns = stripper)
    try:
        parsed_df = df[EXPECTED_COLUMNS]
        if parsed_df.shape[0] > 1:
            logger.info(f'Succesfully parsed {SHEET} sheet - result shape: {parsed_df.shape}')
            return parsed_df
        
        msg = f'Parsing {SHEET} sheet resulted in 0 rows: Dataframe shape: {parsed_df.shape}.'
        logger.error(msg)
        raise ParsingException(msg)
    except KeyError as e:
        logger.error(e)
        raise


def ingest_data(results: pd.DataFrame, bq_schema: List[bigquery.SchemaField], project_id: str, table_dataset: str, table_name: str, write_disposition: Literal['WRITE_APPEND', 'WRITE_TRUNCATE']) -> None:
    global pd, logger, SHEET
    client = bigquery.Client(project=project_id)
    table_id = client.dataset(table_dataset).table(table_name)
    job_config = bigquery.LoadJobConfig(schema=bq_schema, write_disposition=write_disposition)
    client.load_table_from_dataframe(results, table_id, job_config=job_config)
    logger.info(f'Succesfully ingested data from {SHEET} sheet - dataframe shaped :{results.shape} to {project_id}.{table_dataset}.{table_name}')


PATH = '{{PATH}}'
SHEET = 'MS Income'

PROJECT_ID = '{{PROJECT_ID}}'
BQ_DATASET = '{{BQ_DATASET}}'

BQ_TABLE = 'Store_BNPL_WebLoyalty_Retail_BeautyCont2'

WRITE_OPTION = 'WRITE_TRUNCATE'  

read = read_excel_sheet(PATH, SHEET)
df = parse_ms_income(read)

# Replace names with spaces to underscores
df = df.rename(columns= {'Week commencing': 'Week_commencing', 'Web loyalty':'Web_loyalty'})
schema = [
        bigquery.SchemaField(name="Date", field_type="TIMESTAMP"),
        bigquery.SchemaField(name="Store", field_type="FLOAT"),
        bigquery.SchemaField(name="Marketplace", field_type="FLOAT"),
        bigquery.SchemaField(name="BNPL", field_type="FLOAT"),
        bigquery.SchemaField(name="Total", field_type="FLOAT"),
        bigquery.SchemaField(name="Week_commencing", field_type="STRING"),
        bigquery.SchemaField(name="Web_loyalty", field_type="FLOAT")
]
ingest_data(results=df, bq_schema=schema,project_id=PROJECT_ID, table_dataset=BQ_DATASET, table_name=BQ_TABLE, write_disposition=WRITE_OPTION)
