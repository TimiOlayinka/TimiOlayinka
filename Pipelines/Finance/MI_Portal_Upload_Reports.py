########## begin ##########
# pylint: disable=unused-variable, import-error

########## dev / testing req ##########
testing = False

if testing:
    import sys
    sys.path.append(r"C:\github\analytics-code-library\CodeLibrary")
 
########## global imports ##########
import DataPlatform.GCP as GCP
import boto3
from datetime import datetime
import pandas as pd
bigquery = GCP.bigquery

########## global vars ##########
source_folder_root = r"\\prd-xxxx\d$\NPrinting\NPrinting Distributed Reports"
source_file_suffixes = ["pdf","csv","html"]

if not testing:
    project_id = '{{project_id}}'
    lake_project_id = '{{lake_project_id}}'
    target_bucket = "prd-mi-portal-1"
    source_file_modified_hours_back = 6 #can be overridden by config
    config_folder = f"{source_folder_root}\MI Portal Config"
else:
    project_id = 'dp-anly-ut-dev-xxx'
    lake_project_id = 'dp-anly-lake-xx'
    target_bucket = "uat-mi-portal-1"
    source_file_modified_hours_back = 6 #can be overridden by config
    config_folder = "C:\Buckets\MI Portal Config"

########## classes & functions ##########
def get_file_list(folder, include_subfolders = False, suffix = "", return_file_paths = True, return_file_names = False, return_file_directories = False, return_file_created_date = False, return_file_modified_date = False):
    import os

    #vars
    file_list = []

    try:
        #start file search
        for root, directories, files in os.walk(folder, topdown=True):
            for name in files:
                #if not including subfolders and not root skip
                if not include_subfolders and root!=folder:
                    continue
                
                #if file ends in suffix or no suffix given grab file
                if name.endswith(suffix) or suffix=="":                    
                    file = []
                    file_path = os.path.join(root, name)

                    #and decide what to return
                    if return_file_paths:
                        file.append(os.path.join(root, name))
                    if return_file_names:
                        file.append(name)
                    if return_file_directories:
                        file.append(root)
                    if return_file_created_date:
                        file.append(os.path.getctime(file_path))
                    if return_file_modified_date:
                        file.append(os.path.getmtime(file_path))                        

                    if len(file)==1:
                        file_list.append(file[0])
                    else:
                        file_list.append(file)

        return file_list

    except Exception as e:
        print(e)

def get_new_files_list(source_file_modified_hours_back=6):
    import time
    global source_file_suffixes, source_folder_root
    global get_file_list

    all_files = []
    for source_file_suffix in source_file_suffixes:
        all_files.extend(get_file_list(source_folder_root, True, source_file_suffix, True, return_file_created_date = True, return_file_modified_date = True))
    
    new_files = []
    for file, file_created_date, file_modified_date in all_files:
        if source_file_modified_hours_back==0:
            new_files.append([file,file_created_date])
            continue
        if round((time.time() - file_modified_date)/60/60) <= source_file_modified_hours_back:
            new_files.append([file,file_created_date,file_modified_date])

    return new_files

def get_reports(files):
    from datetime import datetime
    import re

    global config_dict

    reports = []
    for f in files:
        #split into folder/report list
        x = f[0].replace(source_folder_root,"").rsplit("\\",4)
        if len(x) < 3:
            continue
        
        #get base 
        extracted_filepath = f[0]
        extracted_file_suffix = extracted_filepath.split(".")[-1]
        extracted_brand = x[1]
        extracted_country=x[2]
        extracted_report_folder = x[3] if len(x)==5 else "*** CANNOT FIND ***"
        extracted_filename = x[4] if len(x)==5 else x[3]
        extracted_createdate = datetime.fromtimestamp(f[1]).isoformat()[:23]+'Z'
        extracted_modifydate = datetime.fromtimestamp(f[2]).isoformat()[:23]+'Z'

        #extract date from file name based on regexp
        extracted_filename_without_suffix = extracted_filename.replace(".{}".format(extracted_file_suffix),"")
        search_filename_yyyy_mm_dd = re.search("\d\d\d\d-\d\d-\d\d", extracted_filename_without_suffix)
        
        #get folder reports possible report
        config_folder_reports = {}
        try:
            config_folder_reports = config_dict["folderMap"][extracted_report_folder]["reportMap"]
        except:
            pass
        
        #loop through folder reports and get report vars
        lk_cluster_report_regexp = ""
        lk_country_breakdown = False
        lk_mi_portal_key = ""
        for report in config_folder_reports:
            lk_report_found = False
            lk_cluster_report_regexp = report.get("cluster_report_regexp","")
            if lk_cluster_report_regexp!="" and lk_cluster_report_regexp in extracted_filename:
                lk_report_found = True
            elif lk_cluster_report_regexp=="":
                lk_report_found = True
            
            if lk_report_found:
                lk_country_breakdown = report.get("country_breakdown",False)
                lk_mi_portal_key = report.get("mi-portal-key","")
                break

        #get derived components
        derived_report_date = "{}T00:00:00.000Z".format(search_filename_yyyy_mm_dd.group(0)) if search_filename_yyyy_mm_dd else ""
        derived_report_date_flag = False if not search_filename_yyyy_mm_dd or search_filename_yyyy_mm_dd == "" else True
        derived_cluster_report_flag = True if lk_cluster_report_regexp != "" else False
        
        #lookup codes    
        insights_brand_code = config_dict["brandMap"].get(extracted_brand,"")
        insights_report_code = lk_mi_portal_key
        insights_country_code = config_dict["countryMap"].get(extracted_country,"")

        #set final keys/locations
        mi_portal_filename = extracted_modifydate if derived_report_date=="" else derived_report_date
        mi_portal_key = ""
        mi_portal_file_key = ""
        insights_mapping_found = False if insights_brand_code=="" or insights_report_code=="" or insights_country_code=="" else True
        if insights_mapping_found and lk_country_breakdown:
            mi_portal_key = "{0}:{1}:{2}".format(insights_brand_code,insights_report_code,insights_country_code)
            mi_portal_file_key = r"{}/{}/{}/{}.{}".format(insights_brand_code,insights_report_code,insights_country_code,mi_portal_filename,extracted_file_suffix) 
        elif insights_mapping_found:
            mi_portal_key = "{0}:{1}".format(insights_brand_code,insights_report_code)
            mi_portal_file_key = r"{}/{}/{}/{}.{}".format(insights_brand_code,insights_report_code,"-",mi_portal_filename,extracted_file_suffix) 
           
        #build dictionary and append
        report = dict(filepath=extracted_filepath,brand=extracted_brand,country=extracted_country,report_folder=extracted_report_folder,filename=extracted_filename, \
        cluster_report_flag=derived_cluster_report_flag,report_date=derived_report_date,report_date_flag=derived_report_date_flag, \
        insights_brand_code = insights_brand_code, insights_country_code = insights_country_code, insights_report_code = insights_report_code, \
        mi_portal_key=mi_portal_key,mi_portal_file_key=mi_portal_file_key)
        reports.append(report)

    return reports

def get_config_dict():
    import json
    global config_folder

    config_dict_names = ["config", "brand_folders","country_folders","report_folders"]
    return_dicts = {}

    for c in config_dict_names:
        with open(f"{config_folder}\\{c}.json", "r") as json_file:
            data = json_file.read()
            json_data = [json.loads(str(item)) for item in data.strip().split('\n')]

        if c=="config":
            for l in json_data:
                return_dicts["configMap"] = {k:v for k,v in l.items()}
            global source_file_modified_hours_back
            source_file_modified_hours_back = int(return_dicts["configMap"].get("source_file_modified_hours_back", source_file_modified_hours_back))

        if c=="brand_folders": return_dicts["brandMap"] = {x["brand_folder"]:x["brand_code"] for x in json_data}

        if c=="country_folders": return_dicts["countryMap"] = {x["country_folder"]:x["country_code"] for x in json_data}

        if c=="report_folders":
            return_dicts["folderMap"] = {x["report_folder"]:{"reportMap":[]} for x in json_data if x["report_folder"]!=""}
            for x in json_data:
                if x["report_folder"]!="":
                    return_dicts["folderMap"][x["report_folder"]]["reportMap"].append({'mi-portal-key':x["report_code"], 'country_breakdown':True if x["country_breakdown"].strip().upper()=="TRUE" else False, 'cluster_report_regexp':x["cluster_report_regexp"].strip()})

    return return_dicts

########## main ##########

#get config
print("\nget config")
config_dict = get_config_dict()

#get files
print("\nget files")
files = get_new_files_list(source_file_modified_hours_back)

#get report
print("\nget reports")
reports = get_reports(files)

#upload files to aws bucket
print("\nupload files")
secret = GCP.access_secret_version(project_id,"connection_aws_mi_portal","latest")
secret_json = eval(secret)
s3_client = boto3.client("s3",aws_access_key_id=secret_json["access_key_id"],aws_secret_access_key=secret_json["secret_access_key"])

for r in reports:
    if r["mi_portal_file_key"]!="":
        try:
            print("\nuploading: {}".format(r["filepath"]))
            s3_client.upload_file(r["filepath"], target_bucket, r["mi_portal_file_key"])
        except:
            print("\n** error uploading: {}".format(r["filepath"]))

#log upload in BQ
client = bigquery.Client(project=lake_project_id)
table_id = client.dataset("Finance").table("MI_Portal_Report_Upload_Log")

result = [{"log_timestamp": datetime.now(), **item} for item in reports]
data = pd.DataFrame(result)

job_config = bigquery.LoadJobConfig(autodetect=True, write_disposition="WRITE_APPEND",
       time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="log_timestamp"
        ))
client.load_table_from_dataframe(data, table_id, job_config=job_config) #don'wait for job to finish           

client.query(f"""ALTER TABLE IF EXISTS `{lake_project_id}.Finance.MI_Portal_Report_Upload_Log`
    SET OPTIONS(
        require_partition_filter = true
    )""")

########## complete ##########
