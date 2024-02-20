import json
import requests
import pandas
import _snowflake
import snowflake.snowpark as snowpark
session = requests.Session()

endpointrequestheader = ''
df = None
df_data = None

def runJson():
        credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
        return credentials

def set_dataframe(df_data, **kwargs):
        
        def set_attributes_in_dataframe_data(job_application_data):
                for attributes in df_data["attributes"].keys():
                        if job_application_data["attributes"][attributes]:
                                df_data["attributes"][attributes].append(job_application_data["attributes"][attributes])
                        else:
                                df_data["attributes"][attributes].append("")

        def set_id_in_dataframe_data(job_application_data_id):
                df_data["id"].append(job_application_data_id)
        
        def set_fk_candidate_and_candidate_name_in_dataframe_data(candidate_api_reponse):
                if candidate_api_reponse["data"]:
                        df_data["fk_candidate"].append(candidate_api_reponse["data"]["id"])
                        if candidate_api_reponse["data"]["attributes"]["first-name"]:
                                first_name = candidate_api_reponse["data"]["attributes"]["first-name"]
                        else :
                                first_name = None
                        if candidate_api_reponse["data"]["attributes"]["last-name"]:
                                last_name = candidate_api_reponse["data"]["attributes"]["last-name"]
                        else:
                                last_name = None
                        if first_name and last_name:
                                name = first_name + " " + last_name
                        else:
                                name = last_name
                        if name:
                                df_data["candidate-name"].append(name.replace("  ", " ").title())
                        else : 
                                df_data["candidate-name"].append("")
                else :
                        df_data["candidate-name"].append("")
        
        set_attributes_in_dataframe_data(kwargs["job_application_data"])
        set_id_in_dataframe_data(kwargs["job_application_data"]["id"])
        set_fk_candidate_and_candidate_name_in_dataframe_data(kwargs["candidate_api_reponse"])

def creat_dataframe(df_data):
        df = pandas.DataFrame(
                {
                        "job_application_id": df_data["id"], "candidate_id": df_data["fk_candidate"],
                        "candidate-name": df_data["candidate-name"]
                }
        )
        return df

def get_endpoint():
        endpointrequestheader = runJson()
        endpoint_url="https://api.teamtailor.com/v1/job-applications" 
        endpoint_request_parameters = dict(created_at_from='2024-01-01')
        status_code = 0
        while status_code != 200:
                #endpoint_request_response = requests.get(endpoint_url,headers=endpointrequestheader,params=endpoint_request_parameters)
                endpoint_request_response = requests.get(endpoint_url,headers=endpointrequestheader,params=endpoint_request_parameters)
                #print(endpoint_request_response.status_code, endpoint_request_response)
                status_code = endpoint_request_response.status_code

        page_count_of_country_job_applications_api_endpoint_response = endpoint_request_response.json()["meta"]["page-count"]
        
        df_data = {
            "country": [], "id": [], "fk_candidate": [], "fk_job": [],
            "attributes": {"created-at": [], "referring-site": [], "rejected-at": [], "updated-at": [], "changed-stage-at": []},
            "candidate-name": [], "job-title": [], "department_name": [], "role_name": [], "user_name": [], "stage-type": [],
            "stage-name": [], "reject-reason": [], "job-created-at": [], "job-human-status": [], "candidate-tags": [],
            "location": [], "stage-change-user": []
        } 

        for current_page_of_country_job_applications_api_endpoint_response in range(1,10):
                try:
                        if endpoint_request_parameters:
                                endpoint_request_parameters["page_number"] = current_page_of_country_job_applications_api_endpoint_response
                        else:
                                endpoint_request_parameters = dict(page_number=current_page_of_country_job_applications_api_endpoint_response)
                        job_applications_api_response = requests.get(endpoint_url,headers=endpointrequestheader,params=endpoint_request_parameters)
                        
                        for row_index, job_application_data in enumerate(job_applications_api_response.json()["data"],1):
                                job_api_reponse = requests.get(job_application_data["relationships"]["job"]["links"]["related"],headers=endpointrequestheader ).json()     
                                candidate_api_response = requests.get(job_application_data["relationships"]["candidate"]["links"]["related"],headers=endpointrequestheader ).json()  
                                set_dataframe(df_data, candidate_api_reponse= candidate_api_response, job_api_reponse = job_api_reponse, job_application_data = job_application_data)
                except:
                        pass

        return creat_dataframe(df_data)    


def newdate(session: snowpark.Session):
        df = get_endpoint()
        table_name = "test"
        session.write_pandas(df,tab, table_name, auto_create_table = True, overwrite= True)