from ast import literal_eval
import pandas
from python_scripts.utils.azure_storage import AzureStorage
from python_scripts.utils.tt_api import TeamTailorAPI


class TTCompiledDataframe:

    def __init__(self):
        self._dataframe = None
        self._dataframe_data = None
        self._dataframe_setting_status = None

    def get_dataframe_setting_status(self):
        return self._dataframe_setting_status

    def set(self, **kwargs):
        kwargs["execution_datetimes"].set_current_country_data_setting_start_datetime()
        if kwargs["verbose"] > 0:
            kwargs["execution_prints"].current_country_data_setting_start(country=kwargs["country"])
        azure_storage = AzureStorage()
        teamtailor_api = TeamTailorAPI()
        teamtailor_api.set_endpoint_request_headers(country=kwargs["country"])
        blob_names = azure_storage.get_blob_names(container_name="teamtailordataframes", country=kwargs['country'])
        if len(blob_names) == 0:
            input_dataframe = None
            endpoint_request_parameters = dict(created_at_from='2021-01-01')
        elif len(blob_names) == 1:
            input_csv_file_name = blob_names[0]
            csv_blob = azure_storage.download_csv_blob_as_dataframe(
                container_name="teamtailordataframes", blob_name=input_csv_file_name)
            input_dataframe = pandas.read_csv(csv_blob, converters={'candidate-tags': literal_eval}, keep_default_na=False)
            input_dataframe[input_dataframe.drop(columns='candidate-tags').columns.to_list()] = input_dataframe[
                input_dataframe.drop(columns='candidate-tags').columns.to_list()].astype(str)
            azure_storage.upload_dataframe_as_csv_blob(
                container_name="teamtailordataframesbackup", blob_name=input_csv_file_name, dataframe=input_dataframe)
            endpoint_request_parameters = dict(
                updated_at_from=f"{input_csv_file_name[-14:-10]}-{input_csv_file_name[-9:-7]}-{input_csv_file_name[-6:-4]}")
        else:
            raise ValueError(
                f"There are {len(blob_names)} blobs from {kwargs['country']} "
                f"({', '.join(blob_names[:-1])} and {blob_names[-1]})")
        try:
            page_count_of_country_job_applications_api_endpoint_response = teamtailor_api.get_endpoint_request_response(
                endpoint_url="https://api.teamtailor.com/v1/job-applications",
                endpoint_request_parameters=endpoint_request_parameters)["meta"]["page-count"]
            self._set_dataframe_data_template()
            for current_page_of_country_job_applications_api_endpoint_response \
                    in range(1, page_count_of_country_job_applications_api_endpoint_response + 1):
                kwargs["execution_datetimes"].set_start_datetime_of_job_applications_api_endpoint_response_current_page()
                if endpoint_request_parameters:
                    endpoint_request_parameters["page_number"] = current_page_of_country_job_applications_api_endpoint_response
                else:
                    endpoint_request_parameters = dict(page_number=current_page_of_country_job_applications_api_endpoint_response)
                job_applications_api_response = teamtailor_api.get_endpoint_request_response(
                    endpoint_url="https://api.teamtailor.com/v1/job-applications",
                    endpoint_request_parameters=endpoint_request_parameters)
                for row_index, job_application_data in enumerate(job_applications_api_response["data"], 1):
                    job_api_response = teamtailor_api.get_endpoint_request_response(
                        endpoint_url=job_application_data["relationships"]["job"]["links"]["related"])
                    candidate_api_response = teamtailor_api.get_endpoint_request_response(
                        endpoint_url=job_application_data["relationships"]["candidate"]["links"]["related"])
                    self._set_dataframe_data(
                        candidate_api_response=candidate_api_response, country=kwargs["country"],
                        job_api_response=job_api_response, job_application_data=job_application_data,
                        teamtailor_api=teamtailor_api)
                    kwargs["execution_datetimes"].set_current_page_end_datetime()
                    if kwargs["verbose"] > 1:
                        kwargs["execution_prints"].status_of_country_data_setting(
                            country=kwargs["country"], execution_datetimes=kwargs["execution_datetimes"],
                            page=current_page_of_country_job_applications_api_endpoint_response,
                            page_count=page_count_of_country_job_applications_api_endpoint_response,
                            row_index=row_index, number_of_rows=len(job_applications_api_response["data"]))
                kwargs["execution_datetimes"].set_current_page_end_datetimes(
                    page=current_page_of_country_job_applications_api_endpoint_response,
                    page_count=page_count_of_country_job_applications_api_endpoint_response)
            self._create_dataframe()
            if kwargs["verbose"] > 0:
                kwargs["execution_prints"].current_country_data_setting_end(country=kwargs["country"])
            if input_dataframe is not None:
                if self._dataframe.empty:
                    self._dataframe = input_dataframe
                    print(f"    Generating '{kwargs['country']}' dataframe (input dataframe only)... done!")
                else:
                    self._dataframe = pandas.concat([input_dataframe, self._dataframe], ignore_index=True).drop_duplicates(
                        ['country', 'job_application_id', 'candidate_id'], keep='last', ignore_index=True)
                    print(f"    Generating '{kwargs['country']}' dataframe (input dataframe concatenated with an updated dataframe)... done!")
                azure_storage.delete_blob(container_name="teamtailordataframes", blob_name=input_csv_file_name)
            else:
                if self._dataframe.empty:
                    print(f"    Generating '{kwargs['country']}' dataframe (new empty dataframe)... done!")
                else:
                    print(f"    Generating '{kwargs['country']}' dataframe (new dataframe)... done!")
            if not self._dataframe.empty:
                azure_storage.upload_dataframe_as_csv_blob(
                    container_name="teamtailordataframes", dataframe=self._dataframe,
                    blob_name="job_application_dataframe_{}_{}_{}_{}.csv".format(
                        kwargs["country"].lower().replace(' ', '_'),
                        kwargs["execution_datetimes"].get_execution_start_datetime().year,
                        str(kwargs["execution_datetimes"].get_execution_start_datetime().month).zfill(2),
                        str(kwargs["execution_datetimes"].get_execution_start_datetime().day).zfill(2)))
                print("'{}' dataframe generated and uploaded!".format(kwargs["country"]))
            else:
                print("'{}' dataframe generated but not uploaded (empty dataframe)!".format(kwargs["country"]))
        except:
            print("\n    An error occurred during execution")
        kwargs["execution_datetimes"].set_current_country_data_setting_end_datetime()
        kwargs["execution_datetimes"].initialize_average_time_to_set_page_data()

    def _create_dataframe(self):
        self._dataframe = pandas.DataFrame({
            "country": self._dataframe_data["country"], "job_application_id": self._dataframe_data["id"],
            "candidate_id": self._dataframe_data["fk_candidate"], "job_id": self._dataframe_data["fk_job"],
            "job-title": self._dataframe_data["job-title"], **self._dataframe_data["attributes"],
            "candidate-name": self._dataframe_data["candidate-name"], "department_name": self._dataframe_data["department_name"],
            "role_name": self._dataframe_data["role_name"], "user_name": self._dataframe_data["user_name"],
            "stage-type": self._dataframe_data["stage-type"], "stage-name": self._dataframe_data["stage-name"],
            "reject-reason": self._dataframe_data["reject-reason"], "job-created-at": self._dataframe_data["job-created-at"],
            "job-human-status": self._dataframe_data["job-human-status"], "candidate-tags": self._dataframe_data["candidate-tags"],
            "location": self._dataframe_data["location"], "stage-change-user": self._dataframe_data["stage-change-user"]})

    @staticmethod
    def _save_dataframe(**kwargs):
        kwargs["dataframe"].to_csv(kwargs["output_csv_file_folder_path"] + "/" + kwargs["output_csv_file_name"], index=False)

    def _set_dataframe_data(self, **kwargs):

        def set_attributes_in_dataframe_data(dataframe_data, job_application_data):
            for attribute in dataframe_data["attributes"].keys():
                if job_application_data["attributes"][attribute]:
                    dataframe_data["attributes"][attribute].append(job_application_data["attributes"][attribute])
                else:
                    dataframe_data["attributes"][attribute].append("")

        def set_candidate_tags_in_dataframe_data(dataframe_data, candidate_api_response):
            dataframe_data["candidate-tags"].append(candidate_api_response["data"]["attributes"]["tags"])

        def set_country_in_dataframe_data(dataframe_data, country):
            dataframe_data["country"].append(country)

        def set_department_location_role_and_user_names_in_dataframe_data(dataframe_data, job_api_response, teamtailor_api):
            for element in ["department", "location", "role", "user"]:
                element_api_response = teamtailor_api.get_endpoint_request_response(
                    endpoint_url=job_api_response["data"]["relationships"][element]["links"]["related"])
                if element == "location":
                    column_name = element
                else:
                    column_name = f"{element}_name"
                if element_api_response["data"]:
                    dataframe_data[column_name].append(element_api_response["data"]["attributes"]["name"])
                else:
                    dataframe_data[column_name].append("")

        def set_fk_candidate_and_candidate_name_in_dataframe_data(dataframe_data, candidate_api_response):
            if candidate_api_response["data"]:
                dataframe_data["fk_candidate"].append(candidate_api_response["data"]["id"])
                if candidate_api_response["data"]["attributes"]["first-name"]:
                    first_name = candidate_api_response["data"]["attributes"]["first-name"]
                else:
                    first_name = None
                if candidate_api_response["data"]["attributes"]["last-name"]:
                    last_name = candidate_api_response["data"]["attributes"]["last-name"]
                else:
                    last_name = None
                if first_name and last_name:
                    name = first_name + " " + last_name
                else:
                    if first_name:
                        name = first_name
                    else:
                        name = last_name
                if name:
                    dataframe_data["candidate-name"].append(name.replace("  ", " ").title())
                else:
                    dataframe_data["candidate-name"].append("")
            else:
                dataframe_data["candidate-name"].append("")

        def set_fk_job_in_dataframe_data(dataframe_data, job_api_response):
            dataframe_data["fk_job"].append(job_api_response["data"]["id"])

        def set_id_in_dataframe_data(dataframe_data, job_application_data_id):
            dataframe_data["id"].append(job_application_data_id)

        def set_job_created_at_in_dataframe_data(dataframe_data, job_api_response):
            dataframe_data["job-created-at"].append(job_api_response["data"]["attributes"]["created-at"])

        def set_job_human_status_in_dataframe_data(dataframe_data, job_api_response):
            dataframe_data["job-human-status"].append(job_api_response["data"]["attributes"]["human-status"])

        def set_job_title_in_dataframe_data(dataframe_data, job_api_response):
            dataframe_data["job-title"].append(job_api_response["data"]["attributes"]["title"])

        def set_reject_reason_in_dataframe_data(dataframe_data, endpoint_url, teamtailor_api):
            reject_reason_api_response = teamtailor_api.get_endpoint_request_response(endpoint_url=endpoint_url)
            if reject_reason_api_response["data"]:
                dataframe_data["reject-reason"].append(reject_reason_api_response["data"]["attributes"]["reason"])
            else:
                dataframe_data["reject-reason"].append("")

        def set_stage_type_and_name_in_dataframe_data(dataframe_data, endpoint_url, teamtailor_api):
            stage_api_response = teamtailor_api.get_endpoint_request_response(endpoint_url=endpoint_url)
            dataframe_data["stage-type"].append(stage_api_response["data"]["attributes"]["stage-type"])
            dataframe_data["stage-name"].append(stage_api_response["data"]["attributes"]["name"])

        def set_recruiter_in_dataframe_data(dataframe_data, candidate_api_response, teamtailor_api):
            page_count_of_activities_api_endpoint_response = teamtailor_api.get_endpoint_request_response(
                endpoint_url=candidate_api_response["data"]["relationships"]["activities"]["links"]["related"]
            )["meta"]["page-count"]
            stage_change_user = ""
            for current_page_count_of_activities_api_endpoint_response \
                    in range(1, page_count_of_activities_api_endpoint_response + 1):
                activities_api_endpoint_response = teamtailor_api.get_endpoint_request_response(
                    endpoint_url=candidate_api_response["data"]["relationships"]["activities"]["links"]["related"],
                    endpoint_request_parameters=dict(page_number=current_page_count_of_activities_api_endpoint_response))
                for activities_api_endpoint_response_data in activities_api_endpoint_response["data"]:
                    if activities_api_endpoint_response_data["attributes"]["code"] == "stage":
                        user_api_response = teamtailor_api.get_endpoint_request_response(
                            endpoint_url=activities_api_endpoint_response_data["relationships"]["user"]["links"]["related"])
                        if user_api_response["data"]:
                            stage_change_user = user_api_response["data"]["attributes"]["name"]
                        else:
                            stage_change_user = ""
            dataframe_data["stage-change-user"].append(stage_change_user)

        set_attributes_in_dataframe_data(self._dataframe_data, kwargs["job_application_data"])
        set_country_in_dataframe_data(self._dataframe_data, kwargs["country"])
        set_id_in_dataframe_data(self._dataframe_data, kwargs["job_application_data"]["id"])
        set_fk_candidate_and_candidate_name_in_dataframe_data(self._dataframe_data, kwargs["candidate_api_response"])
        set_candidate_tags_in_dataframe_data(self._dataframe_data, kwargs["candidate_api_response"])
        set_fk_job_in_dataframe_data(self._dataframe_data, kwargs["job_api_response"])
        set_job_title_in_dataframe_data(self._dataframe_data, kwargs["job_api_response"])
        set_job_created_at_in_dataframe_data(self._dataframe_data, kwargs["job_api_response"])
        set_job_human_status_in_dataframe_data(self._dataframe_data, kwargs["job_api_response"])
        set_department_location_role_and_user_names_in_dataframe_data(
            self._dataframe_data, kwargs["job_api_response"], kwargs["teamtailor_api"])
        set_stage_type_and_name_in_dataframe_data(
            self._dataframe_data, kwargs["job_application_data"]["relationships"]["stage"]["links"]["related"],
            kwargs["teamtailor_api"])
        set_reject_reason_in_dataframe_data(
            self._dataframe_data, kwargs["job_application_data"]["relationships"]["reject-reason"]["links"]["related"],
            kwargs["teamtailor_api"])
        set_recruiter_in_dataframe_data(self._dataframe_data, kwargs["candidate_api_response"], kwargs["teamtailor_api"])

    def _set_dataframe_data_template(self):
        self._dataframe_data = {
            "country": [], "id": [], "fk_candidate": [], "fk_job": [],
            "attributes": {"created-at": [], "referring-site": [], "rejected-at": [], "updated-at": [], "changed-stage-at": []},
            "candidate-name": [], "job-title": [], "department_name": [], "role_name": [], "user_name": [], "stage-type": [],
            "stage-name": [], "reject-reason": [], "job-created-at": [], "job-human-status": [], "candidate-tags": [],
            "location": [], "stage-change-user": []}
