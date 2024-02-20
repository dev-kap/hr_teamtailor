from os import getcwd, getenv
import json
import requests


class TeamTailorAPI:

    def __init__(self):
        self._endpoint_request_headers = None

    @staticmethod
    def _get_endpoint_request_parameters(**kwargs):
        endpoint_request_parameters = {"page[size]": "30"}
        if "endpoint_request_parameters" in kwargs:
            if "created_at_from" in kwargs["endpoint_request_parameters"]:
                endpoint_request_parameters["filter[created-at][from]"] = kwargs["endpoint_request_parameters"]["created_at_from"]
            if "created_at_to" in kwargs["endpoint_request_parameters"]:
                endpoint_request_parameters["filter[created-at][to]"] = kwargs["endpoint_request_parameters"]["created_at_to"]
            if "page_number" in kwargs["endpoint_request_parameters"]:
                endpoint_request_parameters["page[number]"] = kwargs["endpoint_request_parameters"]["page_number"]
            if "updated_at_from" in kwargs["endpoint_request_parameters"]:
                endpoint_request_parameters["filter[updated-at][from]"] = kwargs["endpoint_request_parameters"]["updated_at_from"]
            if "updated_at_to" in kwargs["endpoint_request_parameters"]:
                endpoint_request_parameters["filter[updated-at][to]"] = kwargs["endpoint_request_parameters"]["updated_at_to"]
        return endpoint_request_parameters

    def get_endpoint_request_response(self, **kwargs):
        status_code = 0
        while status_code != 200:
            endpoint_request_response = requests.get(
                kwargs["endpoint_url"], headers=self._endpoint_request_headers,
                params=self._get_endpoint_request_parameters(**kwargs))
            status_code = endpoint_request_response.status_code
        return endpoint_request_response.json()

    def set_endpoint_request_headers(self, **kwargs):
        tt_api_request_header_file_name = getenv('TT_API_REQUEST_HEADERS_FILE_PATH') + "/tt_api_request_header_{}.json".format(
            kwargs["country"].lower().replace(" ", "_"))
        with open(tt_api_request_header_file_name) as json_textiowrapper:
            self._endpoint_request_headers = json.load(json_textiowrapper)[kwargs["country"]]
