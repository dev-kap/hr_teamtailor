import json
from os import getenv, listdir


class ExecutionMenu:

    def __init__(self):
        self._countries = None
        self._dataframe_creation_type = None
        self._endpoint_request_filter = None
        self._set_countries()
        # self._set_dataframe_creation_type()
        # self._set_endpoint_request_filter()

    def get_countries(self):
        return self._countries

    def get_dataframe_creation_type(self):
        return self._dataframe_creation_type

    def get_endpoint_request_filter(self):
        return self._endpoint_request_filter

    def _set_countries(self):
        # countries_selection_type = 0
        # while (countries_selection_type != "1") and (countries_selection_type != "2"):
        #     countries_selection_type = input(
        #         "\nQuestion: Do you want to generate the dataframe of which countries?\n"
        #         "  1 - All listed in input/tt_api_request_headers.json\n"
        #         "  2 - A specific country\n"
        #         "Answer: ")
        #     if (countries_selection_type != "1") and (countries_selection_type != "2"):
        #         print("Invalid answer!")
        countries = []
        for tt_api_request_header_file_name in sorted(listdir(getenv('TT_API_REQUEST_HEADERS_FILE_PATH'))):
            with open(getenv('TT_API_REQUEST_HEADERS_FILE_PATH') + "/" + tt_api_request_header_file_name) as json_textiowrapper:
                countries.append(list(json.load(json_textiowrapper).keys())[0])
        # if countries_selection_type == "2":
        #     while len(countries) > 1:
        #         country = input(
        #             "\nQuestion: Which country? (Allowed: {})\n".format(
        #                 ", ".join(countries[:-1]) + " and {}".format(countries[-1])) +
        #             "Answer: ")
        #         if country not in countries:
        #             print("Invalid country!")
        #         else:
        #             countries = [country]
        self._countries = countries

    def _set_dataframe_creation_type(self):
        dataframe_creation_type = 0
        while (dataframe_creation_type != "1") and (dataframe_creation_type != "2"):
            dataframe_creation_type = input(
                "\nQuestion: What do you want to do?\n"
                "  1 - Create a new dataframe\n"
                "  2 - Update a dataframe\n"
                "Answer: ")
            if (dataframe_creation_type != "1") and (dataframe_creation_type != "2"):
                print("Invalid answer!")
        self._dataframe_creation_type = dataframe_creation_type

    def _set_endpoint_request_filter(self):
        if self._dataframe_creation_type != "3":
            endpoint_request_filter_type = 0
            while (endpoint_request_filter_type != "1") and (endpoint_request_filter_type != "2") \
                    and (endpoint_request_filter_type != "3") and (endpoint_request_filter_type != "4"):
                endpoint_request_filter_type = input(
                    "\nQuestion: What endpoint request filter do you want to apply?\n"
                    "  1 - 'Created at from'\n"
                    "  2 - 'Created at from' and 'created at to'\n"
                    "  3 - 'Updated at from'\n"
                    "  4 - 'Updated at from' and 'updated at to'\n"
                    "Answer: ")
                if (endpoint_request_filter_type != "1") and (endpoint_request_filter_type != "2") \
                        and (endpoint_request_filter_type != "3") and (endpoint_request_filter_type != "4"):
                    print("Invalid answer!")
            if endpoint_request_filter_type == "1":
                endpoint_request_filter = {"created_at_from": None}
            elif endpoint_request_filter_type == "2":
                endpoint_request_filter = {"created_at_from": None, "created_at_to": None}
            elif endpoint_request_filter_type == "3":
                endpoint_request_filter = {"updated_at_from": None}
            else:
                endpoint_request_filter = {"updated_at_from": None, "updated_at_to": None}
            for endpoint_request_filter_type in list(endpoint_request_filter.keys()):
                endpoint_request_filter_date_type = 0
                while (endpoint_request_filter_date_type != "1") and (endpoint_request_filter_date_type != "2") \
                        and (endpoint_request_filter_date_type != "3"):
                    endpoint_request_filter_date_type = input(
                        "\nQuestion: Which date type for '{}' endpoint request filter?\n".format(endpoint_request_filter_type.capitalize().replace("_", " ")) +
                        "  1 - Specific date\n"
                        "  2 - The earliest from the {} column of the dataframe\n".format(endpoint_request_filter_type[:10].replace("_", "-")) +
                        "  3 - The latest from the {} column of the dataframe\n".format(endpoint_request_filter_type[:10].replace("_", "-")) +
                        "Answer: ")
                    if (endpoint_request_filter_date_type != "1") and (endpoint_request_filter_date_type != "2") \
                            and (endpoint_request_filter_date_type != "3"):
                        print("Invalid answer!")
                endpoint_request_filter_date = ""
                if endpoint_request_filter_date_type == "1":
                    while (len(endpoint_request_filter_date.split("-")[0]) != 4) \
                            or (not endpoint_request_filter_date.split("-")[0].isdigit()) \
                            or (len(endpoint_request_filter_date.split("-")[1]) != 2) \
                            or (not endpoint_request_filter_date.split("-")[1].isdigit()) \
                            or (len(endpoint_request_filter_date.split("-")[2]) != 2) \
                            or (not endpoint_request_filter_date.split("-")[2].isdigit()):
                        endpoint_request_filter_date = input(
                            "\nQuestion: Which specific date? (Format example: YYYY-MM-DD)?\n"
                            "Answer: ")
                        break
                        # if (len(endpoint_request_filter_date.split("-")[0]) != 4) \
                        #         or (not endpoint_request_filter_date.split("-")[0].isdigit()) \
                        #         or (len(endpoint_request_filter_date.split("-")[1]) != 2) \
                        #         or (not endpoint_request_filter_date.split("-")[1].isdigit()) \
                        #         or (len(endpoint_request_filter_date.split("-")[2]) != 2) \
                        #         or (not endpoint_request_filter_date.split("-")[2].isdigit()):
                        #     print("Invalid date!")
                elif endpoint_request_filter_date_type == "2":
                    endpoint_request_filter_date = "earliest"
                else:
                    endpoint_request_filter_date = "latest"
                endpoint_request_filter[endpoint_request_filter_type] = endpoint_request_filter_date
            self._endpoint_request_filter = endpoint_request_filter
