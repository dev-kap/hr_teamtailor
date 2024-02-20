from os import environ, getcwd


def set_environment_variables():

    def set_paths():
        environ['INPUT_CSV_FILE_FOLDER_PATH'] = getcwd() + "/data/dataframes"
        environ['EXECUTION_LOG_FILE_PATH'] = getcwd() + "/data/execution_logs"
        environ['TT_API_REQUEST_HEADERS_FILE_PATH'] = getcwd() + "/tt_api_request_headers"

    set_paths()
