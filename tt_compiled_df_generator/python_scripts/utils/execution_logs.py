from os import getenv
import json


class ExecutionLogs:

    def __init__(self, country):
        self._execution_logs = None
        self._file_path = f"{getenv('EXECUTION_LOG_FILE_PATH')}/execution_logs_{country.lower().replace(' ', '_')}.json"
        self._read_file()

    def add_current(self, endpoint_request_filters, execution_datetimes, execution_status):
        self._execution_logs.append({
            'ExecutionDatetimes': execution_datetimes,
            'EndpointRequestFilters': endpoint_request_filters,
            'ExecutionStatus': execution_status})

    def get_current_endpoint_request_filters(self, execution_date_start):
        return {
            'updated_at_from': [
                value for key, value in self._execution_logs[-1]['EndpointRequestFilters'].items() if 'at_to' in key][0],
            'updated_at_to': execution_date_start}

    def get_last(self):
        return self._execution_logs[-1]

    def _read_file(self):
        with open(self._file_path) as json_text_io_wrapper:
            self._execution_logs = json.load(json_text_io_wrapper)

    def write_file(self):
        with open(self._file_path, "w") as json_text_io_wrapper:
            json_text_io_wrapper.write(json.dumps(self._execution_logs, indent=4))
