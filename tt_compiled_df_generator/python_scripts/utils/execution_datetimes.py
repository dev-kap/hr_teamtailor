import datetime


class ExecutionDatetimes:

    def __init__(self):
        self._average_time_to_set_page_data = None
        self._average_time_to_set_the_current_row_of_the_new_column = None
        self._current_country_data_setting_end_datetime = None
        self._current_country_data_setting_start_datetime = None
        self._current_page_end_datetime = None
        self._end_datetime_of_job_applications_api_endpoint_response_current_page = None
        self._end_datetime_of_setting_the_current_row_of_the_new_column = None
        self._start_datetime_of_job_applications_api_endpoint_response_current_page = None
        self._start_datetime_of_setting_the_current_row_of_the_new_column = None
        self._execution_end_datetime = None
        self._execution_start_datetime = None
        self._expected_end_datetime = None
        self._expected_end_datetime_of_adding_the_new_column = None
        self._last_end_datetime_of_setting_the_current_row_of_the_new_column = None
        self._last_page_end_datetime = None
        self._timedeltas_to_set_page_data = []
        self._timedeltas_to_set_the_current_row_of_the_new_column = []

    def initialize_average_time_to_set_page_data(self):
        self._average_time_to_set_page_data = None

    def initialize_current_country_data_setting_datetimes(self):
        self._current_country_data_setting_end_datetime = None
        self._current_country_data_setting_start_datetime = None

    def get_average_time_to_set_page_data(self):
        return self._average_time_to_set_page_data

    def get_average_time_to_set_the_current_row_of_the_new_column(self):
        return self._average_time_to_set_the_current_row_of_the_new_column

    def get_current_country_data_setting_end_datetime(self):
        return self._current_country_data_setting_end_datetime

    def get_current_country_data_setting_start_datetime(self):
        return self._current_country_data_setting_start_datetime

    def get_current_page_days_hours_minutes_seconds(self):
        current_page_days = self._timedeltas_to_set_page_data[-1].days
        current_page_hours, remainder = divmod(self._timedeltas_to_set_page_data[-1].seconds, 3600)
        current_page_minutes, current_page_seconds = divmod(remainder, 60)
        return {
            "days": current_page_days,
            "hours": current_page_hours,
            "minutes": current_page_minutes,
            "seconds": current_page_seconds
        }

    def get_current_page_end_datetime(self):
        return self._current_page_end_datetime

    def get_elapsed_days_hours_minutes_seconds(self):
        elapsed_datetime = self._current_page_end_datetime - self._execution_start_datetime
        elapsed_days = elapsed_datetime.days
        elapsed_hours, remainder = divmod(elapsed_datetime.seconds, 3600)
        elapsed_minutes, elapsed_seconds = divmod(remainder, 60)
        return {"days": elapsed_days, "hours": elapsed_hours, "minutes": elapsed_minutes, "seconds": elapsed_seconds}

    def get_end_datetime_of_job_applications_api_endpoint_response_current_page(self):
        return self._end_datetime_of_job_applications_api_endpoint_response_current_page

    def get_end_datetime_of_setting_the_current_row_of_the_new_column(self):
        return self._end_datetime_of_setting_the_current_row_of_the_new_column

    def get_execution_end_datetime(self):
        return self._execution_end_datetime

    def get_execution_start_datetime(self):
        return self._execution_start_datetime

    def get_expected_end_datetime(self):
        return self._expected_end_datetime.replace(microsecond=0)

    def get_remaining_current_country_data_setting_time(self):
        remaining_execution_timedelta = self._expected_end_datetime - self._end_datetime_of_job_applications_api_endpoint_response_current_page
        remaining_execution_days = remaining_execution_timedelta.days
        remaining_execution_hours, remainder = divmod(remaining_execution_timedelta.seconds, 3600)
        remaining_execution_minutes, time_remaining_timedelta_seconds = divmod(remainder, 60)
        return "{}h{}min".format(
            24 * remaining_execution_days + remaining_execution_hours,
            remaining_execution_minutes
        )

    def get_remaining_current_country_new_column_adding_time(self):
        remaining_execution_timedelta = self._expected_end_datetime_of_adding_the_new_column - self._end_datetime_of_setting_the_current_row_of_the_new_column
        remaining_execution_days = remaining_execution_timedelta.days
        remaining_execution_hours, remainder = divmod(remaining_execution_timedelta.seconds, 3600)
        remaining_execution_minutes, time_remaining_timedelta_seconds = divmod(remainder, 60)
        return "{}h{}min".format(
            24 * remaining_execution_days + remaining_execution_hours,
            remaining_execution_minutes
        )

    def get_remaining_execution_days_hours_minutes(self):
        remaining_execution_timedelta = self._expected_end_datetime - self._current_page_end_datetime
        remaining_execution_days = remaining_execution_timedelta.days
        remaining_execution_hours, remainder = divmod(remaining_execution_timedelta.seconds, 3600)
        remaining_execution_minutes, time_remaining_timedelta_seconds = divmod(remainder, 60)
        return {
            "days": remaining_execution_days,
            "hours": remaining_execution_hours,
            "minutes": remaining_execution_minutes,
            "seconds": time_remaining_timedelta_seconds
        }

    def get_start_datetime_of_setting_the_current_row_of_the_new_column(self):
        return self._start_datetime_of_setting_the_current_row_of_the_new_column

    def set_average_time_to_set_page_data(self):
        self._average_time_to_set_page_data = sum(
            self._timedeltas_to_set_page_data, datetime.timedelta(0)
        ) / len(self._timedeltas_to_set_page_data)

    def set_average_time_to_set_the_current_row_of_the_new_column(self):
        self._average_time_to_set_the_current_row_of_the_new_column = sum(
            self._timedeltas_to_set_the_current_row_of_the_new_column, datetime.timedelta(0)
        ) / len(self._timedeltas_to_set_the_current_row_of_the_new_column)

    def set_current_page_end_datetimes(self, **kwargs):
        self.set_end_datetime_of_job_applications_api_endpoint_response_current_page()
        self.set_timedeltas_to_set_page_data()
        self.set_average_time_to_set_page_data()
        self.set_last_page_end_datetime()
        self.set_expected_end_datetime(page=kwargs["page"], page_count=kwargs["page_count"])

    def set_timedeltas_to_set_page_data(self):
        self._timedeltas_to_set_page_data.append(
            self._end_datetime_of_job_applications_api_endpoint_response_current_page -
            self._start_datetime_of_job_applications_api_endpoint_response_current_page
        )

    def set_timedeltas_to_set_the_current_row_of_the_new_column(self):
        self._timedeltas_to_set_the_current_row_of_the_new_column.append(
            self._end_datetime_of_setting_the_current_row_of_the_new_column -
            self._start_datetime_of_setting_the_current_row_of_the_new_column
        )

    def set_expected_end_datetime(self, **kwargs):
        remaining_pages = kwargs["page_count"] - kwargs["page"]
        self._expected_end_datetime = (
            self._end_datetime_of_job_applications_api_endpoint_response_current_page
            + self._average_time_to_set_page_data * remaining_pages
        )

    def set_expected_end_datetime_of_adding_the_new_column(self, **kwargs):
        remaining_pages = kwargs["number_of_rows"] - kwargs["row_index"]
        self._expected_end_datetime_of_adding_the_new_column = (
            self._end_datetime_of_setting_the_current_row_of_the_new_column
            + self._average_time_to_set_the_current_row_of_the_new_column * remaining_pages)

    def set_last_page_end_datetime(self):
        self._last_page_end_datetime = self._current_page_end_datetime

    def set_last_end_datetime_of_setting_the_current_row_of_the_new_column(self):
        self._last_end_datetime_of_setting_the_current_row_of_the_new_column = self._end_datetime_of_setting_the_current_row_of_the_new_column

    def set_end_datetime_of_job_applications_api_endpoint_response_current_page(self):
        self._end_datetime_of_job_applications_api_endpoint_response_current_page = datetime.datetime.now().replace(microsecond=0)

    def set_end_datetime_of_setting_the_current_row_of_the_new_column(self):
        self._end_datetime_of_setting_the_current_row_of_the_new_column = datetime.datetime.now().replace(microsecond=0)

    def set_start_datetime_of_job_applications_api_endpoint_response_current_page(self):
        self._start_datetime_of_job_applications_api_endpoint_response_current_page = datetime.datetime.now().replace(microsecond=0)

    def set_start_datetime_of_setting_the_current_row_of_the_new_column(self):
        self._start_datetime_of_setting_the_current_row_of_the_new_column = datetime.datetime.now().replace(microsecond=0)

    def set_current_page_end_datetime(self):
        self._current_page_end_datetime = datetime.datetime.now().replace(microsecond=0)

    def set_current_country_data_setting_end_datetime(self):
        self._current_country_data_setting_end_datetime = datetime.datetime.now().replace(microsecond=0)

    def set_current_country_data_setting_start_datetime(self):
        self._current_country_data_setting_start_datetime = datetime.datetime.now().replace(microsecond=0)

    def set_execution_end_datetime(self, **kwargs):
        self._execution_end_datetime = self._current_page_end_datetime
        if kwargs["verbose"]:
            kwargs["execution_prints"].execution_end_datetime(self._execution_end_datetime)

    def set_execution_start_datetime(self, **kwargs):
        self._execution_start_datetime = datetime.datetime.now().replace(microsecond=0)
        self._last_page_end_datetime = self._execution_start_datetime
        if kwargs["verbose"]:
            kwargs["execution_prints"].execution_start_datetime(self._execution_start_datetime)
