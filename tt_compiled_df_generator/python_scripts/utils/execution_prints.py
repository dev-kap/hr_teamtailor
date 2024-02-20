class ExecutionPrints:

    @staticmethod
    def current_country_data_setting_end(**kwargs):
        print(f"\r    Setting '{kwargs['country']}' dataframe data... done!                                                                                ")

    @staticmethod
    def current_country_data_setting_start(**kwargs):
        print(f"\nGenerating and uploading '{kwargs['country']}' dataframe...")

    @staticmethod
    def execution_end_datetime(execution_end_datetime):
        print(f"\nExecution end datetime: {execution_end_datetime}")

    @staticmethod
    def execution_start_datetime(execution_start_datetime):
        print(f"Execution start datetime: {execution_start_datetime}")

    @staticmethod
    def status_of_country_column_adding(**kwargs):
        if kwargs["execution_datetimes"].get_average_time_to_set_the_current_row_of_the_new_column():
            print(
                "\r    Status: Row {}/{} (Average time to set page data: {}s; Ends in approximately {})".format(
                    kwargs["row_index"], kwargs["number_of_rows"],
                    int(kwargs["execution_datetimes"].get_average_time_to_set_the_current_row_of_the_new_column().total_seconds()),
                    kwargs["execution_datetimes"].get_remaining_current_country_new_column_adding_time()), end='')
        else:
            print(
                "\r    Status: Row {}/{}".format(kwargs["row_index"], kwargs["number_of_rows"]), end='')

    @staticmethod
    def status_of_country_data_setting(**kwargs):
        if kwargs["execution_datetimes"].get_average_time_to_set_page_data():
            print(
                "\r    Setting '{}' dataframe data from page {}/{}, job application {}/{} (Average time per page: {}s; Ends in ± {})...".format(
                    kwargs['country'], kwargs["page"], kwargs["page_count"], kwargs["row_index"], kwargs["number_of_rows"],
                    int(kwargs["execution_datetimes"].get_average_time_to_set_page_data().total_seconds()),
                    kwargs["execution_datetimes"].get_remaining_current_country_data_setting_time()), end='')
        else:
            print(
                "\r    Setting '{}' dataframe data from page {}/{}, job application {}/{}...".format(
                    kwargs['country'], kwargs["page"], kwargs["page_count"], kwargs["row_index"], kwargs["number_of_rows"]),
                end='')
