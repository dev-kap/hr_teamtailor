from os import listdir


def get_csv_file_name(**kwargs):
    input_csv_file_names = [
        input_csv_file_name for input_csv_file_name in sorted(listdir(kwargs["csv_file_folder_path"]))
        if f"job_application_dataframe_{kwargs['country'].lower().replace(' ', '_')}_20" in input_csv_file_name]
    if not input_csv_file_names:
        raise Exception("There is no CSV file for {}".format(kwargs["country"]))
    return input_csv_file_names[0]
