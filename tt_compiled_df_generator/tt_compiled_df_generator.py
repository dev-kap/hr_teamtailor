from python_scripts.environment_variables.environment_variables import set_environment_variables
from python_scripts.tt_dataframe.tt_compiled_dataframe import TTCompiledDataframe
from python_scripts.utils.execution_datetimes import ExecutionDatetimes
from python_scripts.utils.execution_menu import ExecutionMenu
from python_scripts.utils.execution_prints import ExecutionPrints


set_environment_variables()
execution_prints = ExecutionPrints()
execution_datetimes = ExecutionDatetimes()
execution_datetimes.set_execution_start_datetime(execution_prints=execution_prints, verbose=True)
execution_menu = ExecutionMenu()
for country in execution_menu.get_countries():
    tt_compiled_dataframe = TTCompiledDataframe()
    tt_compiled_dataframe.set(
        country=country, execution_datetimes=execution_datetimes, execution_prints=execution_prints, verbose=2)
execution_datetimes.set_execution_end_datetime(execution_prints=execution_prints, verbose=True)
