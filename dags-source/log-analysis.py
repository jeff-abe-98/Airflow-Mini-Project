from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from datetime import datetime, timedelta
from pathlib import Path


default_args = {
    'start_date': datetime(2024, 5, 18, 18, 0),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG('log_analysis',
         default_args=default_args,
         description='A dag to get market volatility',
         schedule_interval='0 18 * * 1-5'
         ):

        
    def analyze_file(file_name):
        """
        Gets the error messages in the log file at file_name, and returns the count and a list of messages
        """
        error_ct = 0
        error_msg = []
        with open(file_name) as log_file:    # Opening log file
                for line in log_file.readlines():    # Iterating through the lines
                    if line[0] != '[':
                        # Move to next line if its not a logging formated line
                        continue
                    split_line = line.split(' - ')
                    if split_line[0][-5] == 'ERROR':
                        # Adding to the count and list if ERROR
                        error_ct += 1
                        error_msg.append(line)
        return error_ct, error_msg
    
    @task()
    def analyze_log(task_id):
        """
        Task that returns the number of error messages and the content of those error messages for the specified task in the market_vol dag
        """
        log_base_folder = '~/airflow/logs/dagid=marketvol'

        # Gets all logs in path
        file_list = Path(log_base_folder).rglob('*.log')

        # Grabs only the log paths for the specified task_id
        task_logs = [file for file in file_list if task_id in file]

        # Gets total count and message list
        error_ct = 0
        error_list = []
        for log in task_logs:
            ct, msg = analyze_file(log)
            error_ct += ct
            error_list.append(msg)

        print(f'Log error summary for task: {task_id}')
        print(f'Total number of errors: {error_ct}')
        print('Here are all the errors:')
        print(*error_list, end='\n')

    t1 = analyze_log.override(task_id='tsla_log_analyzer')('get_tsla_data')
    t2 = analyze_log.override(task_id='appl_log_analyzer')('get_appl_data')
