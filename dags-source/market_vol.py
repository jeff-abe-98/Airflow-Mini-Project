from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2024, 5, 18, 18, 0),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG('marketvol',
         default_args=default_args,
         description='A dag to get market volatility',
         schedule_interval='0 18 * * 1-5'
         ):
    make_tmp_dir = BashOperator(task_id='make_tmp_dir',
                                bash_command='mkdir -p /opt/airflow/temp_data/{{ ds }}')
    @task()
    def get_market_data(ticker, **kwargs):
        import yfinance as yf
        start_date = kwargs['data_interval_start']
        end_date = kwargs['data_interval_end']
        ds = kwargs['ds']

        df = yf.download(ticker, start=start_date, end=end_date, interval='1m')

        df.to_csv(f'/opt/airflow/temp_data/{ds}/{ticker}_data.csv')

    t1 = get_market_data.override(task_id='get_apple_data')('AAPL')
    t2 = get_market_data.override(task_id='get_tesla_data')('TSLA')

    t3 = BashOperator(task_id='move_apple_data',
                      bash_command='mkdir -p /opt/airflow/data/{{ ds }} && mv /opt/airflow/temp_data/{{ ds }}/AAPL_data.csv /opt/airflow/data/{{ ds }}/AAPL_data.csv' )
    t4 = BashOperator(task_id='move_tesla_data',
                      bash_command='mkdir -p /opt/airflow/data/{{ ds }} && cp /opt/airflow/temp_data/{{ ds }}/AAPL_data.csv /opt/airflow/data/{{ ds }}/TSLA_data.csv' )
    
    @task(task_id='query_market_data')
    def query_market_data( **kwargs):
        import pandas as pd
        import numpy as np
        import math
        import glob
        import os   

        def weighted_std(values, weights):
            """
            Return the weighted average and standard deviation.

            They weights are in effect first normalized so that they 
            sum to 1 (and so they must not all be 0).

            values, weights -- NumPy ndarrays with the same shape.
            """
            average = np.average(values, weights=weights)
            # Fast and numerically precise:
            variance = np.average((values-average)**2, weights=weights)
            return math.sqrt(variance)

        ds = kwargs['ds']

        path = '/opt/airflow/data'
        all_files = glob.glob(os.path.join(path, f'/{ds}/*.csv'))
        print(all_files)

        appl_df = pd.read_csv(f'/opt/airflow/data/{ds}/AAPL_data.csv', index_col=None, header=0)
        tsla_df = pd.read_csv(f'/opt/airflow/data/{ds}/TSLA_data.csv', index_col=None, header=0)

        data = {'Ticker':['AAPL', 'TSLA'],
                'Volatility':[weighted_std(appl_df['Close'], appl_df['Volume']), weighted_std(tsla_df['Close'], tsla_df['Volume'])]}

        final = pd.DataFrame(data)

        final.to_csv(f'/opt/airflow/data/{ds}/volatitly.csv', index=False)

    t5 = query_market_data()

    make_tmp_dir >> [t1, t2] 
    t1 >> t3
    t2 >> t4
    [t3, t4] >> t5
    