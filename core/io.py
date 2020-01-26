import pandas as pd

CHART_OF_ACCOUNTS_PATH = 'master/chart_of_accounts.csv'
MASTER_JOURNAL_PATH = 'master/master_journal.csv'
METADATA_PATH = 'master/metadata.yaml'

def _raw_master_journal(data_dir):
    mj_file = data_dir / MASTER_JOURNAL_PATH
    return  (pd.read_csv(mj_file).astype({'date': 'datetime64[ns]'})
                                 .assign(transaction_id=lambda df: df['id'])
                                 .drop(columns=['id']))

def _raw_chart_of_accounts(data_dir):
    return pd.read_csv(data_dir / CHART_OF_ACCOUNTS_PATH)