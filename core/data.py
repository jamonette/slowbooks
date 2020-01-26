import core.io as io
import numpy as np
import pandas as pd

def master_journal(data_dir):

    def net_amount_predicate(df):
        return (((df['action'] == 'debit') & (df['debit_increases_balance'] == True)) |
                ((df['action'] == 'credit') & (df['debit_increases_balance'] == False)))

    return (io._raw_master_journal(data_dir)
            .astype({'date': 'datetime64[ns]'})
            .merge(right=io._raw_chart_of_accounts(data_dir),
                   how='left',
                   left_on='account_id',
                   right_on='id',
                   validate='many_to_one')
           .assign(account_name=lambda df: df['name'])
           .assign(net_amount=lambda df: np.where(net_amount_predicate(df), df['amount'], df['amount'] * -1))
           .assign(debit_amount=lambda df: np.where((df['action'] == 'debit'), df['amount'], 0))
           .assign(credit_amount=lambda df: np.where((df['action'] == 'credit'), df['amount'], 0))
           .pipe(lambda df: df[['type', 'category', 'account_id', 'account_name',
                                'date', 'transaction_id', 'description', 'net_amount',
                                'debit_amount', 'credit_amount', 'action']]))

# account-wise representation of journal entries
def general_ledger(master_journal):
    grouped = master_journal.groupby('account_name')
    by_account = {k: grouped.get_group(k) for k in grouped.groups}

    output = {}
    for acct_name, entries in by_account.items():
        debits = entries.loc[entries['action'] == 'debit', ['date', 'transaction_id', 'description', 'debit_amount']]
        credits = entries.loc[entries['action'] == 'credit', ['date', 'transaction_id', 'description', 'credit_amount']]
        output[acct_name] = {'debit': debits,
                             'credit': credits}

    return output

# return a dataframe that appends the cartesian product of
# [periods x accounts] to the master journal with zero values
#
# this explicitly creates and zero-fills rows for any (account, period)
# pairs that are not present in the journal in order to guarantee
# that all are shown in downstream reports
def statement_data(chart_of_accounts, master_journal, period_range):

    freq = period_range.freqstr

    pr_as_df = (period_range
                 .to_frame(name='period')
                 .assign(cj_key=1)) # use key to fake a cross-join

    filler_data = (chart_of_accounts[['type', 'category', 'id', 'name']]
                    .assign(cj_key=1)
                    .merge(right=pr_as_df, on='cj_key')
                    .drop('cj_key', 1)
                    .rename(columns={'id': 'account_id', 'name': 'account_name'})
                    .assign(date=lambda df: pd.Series(np.repeat(np.nan, len(df))).astype('datetime64[ns]'))
                    .assign(credit_amount=0.0)
                    .assign(debit_amount=0.0)
                    .assign(net_amount=0.0)
                    .pipe(lambda df: df[['period', 'type', 'category', 'account_id', 'account_name',
                                         'net_amount', 'debit_amount', 'credit_amount']]))

    sd = (filler_data.append(other=master_journal, sort=False)
           .assign(period=lambda df: np.where(df['period'].isnull(), df['date'].dt.to_period(freq), df['period']))
           .astype({'period': f'period[{freq}]', 'transaction_id': 'Int64'})
           .reset_index(drop=True)
           .fillna({'credit_amount': 0, 'debit_amount': 0, 'net_amount': 0})
           .pipe(lambda df: df[['period', 'type', 'category', 'account_id', 'account_name', 'date',
                                'transaction_id', 'net_amount', 'debit_amount', 'credit_amount']]))

    # account category is ragged hierarchy spec'd as a ':' delimited text column -
    # split that here and expand it into the corresponding number of columns
    cat_cols = (sd['category']
                 .reset_index(drop=True)
                 .str.split(':', expand=True)
                 .fillna(axis=1, method='pad')
                 .pipe(lambda df: df.rename(lambda col_name: f'category_{col_name}', axis=1)))

    return (sd.join(cat_cols)
              .rename(columns={'category': 'all_categories'}))

# sum of ledger entry net amounts for a given account within a given period
#
# REFACTOR have to pass (the same) period range to both `statement_data` and
# downstream functions - bundle PR with SD output in a dataclass?
def cash_flow(stmt_data, period_range):
    start = period_range.to_list()[0].to_timestamp()
    end = period_range.to_list()[-1].to_timestamp()

    sd = (stmt_data
          # note: allow NaN dates in order to retain (period, account) filler data
          .pipe(lambda df: df[((df['date'] > start) & (df['date'] <= end)) |
                               (df['date'].isna())]))

    category_levels = [c for c in sd.columns if 'category_' in c]
    report_index = ['period', 'type'] + category_levels + ['account_name']

    return (sd[report_index + ['net_amount']]
              .set_index(report_index, drop=True)
              .groupby(report_index)
              .sum())

# cumulative sum of ledger entry net amounts for a given account, from
# the beginning of time until the end of a given period
#
# to calculate:
# - group all journal entries by account
# - for each account:
#   - sort journal entries by date
#   - compute cumulative sum
#   - group by period
#   - select the value of the cumulative sum for the last date in the period
def balance_sheet(stmt_data, period_range):

    end = period_range.to_list()[-1].to_timestamp()
    sd = (stmt_data
          # note: since we want cumulative sum, filter on end date only
          # note: allow NaN dates in order to retain (period, account) filler data
          .pipe(lambda df: df[(df['date'] <= end) | df['date'].isna()]))

    category_levels = [c for c in sd.columns if 'category_' in c]
    report_index = ['period', 'type'] + category_levels + ['account_name']

    return (sd
              .set_index(report_index)
              .sort_values(['period', 'date'])
              .groupby('account_id')
              .apply(lambda df: (df.assign(bs_amount=df['net_amount'])
                                   .pipe(lambda df: df[['bs_amount']])
                                   .cumsum()
                                   .groupby('period')
                                   .tail(1)))
              .reset_index()
              .set_index(report_index)
              .pipe(lambda df: df[['bs_amount']]))

def chart_of_accounts(data_dir):
    return io._raw_chart_of_accounts(data_dir)