import functools
import numpy as np
import pandas as pd

BALANCE_DATA_DIR = 'balance-data'
CHART_OF_ACCOUNTS_PATH = 'master/chart_of_accounts.csv'
MASTER_JOURNAL_PATH = 'master/master_journal.csv'
METADATA_PATH = 'master/metadata.yaml'

DUMMY_TRANSACTION_ID = -1

###############################################################################
#### Raw data #################################################################
###############################################################################

def chart_of_accounts(data_dir):
    raw_coa = pd.read_csv(data_dir / CHART_OF_ACCOUNTS_PATH)

    cat_cols = (raw_coa['category']
                .reset_index(drop=True)
                .str.split(':', expand=True)
                .fillna(axis=1, method='pad')
                .pipe(lambda df: df.rename(lambda col_num: f'category_{col_num}', axis=1)))

    return (raw_coa.join(cat_cols)
                   .drop(columns=['category']))

def master_journal(data_dir, coa):
    mj_file = data_dir / MASTER_JOURNAL_PATH
    mj = (pd.read_csv(mj_file)
          .astype({'date': 'datetime64[ns]'})
          .rename(columns={'id': 'transaction_id'}))

    return enrich_journal(coa, mj)

def balance_data(data_dir, period_range):
    files = [f for f in (data_dir / BALANCE_DATA_DIR).glob('**/*') if f.is_file()]
    return pd.concat([pd.read_csv(f)
                        .astype({'date': 'datetime64[ns]'})
                        .assign(period=lambda df: df['date'].dt.to_period(freq=period_range.freqstr))
                        .groupby(['account_name', 'period'])
                        .apply(lambda df: df.sort_index()['balance'].head(1))
                        .to_frame()
                        .reset_index()
                        .set_index(['account_name', 'period'])
                     for f in files])

###############################################################################
#### Enrich + modify #################################################################
###############################################################################

def enrich_journal(chart_of_accounts, raw_journal, join_key='account_id'):
    def net_amount_predicate(df):
        return (((df['action'] == 'debit') & (df['debit_increases_balance'] == True)) |
                ((df['action'] == 'credit') & (df['debit_increases_balance'] == False)))

    return (raw_journal.merge(right=chart_of_accounts,
                              how='left',
                              left_on=join_key,
                              right_on='id' if join_key == 'account_id' else 'name',
                              validate='many_to_one',
                              suffixes=(False, False))
            .assign(account_name=lambda df: df['name'])
            .assign(account_id=lambda df: df['id'])
            .assign(net_amount=lambda df: np.where(net_amount_predicate(df), df['amount'], df['amount'] * -1))
            .assign(debit_amount=lambda df: np.where((df['action'] == 'debit'), df['amount'], 0))
            .assign(credit_amount=lambda df: np.where((df['action'] == 'credit'), df['amount'], 0))
            .pipe(lambda df: shape_like_journal(df)))

def shape_like_journal(df):

    journal_columns =\
        (['type'] + [col for col in df.columns if 'category_' in str(col)] +
         ['account_id', 'account_name', 'transaction_id', 'date', 'description',
          'net_amount', 'debit_amount', 'credit_amount', 'action'])

    return df.pipe(lambda df: df[journal_columns])

def shape_like_statement(df):

    journal_columns =\
        (['period', 'type'] + [col for col in df.columns if 'category_' in str(col)] +
         ['account_id', 'account_name', 'transaction_id', 'date', 'description',
          'net_amount', 'debit_amount', 'credit_amount', 'action'])

    return df.pipe(lambda df: df[journal_columns])

def statement_index(stmt_data):
    return (['period', 'type'] +
            [c for c in stmt_data.columns if 'category_' in str(c)] +
            ['account_name'])

# return a dataframe that appends the cartesian product of
# [periods x accounts] to the master journal with zero values
#
# this explicitly creates and zero-fills rows for any (account, period)
# pairs that are not present in the journal in order to guarantee
# that all are shown in downstream reports
def statement_data(chart_of_accounts, journal, period_range):

    freq = period_range.freqstr

    pr_as_df = (period_range
                .to_frame(name='period')
                # use cj_key to fake a cross-join
                .assign(cj_key=1))

    filler_data = (chart_of_accounts
                   .assign(cj_key=1)
                   .merge(right=pr_as_df, on='cj_key')
                   .drop('cj_key', 1)
                   .rename(columns={'id': 'account_id', 'name': 'account_name'})
                   .assign(date=lambda df: pd.Series(np.repeat(np.nan, len(df))).astype('datetime64[ns]'))
                   .assign(credit_amount=0.0)
                   .assign(debit_amount=0.0)
                   .assign(net_amount=0.0)
                   .assign(description='')
                   .assign(action=None)
                   .assign(transaction_id=DUMMY_TRANSACTION_ID)
                   .pipe(lambda df: shape_like_statement(df)))

    return (filler_data
            .append(other=journal, sort=False)
            .assign(period=lambda df: np.where(df['period'].isnull(), df['date'].dt.to_period(freq), df['period']))
            .astype({'period': f'period[{freq}]', 'transaction_id': 'Int64'})
            .reset_index(drop=True)
            .fillna({'credit_amount': 0, 'debit_amount': 0, 'net_amount': 0})
            .pipe(lambda df: shape_like_statement(df)))

###############################################################################
#### Report data #################################################################
###############################################################################

# account-wise representation of journal entries
def general_ledger(journal):
    grouped = journal.groupby('account_name')
    by_account = {k: grouped.get_group(k) for k in grouped.groups}

    output = {}
    for acct_name, entries in by_account.items():
        debits = entries.loc[entries['action'] == 'debit', ['date', 'transaction_id', 'description', 'debit_amount']]
        credits = entries.loc[entries['action'] == 'credit', ['date', 'transaction_id', 'description', 'credit_amount']]
        output[acct_name] = {'debit': debits.sort_values('date'),
                             'credit': credits.sort_values('date')}
    return output


# sum of ledger entry net amounts for a given account within a given period
def cash_flow(stmt_data, period_range):
    sd = stmt_data[(stmt_data['period'] >= period_range[0]) &
                   (stmt_data['period'] <= period_range[-1])]

    report_index = statement_index(sd)
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
    return (stmt_data[(stmt_data['period'] <= period_range[-1])]
            .pipe(lambda df: df.set_index(statement_index(df)))
            .sort_values(['period', 'date'])
            .groupby('account_id')
            .apply(lambda df: (df.assign(bs_amount=df['net_amount'])
                               .pipe(lambda df: df[['bs_amount']])
                               .cumsum()
                               .groupby('period')
                               .tail(1)))
            .reset_index()
            .pipe(lambda df: df[(df['period'] >= period_range[0]) &
                                (df['period'] <= period_range[-1])])
            .pipe(lambda df: df.set_index(statement_index(df))))

###############################################################################
#### Calcs #################################################################
###############################################################################

def infer_gains(coa, mj, stmt_data, balance_data_raw, period_range):

    bs = balance_sheet(stmt_data, period_range)
    balance_diff =\
        (balance_data_raw
         .groupby('account_name')
         .apply(lambda df: df.reset_index()
                .set_index('period')
                .reindex(period_range)
                .rename_axis('period'))
         .pipe(lambda df: df.loc[df.index.get_level_values('period') >= period_range[0]])
         .join(bs, how='right')
         .pipe(lambda df: df.replace({col: {0: np.nan} for col in df.columns if col != 'account_id'}))
         .drop(columns=['account_name'])
         .groupby(level='account_name')
         .apply(lambda df: (df.assign(diff_vs_actual=lambda df: df['balance'] - df['bs_amount'])
                              .assign(diff_vs_actual_interp=lambda df: df['diff_vs_actual'].interpolate(method='linear'))
                              .assign(gain=lambda df: df['diff_vs_actual_interp'].diff()))))

    journal_template = (balance_diff
                        .reset_index()
                        .assign(date=lambda df: df['period'].dt.to_timestamp())
                        .assign(transaction_id=DUMMY_TRANSACTION_ID))

    gain_entries = [
            journal_template
                # TODO: does this hold for liability accounts as well?
                .assign(action=lambda df: np.where(df['gain'] >= 0, 'debit', 'credit'))
                .assign(amount=lambda df: df['gain'].abs())
                .assign(description='Unrealized gain calculated from account statement'),
            journal_template
                .assign(description=lambda df: 'Unrealized gain calculated from rec data for: ' + df['account_name'])
                .assign(account_name='unrealized gains')
                # TODO: does this hold for liability accounts as well?
                .assign(action=lambda df: np.where((df['gain'] * -1) >= 0, 'debit', 'credit'))
                .assign(amount=lambda df: df['gain'].abs())
    ]

    entries = pd.concat(gain_entries).reset_index().drop(columns=['type', 'category_0', 'category_1', 'account_id'])
    gain_journal = enrich_journal(coa, entries, join_key='account_name')
    mj_with_gains = pd.concat([mj, gain_journal])

    return mj_with_gains, balance_diff

def _diff_first_balance_vs_bs(df):
    df = df.reset_index().replace({'bs_amount': {0: np.nan},
                                   'balance': {0: np.nan}})

    df['balance'] = df['balance'].interpolate(method='linear')
    i_bal = x if (x := df['balance'].first_valid_index()) else 0
    i_bs = x if (x := df['bs_amount'].first_valid_index()) else 0
    idx = max(i_bal, i_bs)
    diff = df['balance'].iat[idx] - df['bs_amount'].iat[idx]

    return df.assign(bs_diff_vs_bal=diff).dropna().head(1)


