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

def fetch_chart_of_accounts(data_dir):
    raw_coa = pd.read_csv(data_dir / CHART_OF_ACCOUNTS_PATH)
    cat_cols = (raw_coa['category']
                .reset_index(drop=True)
                .str.split(':', expand=True)
                .fillna(axis=1, method='pad')
                .rename(lambda col_num: f'category_{col_num}', axis=1))

    return (raw_coa
            .join(cat_cols)
            .drop(columns=['category'])
            .rename(columns={'id': 'account_id', 'name': 'account_name'}))

def fetch_master_journal(data_dir, chart_of_accounts):
    mj_file = data_dir / MASTER_JOURNAL_PATH
    mj = (pd.read_csv(mj_file)
          .astype({'date': 'datetime64[ns]'})
          .rename(columns={'id': 'transaction_id'}))

    return _build_journal(chart_of_accounts, mj)

def fetch_balance_data(data_dir):
    files = [f for f in (data_dir / BALANCE_DATA_DIR).glob('**/*') if f.is_file()]
    return pd.concat([pd.read_csv(f)
                        .astype({'date': 'datetime64[ns]'})
                     for f in files])

###############################################################################
#### Core DataFrame shapes ####################################################
###############################################################################

# given a journal, add filler data such that there is at least one
# (blank) line item for each (account, period) combination in all
# downstream reports

def statement_data(chart_of_accounts,
                   journal,
                   period_range,
                   with_gains=False,
                   balance_data=None):

    statement =\
        (chart_of_accounts
         # generate all combinations of (account, period) by faking
         # a cartesian join using a special key
         .assign(crossjoin_key=1)
         .merge(right=(period_range
                       .to_frame(name='period')
                       .assign(crossjoin_key=1)),
                on='crossjoin_key')
         .drop('crossjoin_key', 1)
         .rename(columns={'id': 'account_id', 'name': 'account_name'})
         .assign(date=lambda df: df['period'].dt.start_time)
         .assign(credit_amount=0.0)
         .assign(debit_amount=0.0)
         .assign(net_amount=0.0)
         .assign(description='')
         .assign(action=None)
         .assign(transaction_id=DUMMY_TRANSACTION_ID)
         .pipe(lambda df: df[_get_statement_columns(df)])

         # combine with the actual journal data
         .append(other=_journal_to_statement(journal, period_range), sort=False)
         .pipe(lambda df: _journal_to_statement(df, period_range)))

    generated_entries = [
        _generate_inferred_gains(chart_of_accounts, statement, balance_data, period_range) if with_gains else pd.DataFrame(),
        _generate_closing_entries(chart_of_accounts, statement, period_range),
    ]

    return (pd
            .concat(generated_entries)
            .pipe(lambda df: _journal_to_statement(df, period_range))
            .append(statement))

def _build_journal(chart_of_accounts,
                   journal_like,
                   join_key='account_id'):

    return (journal_like
            .merge(right=chart_of_accounts,
                   how='left',
                   left_on=join_key,
                   right_on='account_id' if join_key == 'account_id' else 'account_name',
                   validate='many_to_one',
                   suffixes=('', '_coa'))
            .assign(net_amount=lambda df: np.where(_net_amount_predicate(df), df['amount'], df['amount'] * -1))
            .assign(debit_amount=lambda df: np.where((df['action'] == 'debit'), df['amount'], 0))
            .assign(credit_amount=lambda df: np.where((df['action'] == 'credit'), df['amount'], 0))
            .pipe(lambda df: df[_get_journal_columns(df)]))

def _net_amount_predicate(df):
    # comparison to `True` is due to Pandas weirdness
    return (((df['action'] == 'debit') & (df['debit_increases_balance'] == True)) |
            ((df['action'] == 'credit') & (df['debit_increases_balance'] == False)))

def _journal_to_statement(journal_like, period_range):
    return (journal_like
            .assign(period=lambda df: df['date'].dt.to_period(period_range.freqstr))
            .reset_index()
            .pipe(lambda df: df[_get_statement_columns(df)]))

def _get_journal_columns(journal_like):
    return (['type'] + [col for col in journal_like.columns if 'category_' in str(col)] +
            ['account_id', 'account_name', 'transaction_id', 'date', 'description',
             'net_amount', 'debit_amount', 'credit_amount', 'action'])

def _get_statement_columns(statement_like):
    return (['period', 'type'] + [col for col in statement_like.columns if 'category_' in str(col)] +
            ['account_id', 'account_name', 'transaction_id', 'date', 'description',
             'net_amount', 'debit_amount', 'credit_amount', 'action'])

###############################################################################
#### Report data ##############################################################
###############################################################################

def general_ledger(stmt_data):
    grouped = stmt_data.groupby('account_name')
    by_account = {k: grouped.get_group(k) for k in grouped.groups}

    output = {}
    for acct_name, entries in by_account.items():
        debits = entries.loc[entries['action'] == 'debit', ['date', 'transaction_id', 'description', 'debit_amount']]
        credits = entries.loc[entries['action'] == 'credit', ['date', 'transaction_id', 'description', 'credit_amount']]
        output[acct_name] = {'debit': debits.sort_values('date'),
                             'credit': credits.sort_values('date')}
    return output


def cash_flow(stmt_data, period_range):
    sd = stmt_data[(stmt_data['period'] >= period_range[0]) &
                   (stmt_data['period'] <= period_range[-1])]

    report_index = _get_report_index(sd)
    return (sd[report_index + ['net_amount']]
            .set_index(report_index, drop=True)
            .groupby(report_index)
            .sum())

def balance_sheet(stmt_data, period_range):

    # - group journal entries by account
    # - for each account:
    #   - sort journal entries by date
    #   - compute cumulative sum
    #   - group by period
    #   - select the value of the cumulative sum for the last date in the period

    return (stmt_data[(stmt_data['period'] <= period_range[-1])]
            .pipe(lambda df: df.set_index(_get_report_index(df)))
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
            .pipe(lambda df: df.set_index(_get_report_index(df))))

def _get_report_index(stmt_data):
    return (['period', 'type'] +
            [col for col in stmt_data.columns if 'category_' in str(col)] +
            ['account_name'])

###############################################################################
#### Generated entries ########################################################
###############################################################################

def _generate_inferred_gains(chart_of_accounts,
                             stmt_data,
                             raw_balance_data,
                             report_range):
    balance_data =\
        (raw_balance_data
         .assign(period=lambda df: df['date'].dt.to_period(freq=report_range.freqstr))
         .groupby(['account_name', 'period'])
         .apply(lambda df: df.sort_values('date')[['date', 'balance']].head(1))
         .droplevel(2))

    # generate gains entries starting at the earliest available balance date -
    # ignore the incoming `report_range` and use it only as a sotpping point
    period_range = pd.period_range(balance_data['date'].sort_values()[0],
                                   report_range[-1].end_time,
                                   freq=report_range.freqstr)

    journal_template =\
        (balance_data
         .groupby('account_name')
         .apply(lambda df: df.reset_index()
                             .set_index('period')
                             .reindex(period_range)
                             .rename_axis('period'))
         .join(balance_sheet(stmt_data, period_range),
               how='right')
         .drop(columns=['account_name'])
         .groupby(level='account_name')
         .apply(lambda df: (df.assign(diff_vs_actual=lambda df: df['balance'] - df['bs_amount'])
                              .assign(diff_vs_actual_interp=lambda df: df['diff_vs_actual'].interpolate(method='linear'))
                              .assign(gain=lambda df: df['diff_vs_actual_interp'].diff())))
         .reset_index()
         .assign(date=lambda df: df['period'].dt.to_timestamp())
         .assign(transaction_id=DUMMY_TRANSACTION_ID))

    # TODO: verify signed-ness of gain/loss entries for liability accounts. ... maybe write some tests? :)
    gain_entries = [
        journal_template
            .assign(action=lambda df: np.where(df['gain'] >= 0, 'debit', 'credit'))
            .assign(amount=lambda df: df['gain'].abs())
            .assign(description='Unrealized gain calculated from account statement'),
        journal_template
            .assign(description=lambda df: 'Unrealized gain calculated from rec data for: ' + df['account_name'])
            .assign(account_name='unrealized gains')
            .assign(action=lambda df: np.where((df['gain'] * -1) >= 0, 'debit', 'credit'))
            .assign(amount=lambda df: df['gain'].abs())
    ]

    return (pd.concat(gain_entries)
            .dropna(subset=['amount'])
            .reset_index()
            .drop(columns=['type', 'category_0', 'category_1', 'account_id'])
            .pipe(lambda df: _build_journal(chart_of_accounts, df, join_key='account_name'))
            .pipe(lambda df: df[_get_journal_columns(df)]))

def _generate_closing_entries(chart_of_accounts, stmt_data, period_range):
    return (chart_of_accounts
            .pipe(lambda df: df.loc[df['closing_account'].notna(), ['account_name', 'closing_account']])
            .assign(crossjoin_key=1)
            .merge(right=period_range.to_frame(name='period').assign(crossjoin_key=1),
                   on='crossjoin_key')
            .drop('crossjoin_key', 1)
            .set_index(['period', 'account_name'])
            .merge(right=(stmt_data.set_index(['period', 'account_name'])
                          .sort_index()
                          .rename_axis(['period', 'account_name'])
                          .dropna(subset=['net_amount'])),
                   how='inner',
                   on=['period', 'account_name'])
            .pipe(lambda df: df[df['net_amount'] != 0])
            .reset_index()
            .pipe(lambda df: df[['period', 'closing_account', 'date', 'description', 'net_amount', 'action']])
            .rename(columns={'closing_account': 'account_name', 'net_amount': 'amount'})
            .assign(action=lambda df: np.where(df['action'] == 'debit', 'credit', 'debit'))
            .assign(amount=lambda df: df['amount'] * -1)
            .assign(transaction_id=DUMMY_TRANSACTION_ID)
            .pipe(lambda df: _build_journal(chart_of_accounts, df, join_key='account_name')))

