import core as sb
import numpy as np
import pandas as pd

#################################################################
### Workbook for generally messing around with the data #########
#################################################################

def run_workbook(data_dir):
    _set_pandas_display_options()

    basic_reports(data_dir)
    bank_rec_example(data_dir)

def bank_rec_example(data_dir):

    start_date = pd.Timestamp('20171101')
    end_date = pd.Timestamp('20201231')
    period_range = pd.period_range(start=start_date, end=end_date, freq='M')

    balance_data_raw = sb.data.balance_data(data_dir, period_range)
    coa = sb.data.chart_of_accounts(data_dir)
    mj = sb.data.master_journal(data_dir, coa)
    sd = sb.data.statement_data(coa, mj, period_range)
    bs = sb.data.balance_sheet(sd, period_range)

    report = (bs.reset_index()
              .pipe(lambda df: df[df['type'] == 'asset'])
              .set_index(['account_name', 'period'])
              .join(balance_data_raw[['balance']], how='left')
              .assign(DIFF=lambda df: df['balance'] - df['bs_amount'])
              .reset_index()
              .set_index(['account_name', 'period'])
              .sort_index()
              .pipe(lambda df: df[['balance', 'bs_amount', 'DIFF']])
              .rename_axis('values', axis=1)
              .stack('values')
              .unstack('account_name')
              .unstack('values'))

    print(report)

def basic_reports(data_dir):
    start_date = pd.Timestamp('20170101')
    end_date = pd.Timestamp('20201231')
    period_range = pd.period_range(start=start_date, end=end_date, freq='M')

    coa = sb.data.chart_of_accounts(data_dir)
    mj = sb.data.master_journal(data_dir, coa)
    sd = sb.data.statement_data(coa, mj, period_range)
    bd = sb.data.balance_data(data_dir, period_range)

    mj_with_gains, balance_diff = sb.data.infer_gains(coa, mj, sd, bd, period_range)
    sd_with_gains = sb.data.statement_data(coa, mj_with_gains, period_range)

    gl = sb.data.general_ledger(mj_with_gains)
    cf = sb.data.cash_flow(sd_with_gains, period_range)

    bs = sb.data.balance_sheet(sd_with_gains, period_range)

    gains = (bs.loc[bs.index.get_level_values('type') == 'asset', :]
             .reset_index()
             .set_index(['period', 'account_name'])
             .rename_axis('values', axis=1)
             .stack('values')
             .unstack('account_name')
             .unstack('values'))

    glr = sb.reports.general_ledger(gl)
    cfr = sb.reports.cash_flow(cf)
    bsr = sb.reports.balance_sheet(bs)

    print(f'''
        {cfr}

        {bsr}

        {gains}
     ''')

def _set_pandas_display_options():
    #pd.options.display.multi_sparse = False
    pd.options.display.expand_frame_repr = False
    pd.options.display.float_format = '${:,.2f}'.format
    pd.options.display.max_columns = 10000
    pd.options.display.max_colwidth = 50
    pd.options.display.max_info_columns = 10000
    pd.options.display.max_info_rows = 10000
    pd.options.display.max_rows = 10000
    pd.options.display.max_seq_items = 10000
