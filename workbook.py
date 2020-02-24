import core as sb
import numpy as np
import pandas as pd

#################################################################
### Workbook for generally messing around with the data #########
#################################################################

def run_workbook(data_dir):
    _set_pandas_display_options()

    start_date = pd.Timestamp('20171231')
    end_date = pd.Timestamp('20191231')
    period_range = pd.period_range(start=start_date, end=end_date, freq='M')

    coa = sb.data.fetch_chart_of_accounts(data_dir)
    mj = sb.data.fetch_master_journal(data_dir, coa)
    bd = sb.data.fetch_balance_data(data_dir)
    sd = sb.data.statement_data(coa, mj, period_range, with_gains=True, balance_data=bd)

    gl = sb.data.general_ledger(sd)
    cf = sb.data.cash_flow(sd, period_range)
    bs = sb.data.balance_sheet(sd, period_range)


    glr = sb.reports.general_ledger(gl)
    isr = sb.reports.income_statement(cf)
    cfr = sb.reports.cashflow_statement(cf)
    bsr = sb.reports.balance_sheet(bs)


    print(f'''
    Income statement
    ================
    {isr}

    Cashflow statement
    ==================

    {cfr}

    Balance sheet
    =============

    {bsr}
     ''')

def _set_pandas_display_options():
    #pd.options.display.multi_sparse = False
    pd.options.display.expand_frame_repr = False
    pd.options.display.float_format = '${:,.2f}'.format
    pd.options.display.max_columns = 10000
    pd.options.display.max_colwidth = 50
    pd.options.display.max_info_columns = 10000
    pd.options.display.max_info_rows = 10000
    pd.options.display.max_rows = 10000000
    pd.options.display.max_seq_items = 10000

