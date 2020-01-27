import pandas as pd
import core as sb

def run_workbook(data_dir):
    _set_pandas_display_options()

    mj = sb.data.master_journal(data_dir)
    coa = sb.data.chart_of_accounts(data_dir)

    start = pd.Timestamp('20190101')
    end = pd.Timestamp('20200131')
    period_range = pd.period_range(start=start, end=end, freq='M')

    #################################################################
    ### Workbook for generally messing around with the data #########
    #################################################################

    gl = sb.data.general_ledger(mj)
    sd = sb.data.statement_data(coa, mj, period_range)
    #cf = sb.data.cash_flow(sd, period_range)
    #bs = sb.data.balance_sheet(sd, period_range)

    #glr = sb.reports.general_ledger(gl)
    #cfr = sb.reports.cash_flow(cf)
    #bsr = sb.reports.balance_sheet(bs)

    # TODO add ability to assign targets to account
    # categories in addition to individual accounts

    example_budget_items = [
        ('groceries', 'M', 250.0),
        ('restaurant', 'D', 10),
        ('rent', 'M', 2500),
        ('misc goods', 'M', 100),
        ('misc subscriptions', 'M', 20),
    ]

    # budget is expressed as (interval, [(account, frequency, amount)]
    #
    # this allows for budgets that change over time
    example_budget = [(pd.Interval(start, end), example_budget_items)]

    diff = sb.budget.budget_vs_actuals(example_budget, coa, mj, period_range)
    pt = diff.pivot_table(index=['account_name'], columns='period')

    print(pt)


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