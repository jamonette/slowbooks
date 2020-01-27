from functools import reduce
import core as sb
import pandas as pd

# no native way to compare frequencies in pandas?
FREQ_COMP = {'D': 2, 'M': 1, 'Y': 0}

# a budget is a target value for a given cost within
# a given period of time
#
# we can represent this easily as the amount and frequency of a
# certain cost (described by 'account') within a certain interval
#
#   {(interval, {(account, freq, amount)}}
#
# from this starting point we can generate a dataframe shaped
# like a cash flow statement, then overlay that with actuals
# from any period to determine difference vs. target
#
# since the budget spec is represented in code, its easy to
# do things like:
#   - apply a constant inflation factor over time
#   - build a function that can yield concrete target amounts given
#     a base budget an some another arbitrary time series input
#   - test an arbitrary number of scenarios

# REFACTOR improve the way data is passed through functions in the API
#
# trying to stick to pure functions in the library but also avoid
# having to pass around dumb 'data container' objects
#
# should be possible with some thought and planning, but the API
# surface is getting kinda messy as it stands

def budget_vs_actuals(budgets, chart_of_accounts, master_journal, report_period_range):

    # create a period range for each budget item that starts/stops
    # on the endpoints of the budget's associated interval

    items_dfs_by_acct = {}
    for interval, budget_spec in budgets:
        for (account, item_freq, amount) in budget_spec:

            # - create a period range over the item interval at item frequency
            # - resample to match the reporting period range
            # - sum values to downsample, pad then divide to upsample

            report_freq = report_period_range.freqstr
            budget_range = pd.period_range(interval.left, interval.right, freq=item_freq)
            item_df = (budget_range.to_frame()
                             .assign(budget_amount=amount)
                             .assign(account_name=account))

            aligned = None

            # resample .sum() can't handle strings, so we have to repopulate 'account_name'
            if FREQ_COMP[item_freq] > FREQ_COMP[report_freq]:
                aligned = item_df.resample(report_freq, closed='left').sum()
                aligned['account_name'] = item_df['account_name'].values[:len(aligned)]

            # upsample overshoots the interval, have to truncate
            elif FREQ_COMP[item_freq] < FREQ_COMP[report_freq]:
                periods = len(report_period_range)
                resampled = item_df.resample(report_freq, closed='left').ffill()
                resampled['budget_amount'] = resampled['budget_amount'] / periods
                aligned = resampled[:periods]

            else:
                aligned = item_df

            items_dfs_by_acct[account] = items_dfs_by_acct.get(account, []) + [aligned]

    output = []
    for acct, item_dfs in items_dfs_by_acct.items():
        summed = reduce(lambda x, y: x.add(y), item_dfs).reset_index()
        output.append(summed)

    # join with COA data to populate type and categories
    budget_df = (pd
                 .concat(output, ignore_index=True, sort=True)
                 .merge(right=chart_of_accounts,
                        left_on='account_name',
                        right_on='name',
                        how='left',
                        sort=True)
                 .pipe(lambda df: sb.data.expand_category_columns(df))
                 .rename(columns={'index': 'period'})
                 .pipe(lambda df: df.set_index(sb.data.statement_index(df)))
                 .pipe(lambda df: df[['budget_amount']]))

    accounts = items_dfs_by_acct.keys()
    sd = sb.data.statement_data(chart_of_accounts, master_journal, report_period_range)
    cf = (sb.data.cash_flow(sd, report_period_range)
          .assign(account_name=lambda df: df.index.get_level_values('account_name'))
          .pipe(lambda df: df[df['account_name'].isin(accounts)])
          .drop(columns=['account_name']))

    budget_remaining = budget_df['budget_amount'] - cf['net_amount']
    return budget_remaining.to_frame()

