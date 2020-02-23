from itertools import zip_longest
import pandas as pd

###########################################################################
#### Plain text / CLI friendly reports ####################################
###########################################################################

def general_ledger(journal_by_account):
    output = []
    for acct_name, ledgers in journal_by_account.items():
        header = f'{acct_name}\n{"-" * len(acct_name)}'
        ledgers = as_columns(ledgers['debit'], ledgers['credit'])
        output.append(''.join([header, ledgers, '\n\n']))

    return ''.join(output)

def balance_sheet(bs_data):

    assets = bs_data[bs_data.index.get_level_values('type') == 'asset'].droplevel('type')
    liability = bs_data[bs_data.index.get_level_values('type') == 'liability'].droplevel('type')
    equity = bs_data[bs_data.index.get_level_values('type') == 'equity'].droplevel('type')

    return f"""\
        Assets
        {pivot(assets, 'period', ['category_0'], row_totals=False)}

        Liability
        {pivot(liability, 'period', ['category_0'], row_totals=False)}

        Equity
        {pivot(equity, 'period', ['category_0'], row_totals=False)}"""


def cash_flow(cf_data):

    income = cf_data[cf_data.index.get_level_values('type') == 'income'].droplevel('type')
    expense = cf_data[cf_data.index.get_level_values('type') == 'expense'].droplevel('type')
    liability = cf_data[cf_data.index.get_level_values('type') == 'liability'].droplevel('type')
    assets = cf_data[cf_data.index.get_level_values('type') == 'assets'].droplevel('type')

    net_income = (income.groupby('period').sum() - expense.groupby('period').sum()).T
    free_cf_to_equity = net_income - (-1 * liability.groupby('period').sum().T)

    # sanity check: (FCF to equity - cash flow into assets) should equal zero
    err = (free_cf_to_equity - assets.groupby('period').sum().T)

    # TODO Net Income / FCFTE calc is erroneous

    return f"""\
        Income
        {pivot(income, 'period', ['category_0'])}

        Expense
        {pivot(expense, 'period',['category_0'])}

        Net Income
        {net_income}

        Debt service
        {pivot(liability, 'period', ['category_0'])}

        Free cash flow to equity\n
        {free_cf_to_equity}

        Error:\n
        {err}"""

###########################################################################
#### Dataframe utils ######################################################
###########################################################################

# native pandas pivot can't do subtotals for arbitrary levels
# within a multilevel index - do that here
def pivot(df, column, subtotal_lvls, row_totals=True, col_totals=True):
    return (df
            .unstack(column)
            .pipe(with_index_subtotals, subtotal_lvls)
            .pipe(lambda df: with_total_row(df) if col_totals else df)
            .pipe(lambda df: df.assign(TOTAL=lambda df: df.sum(axis=1)) if row_totals else df))

# given a dataframe with a MultiIndex, add subtotal rows for the index
# levels in 'idx_levels_to_subtotal'
def with_index_subtotals(df, idx_levels_to_subtotal, st_name='SUBTOTAL'):

    subtotal_dfs = []
    for curr_level in idx_levels_to_subtotal:
        i = df.index.names.index(curr_level)
        prev_levels = df.index.names[:i]
        next_levels = df.index.names[(i + 1):]

        subtotal_df = df.groupby(prev_levels + [curr_level]).sum()
        index_as_df = subtotal_df.index.to_frame()

        for i, lvl in enumerate(next_levels):
            if i == 0:
                prev_level_labels = (index_as_df.index
                                       .get_level_values(curr_level)
                                       .to_series()
                                       .astype(str))

                index_as_df[lvl] = (prev_level_labels  + f' {st_name}').to_list()
            else:
                index_as_df[lvl] = [''] * len(index_as_df)

        subtotal_df_idx = pd.MultiIndex.from_frame(index_as_df)
        subtotal_dfs.append(subtotal_df.set_index(subtotal_df_idx))

    return pd.concat([df] + subtotal_dfs).sort_index()

def with_total_row(df):
    for name in df.index.names:
        df = df.query(f'not {name}.str.contains("TOTAL")')

    idx = ([['']] * (len(df.index.names) - 1)) + [['TOTAL']]
    return df.append((df.sum().to_frame().T
                      .set_index(pd.MultiIndex.from_arrays(idx))))

###########################################################################
#### Plaintext / CLI Formatters ###########################################
###########################################################################

def as_columns(*inputs, spacing='  |  '):
    # allow caller to pass in raw pandas objects without calling their
    # special `to_string` method
    text_blocks = [i.to_string() if getattr(i, 'to_string', None) else i for i in inputs]

    text_blocks_as_line_lists = [tb.split('\n') for tb in text_blocks]
    max_widths = [max([len(line) for line in line_list]) for line_list in text_blocks_as_line_lists]

    out_lines = []
    for blocks in zip_longest(*text_blocks_as_line_lists, fillvalue=''):
        output_line = spacing.join([line.ljust(padding) for line, padding in zip(blocks, max_widths)])
        out_lines.append(output_line)

    output = '\n'.join(out_lines)
    return output
