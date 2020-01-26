## _slowbooks_

#### _what is this?_

A simple double entry accounting system implemented using `pandas` and not much else. 

#### _why is this?_

Learning exercise, mostly.

#### _components_

- CLI utility for importing transaction data from financial institutions

- Mini-library that provides basic primitives for transforming raw transaction data into journal/ledger entries and basic financial statement data

- A few pre-baked reports: balance sheet, statement of cashflows, general ledger

#### _design principles_

`simplify, simplify`

Tools that make the common case easy tend to make everything else hard. Corporate finance software is overkill, personal finance software is underkill.

Sometimes it's hard to beat a general purpose programming language, and data science tooling turns out to be super effective for financial accounting.

#### _data_

All plain text, all the time. CSV in, CSV out. Filesystem for persistence. `Git` for versioning. Full traceability of output back to source material, with full version history.

You can fit five lifetimes of personal finance data into RAM on a modern toaster. No need to over-complicate this.

Plus, since `git` maintains a hashed history of changes back to repo init, this is basically _blockchain accounting_.

#### _reporting and exploratory analysis_ 

You gotta import some data before you do anything, but here's an example:

```python
import pandas as pd
import slowbooks as sb

data_dir = '/home/billybob/docs/slowbooks-data'
mj = sb.data.master_journal(data_dir)
coa = sb.data.chart_of_accounts(data_dir)

period_range = pd.period_range('20190101', '20200131', freq='M')
sd = sb.data.statement_data(coa, mj, period_range)

# these calls return pandas dataframes, you can use them for whatever

gl = sb.data.general_ledger(mj) 
cf = sb.data.cash_flow(sd)
bs = sb.data.balance_sheet(sd)

# print some plaintext mode reports just for funsies

print(sb.reports.general_ledger(gl))
print(sb.reports.cash_flow(cf))
print(sb.reports.balance_sheet(bs))

# you can do this too

with open('my_bs.html', 'w') as f:
    f.write(bs.to_html())
```

#### _importing transaction data_

Transactions from banks and the like need to be be translated into double-entry form and debit|credit two (or more) corresponding accounts in the book's CoA.

Imports are done with a combination of pluggable _transaction matchers_, plus manual editing of any remaining unmatched source transactions.

SB includes a configurable regex-based matcher to match TX descriptions, but can also load user provided matcher plugins.

###### _workflow_
1. Go get some CSV's. Create a new `git` repo (separate from `SB`) and commit the TX files to `/source`
2. Write a matcher config. See `/example`. Commit that to `git` too.
3. Add a line like `matcher:regex_matcher:my_checking_acct_v0` to the first row of your CSV's. Don't change anything else.
4. `slowbooks import`
5. Review the results in `/pending`. Edit any transactions that weren't auto-matched.
6. Run `slowbooks post` to commit the changes to the master journal.

###### _reimporting files_

From an accounting standpoint, you shouldn't do this. Changes should be made with correcting entries.

From a usability standpoint.. nobody's got time for that when it take 5 runs to get your matcher config dialed in.

1. `slowbooks reimport [files]`

   This reimports `[files]` using your (presumably updated) matcher config. You are now starting from scratch and
   just lost any of your tedious manual edits.
   
2. `slowbooks gen-mergefiles`

   ... but you can restore them from your previous posts to the journal, in the form of a _mergefile_ which is dumped to `/pending`
    
    Review `pending` to make sure everything looks okay, and edit as needed.   
   
3. `slowbooks merge-edits`

    Combine the manual edits from the previous import of `[file]` with the latest auto-matches.

4. `slowbooks post`


##### TODO

<sup>.. some tests would probably be smart?</sup>

- bank-rec capability
- budgeting
- forecasting
- scenario / sensitivity analysis
- maybe some fancier types of output

