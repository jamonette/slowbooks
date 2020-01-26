from .datatypes import _try_parse, Split
import dateutil.parser
import re

class TransactionMatcher:

    def __init__(self, matcher_plugin):
        self.matchers = {'regex_matcher': RegexMatcher,
                         'manual_entry': ManualEntry}

        self.matcher_configs = {'regex_matcher': {},
                                'manual_entry': {}}

        if matcher_plugin:
            self.matchers.update(matcher_plugin.get_matchers())
            self.matcher_configs.update(matcher_plugin.get_matcher_configs())

    def populate_entries(self, matcher_name, matcher_arg, csv_rows, entries):

        if matcher_name not in self.matchers:
            raise RuntimeError(f'Matcher not found: {matcher_name}')

        if matcher_name not in self.matcher_configs:
            raise RuntimeError(f'Config for matcher not found: {matcher_name}')

        matcher = self.matchers[matcher_name]()
        matcher_config = self.matcher_configs[matcher_name]
        matcher.match(matcher_config, matcher_arg, csv_rows, entries)

# this is a passthrough matcher that copies values verbaitm from the CSV,
# assuming schema [date, description, s1 account id, s1 name, s1 amount, s1 action... * n]
class ManualEntry:
    def match(self, matcher_config, matcher_arg, csv_rows, entries):
        for row, entry in zip(csv_rows, entries):
            entry.date = row[0]
            entry.description = row[1]
            entry.source = 'manually_entered'
            entry.splits = [Split(account_id=_try_parse(row[2], int),
                                  account_name=row[3],
                                  amount=_try_parse(row[4], float),
                                  action=row[5]),
                            Split(account_id=_try_parse(row[6], int),
                                  account_name=row[7],
                                  amount=_try_parse(row[8], float),
                                  action=row[9])]

class RegexMatcher:

    def match(self, matcher_config, matcher_arg, csv_rows, entries):
        col_map = matcher_config[matcher_arg]['col_map']
        rules = matcher_config[matcher_arg]['rules']

        for row, entry in zip(csv_rows, entries):

            amount = col_map['amount'](row)
            entry.date = dateutil.parser.parse(col_map['date'](row))
            entry.description = col_map['description'](row)

            rule = next((t for t in rules
                             if re.search(t['regex'], entry.description)), None)

            # one of our rules matched this transaction - populate
            # the journal entry and splits with the data from the matcher
            if rule:
                splits = self._get_splits_from_rule(rule, amount)

                if ('ignore' in rule) and rule['ignore']:
                    entry.source = 'imported_ignored'
                    entry.description = '[SLOWBOOKS IGNORED] orig: ' + str(row)
                    entry.splits = splits
                    for s in entry.splits:
                        s.amount = 0
                else:
                    entry.source = 'imported_matched'
                    entry.splits = splits

            # none of the rules matched the transaction - create a generic
            # pair of splits for the user to edit manually before posting
            else:
                entry.source = 'imported_edited'
                entry.splits = [Split(amount=amount, action='debit'),
                                Split(amount=amount, action='credit')]

    # splits are specd by the matcher as either:
    #  - a tuple of ints, interpreted as a tuple of account ids
    #  - a tuple of strs, interpreted as a tuple of account names
    #  - a list of dicts representing Splits, but with a 'percentage' field
    #    instead of amount, from which the actual amount per split is computed
    def _get_splits_from_rule(self, rule, tx_amount):
        splits = []
        if rule and (type(rule['splits']) is tuple):
            key = 'account_id' if (type(rule['splits'][0]) is int) else 'account_name'

            s1 = {key: rule['splits'][0],
                  'action': 'debit',
                  'amount': tx_amount}
            s2 = {key: rule['splits'][1],
                  'action': 'credit',
                  'amount': tx_amount}

            splits += [Split(**s1), Split(**s2)]
        else:
            for split in rule['splits']:
                split_amount = (split['percentage'] / 100) * tx_amount
                split_dict = {**{k: v for k, v in split.items() if k != 'percentage'},
                              'amount': split_amount}
                splits.append(Split(**split_dict))

        return splits