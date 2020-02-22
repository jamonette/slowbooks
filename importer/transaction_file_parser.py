from .datatypes import JournalEntry, Split, parse_number
import re

# Copies values verbatim from the source file. Requires CSV schema:
# [date, description, s1 account id, s1 name, s1 amount, s1 action... * n]
class PassthroughParser:
    def parse(self, input_rows, config):
        entries = []

        for row_num, row in enumerate(input_rows):
            splits = [Split(account_id=parse_number(row[2], int),
                            account_name=row[3],
                            amount=parse_number(row[4], float),
                            action=row[5]),
                      Split(account_id=parse_number(row[6], int),
                            account_name=row[7],
                            amount=parse_number(row[8], float),
                            action=row[9])]
            entries.append(JournalEntry(date=row[0],
                                        description=row[1],
                                        splits=splits,
                                        source_file_line=row_num,
                                        input_type='manual'))
        return entries

class BasicMatcherParser:
    def parse(self, input_rows, config):
        entries = []

        for row_num, row in enumerate(input_rows):
            date = config['col_map']['date'](row)
            amount = config['col_map']['amount'](row)
            description = config['col_map']['description'](row)

            # splits are spec'd by the matcher as either:
            #  - a tuple of ints, interpreted as a tuple of account ids
            #  - a tuple of strs, interpreted as a tuple of account names
            #  - a list of dicts representing Splits, but with a 'percentage' field
            #    instead of amount, from which the actual amount per split is computed

            matched_rule = next((r for r in config['matcher_rules'] if re.search(r['regex'], row[r['col']])), None)

            splits = []
            input_type = 'auto'
            if matched_rule and (type(matched_rule['splits']) is tuple):
                key = 'account_id' if (type(matched_rule['splits'][0]) is int) else 'account_name'

                s1 = {key: matched_rule['splits'][0],
                      'action': 'debit',
                      'amount': amount}
                s2 = {key: matched_rule['splits'][1],
                      'action': 'credit',
                      'amount': amount}

                splits += [Split(**s1), Split(**s2)]
            elif matched_rule and matched_rule['splits']:
                for split in matched_rule['splits']:
                    split_amount = (split['percentage'] / 100) * amount
                    split_dict = {**{k: v for k, v in split.items() if k != 'percentage'},
                                  'amount': split_amount}
                    splits.append(Split(**split_dict))
            else:
                input_type = 'edit'
                splits = [Split(amount=amount, action='debit'),
                          Split(amount=amount, action='credit')]

            ignore = 'ignore' in matched_rule if matched_rule else False
            if not ignore:
                entries.append(JournalEntry(date=date,
                                            description=description,
                                            splits=splits,
                                            input_type=input_type,
                                            source_file_line=row_num))

        return entries


BUILTIN_PARSERS = {
    'passthrough_parser': PassthroughParser,
    'basic_matcher_parser': BasicMatcherParser,
}

