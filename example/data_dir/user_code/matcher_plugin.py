# importer attempts to load this class on startup
class MatcherPlugin:

    # importer calls these functions to pick up any user defined matchers

    def get_matchers(self):
        return {}

    def get_matcher_configs(self):
        return {'regex_matcher': MatcherPlugin.regex_matcher_config}

    regex_matcher_config =\
        # the matcher directive in each source file provides
        # an argument that can be used to select different rule sets
        # for different schemas (which vary between banks, over time, etc)
        {'my_bank_v0':

          # provide a mapping from csv row (list[list[str]]) to a few standard
          # fields used by the importer
          {'col_map': {'description': lambda r: r[3],
                       'amount': lambda r: float(r[4]),
                       'date': lambda r: r[0]},           
           'rules': [
                    
                    # each of these rules will return a set of splits for a given
                    # incoming transaction, representing debits/credits against
                    # a CoA account
                    #
                    # splits can be provided as:
                    #  - a tuple of ints, interpreted as a tuple of account ids (left is debit, right is credit)
                    #  - a tuple of strs, interpreted as a tuple of account names
                    #  - a list of dicts representing Splits, but with a percentage field
                    #    instead of amount, from which the actual amount per split is computed
                    {'regex': 'rent payment', 'col': 3, 'splits': ('rent', 'checking account')},
                    {'regex': 'payroll', 'col': 3, 'splits': ('checking account', 'wages')},
                    {'regex': 'credit card autopay', 'col': 3, 'splits': (47, 0)},
                    # more complicated transactions can provide a list of splits, for ex. one debit,
                    # and two credit accounts that split the TX amount 70/30
                    {'regex': 'CONSERVICE', 'col': 3, 'splits': ['your split objs here']},
          ]}}

         # add additional sets of matchers here to handle additional accounts, or
         # changes in TX file format over time
         #'my_credit_card_v0': {
         #  'col_map': {
         #  }
         #}

