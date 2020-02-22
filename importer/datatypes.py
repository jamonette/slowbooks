from dataclasses import dataclass
from datetime import datetime
import dataclasses
import dateutil.parser
import re

@dataclass
class JournalEntry:
    id: int = None
    date: datetime = None
    description: str = None
    splits: list = None
    source_file: str = None
    source_file_line: int = None
    input_type: str = None

    @staticmethod
    def to_csv(entries):
        header_row =[[
            'id', 'source_file', 'source_file_line', 'input_type', 'date', 'description',
            'split_0_account_id', 'split_0_account_name', 'split_0_account_action', 'split_0_amount',
            'split_1_account_id', 'split_1_account_name', 'split_1_account_action', 'split_1_amount',
            # additional splits will serialize successfully, but
            # the header will not reflect the additional columns
        ]]

        entry_rows = []
        for e in entries:
            row = [e.id, e.source_file, e.source_file_line, e.input_type, e.date, e.description]
            for s in e.splits:
                row += s.to_csv_row()
            entry_rows.append(row)

        return header_row + entry_rows

    @staticmethod
    def from_csv(csv_rows):
        num_header_rows = 1
        return [JournalEntry._from_csv_row(r) for r in csv_rows[num_header_rows:]]

    @staticmethod
    def _from_csv_row(row):
        num_je_cols = 6
        num_split_cols = 4
        num_splits = int(len(row[num_je_cols:]) / num_split_cols)

        # after the JE columns, determine number of splits by dividing the remaining
        # number of columns by the number of cols used to represent a split
        split_vals = [row[num_je_cols:][(i * num_split_cols):((i + 1) * num_split_cols)] for i in range(num_splits)]
        splits = [Split(account_id=parse_number(vals[0], int),
                        account_name=vals[1],
                        action=vals[2],
                        amount=parse_number(vals[3], float)) for vals in split_vals]

        return JournalEntry(id=int(row[0]),
                            source_file=row[1],
                            source_file_line=row[2],
                            input_type=row[3],
                            date=dateutil.parser.parse(row[4]),
                            description=row[5],
                            splits=splits)

@dataclass
class Split:
    account_id: int = None
    account_name: str = None
    action: str = None
    amount: float = None

    def to_csv_row(self):
        return [self.account_id, self.account_name, self.action, self.amount]

@dataclass
class Metadata:
    tx_id_counter: int
    file_id_map: dict

    def log_post(self, file_path, num_entries):
        self.file_id_map[file_path] = {'start_id': self.tx_id_counter,
                                       'num_entries': num_entries}

        self.tx_id_counter += num_entries

    def get_all_imported_files(self):
        return list(self.file_id_map.keys())

    def to_dict(self):
        return dataclasses.asdict(self)

def parse_number(val, func):
    try:
        val = re.sub(r'[^\d.]', '', val) if type(val) is str else val
        return func(val)
    except ValueError:
        return None
