from dataclasses import dataclass, field
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
    source: str = None

    @staticmethod
    def to_csv(entries):
        header_row =\
            [['id', 'source', 'date', 'description',
             'split_0_account_id', 'split_0_account_name', 'split_0_account_acount', 'split_0_amount',
             'split_1_account_id', 'split_1_account_name', 'split_1_account_acount', 'split_1_amount']]

        entry_rows = []
        for e in entries:
            row = [e.id, e.source, e.date, e.description]
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
        num_je_cols = 4
        num_split_cols = 4
        num_splits = int(len(row[num_je_cols:]) / num_split_cols)

        # after the JE columns, determine number of splits by dividing the remaining
        # number of columns by the number of cols used to represent a split
        split_vals = [row[num_je_cols:][(i * num_split_cols):((i + 1) * num_split_cols)] for i in range(num_splits)]
        splits = [Split(account_id=_try_parse(vals[0], int),
                        account_name=vals[1],
                        action=vals[2],
                        amount=_try_parse(vals[3], float)) for vals in split_vals]

        return JournalEntry(id=int(row[0]),
                          source=row[1],
                          date=dateutil.parser.parse(row[2]),
                          description=row[3],
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
    import_runs: list = field(default_factory=lambda: [])

    def start_import_run(self, files):
        # pick up the starting journal entry id from the last non-reimport run,
        # or initialize to 0 if this is the first run
        last_non_reimport = next(filter(lambda r: not r.is_reimport, reversed(self.import_runs)), None)
        first_id = last_non_reimport.last_entry_id + 1 if last_non_reimport else 0

        self.import_runs.append(
            ImportRun(id=self._next_run_id(),
                      import_timestamp=datetime.now(),
                      is_reimport=False,
                      first_entry_id=first_id,
                      files_processed=[str(f) for f in files]))

        return first_id

    def start_reimport_run(self, files):
        self.import_runs.append(
            ImportRun(id=self._next_run_id(),
                      import_timestamp=datetime.now(),
                      is_reimport=True,
                      files_processed=[str(f) for f in files]))

    def _next_run_id(self):
        prev_import_run = self.import_runs[-1] if self.import_runs else None
        return prev_import_run.id + 1 if prev_import_run else 0

    def finish_import_run(self, num_entries_imported):
        curr_run = self.import_runs[-1]
        curr_run.completed_timestamp = datetime.now()
        if not curr_run.is_reimport and num_entries_imported:
            curr_run.last_entry_id = curr_run.first_entry_id + num_entries_imported - 1

    def get_all_imported_files(self):
        fp = set()
        for run in self.import_runs:
            for f in run.files_processed:
                fp.add(f)
        return fp

    def is_pending(self):
        return (self.import_runs[-1].completed_timestamp is None) if self.import_runs else False

    @staticmethod
    def from_dict(data):
        return Metadata(import_runs=[ImportRun(**r) for r in data['import_runs']])

    def to_dict(self):
        return {'import_runs': [dataclasses.asdict(r) for r in self.import_runs]}

@dataclass
class ImportRun:
    id: int = None
    import_timestamp: datetime = None
    completed_timestamp: datetime = None
    is_reimport: bool = None
    first_entry_id: int = None
    last_entry_id: int = None
    files_processed: list = None

def _try_parse(val, func):
    try:
        val = re.sub(r'[^\d.]', '', val) if type(val) is str else val
        return func(val)
    except:
        return None
