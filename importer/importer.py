from .datatypes import JournalEntry, Metadata
from core.data import CHART_OF_ACCOUNTS_PATH, MASTER_JOURNAL_PATH, METADATA_PATH
from pathlib import Path
import csv
import functools
import importer.transaction_file_parser
import re
import sys
import yaml

class Importer:

    def __init__(self, data_dir, parser_plugin):

        self.coa_file = data_dir / CHART_OF_ACCOUNTS_PATH
        self.mj_file = data_dir / MASTER_JOURNAL_PATH
        self.md_file = data_dir / METADATA_PATH

        self.source_dir = data_dir / 'source'
        self.preprocessed_dir = data_dir / 'preprocessed'
        self.pending_dir = data_dir / 'pending'
        self.posted_dir = data_dir / 'posted'

        self.parsers = importer.transaction_file_parser.BUILTIN_PARSERS
        self.parsers.update(parser_plugin.parsers)
        self.parser_configs = parser_plugin.parser_configs

        if not self.coa_file.is_file():
            print(f'Failed to instantiate importer: [master/chart_of_accounts.csv] not found. Exiting.')
            sys.exit(1)

        with open(self.coa_file) as f:
            self.chart_of_accounts = list(csv.DictReader(f))

        self.coa_id_to_name = {int(a['id']): a['name'] for a in self.chart_of_accounts}
        self.coa_name_to_id = {a['name']: int(a['id']) for a in self.chart_of_accounts}

    def import_transactions(self, files_arg=None):
        with MetadataManager(self.md_file) as metadata:

            if list(self.pending_dir.glob('**/*')):
                print('/pending dir must be empty before starting new import. Exiting.')
                sys.exit(1)

            files_to_import = []
            if files_arg is not None:
                files = [Path(f) for f in files_arg]
                files_to_import = sorted([f for f in files if (self.source_dir / f).is_file()])
            else:
                source_dir_files = [f.relative_to(self.source_dir) for f in self.source_dir.glob('**/*') if f.is_file()]
                already_imported = [Path(f) for f in metadata.get_all_imported_files()]
                files_to_import = sorted(list(set(source_dir_files) - set(already_imported)))

            if not files_to_import:
                print('No files to import. Exiting.')
                sys.exit(1)

            print('Importing files:')
            start_tx_id = metadata.tx_id_counter
            for source_file in files_to_import:

                source_file_rows = self._read_csv(self.source_dir / source_file)
                [_, parser_name, parser_config_id] = source_file_rows[0][0].split(':')
                data_rows = source_file_rows[2:]

                parser = self.parsers[parser_name]()
                config = self.parser_configs[parser_name][parser_config_id] if parser_name in self.parser_configs else None
                entries = parser.parse(data_rows, config)

                for id, entry in enumerate(entries, start_tx_id):
                    entry.source_file = source_file
                    entry.id = id
                start_tx_id += len(entries)

                entries.sort(key=functools.cmp_to_key(lambda a, b: -1 if a.input_type == 'edit' else 0))
                output = JournalEntry.to_csv(entries)
                self._write_csv(output, self.pending_dir / source_file)

                print(f'\t- imported {source_file}')

            print('Import succeeded. Imported files have been staged to /pending')

    def post_to_journal(self):
        with MetadataManager(self.md_file) as metadata:
            pending_files = [f.relative_to(self.pending_dir) for f in self.pending_dir.glob('**/*') if f.is_file()]
            if not pending_files:
                print('Post failed, no pending import run found. Exiting.')
                sys.exit(1)

            print(f'Post pending files:')
            num_entries_posted = 0
            for file in pending_files:
                input_entries = JournalEntry.from_csv(self._read_csv(self.pending_dir / file))
                output_entries = self._post_entries(input_entries)
                num_entries_posted += len(output_entries)

                output_rows = JournalEntry.to_csv(output_entries)
                self._write_csv(output_rows, (self.posted_dir / file))

                metadata.log_post(str(file), len(output_entries))
                print(f'\t- {file}')

            print(f'Validate posted journal entries from:')
            all_journal_entries = []
            for file in [f for f in self.posted_dir.glob('**/*') if f.is_file()]:
                all_journal_entries += JournalEntry.from_csv(self._read_csv(file))
                print(f'\t- {file}')

            self._validate_journal(all_journal_entries)

            output = [['id', 'date', 'description', 'account_id', 'account_name', 'action', 'amount']]
            for entry in all_journal_entries:
                for split in entry.splits:
                    output.append([entry.id, entry.date, entry.description] + split.to_csv_row())

            self._write_csv(output, self.mj_file)

            print(f'- wrote master journal file\nPost succeeded.')

    def _post_entries(self, journal_entries):

        # fetch any missing account_id's for the supplied account_name's,
        # and vise versa
        for entry in journal_entries:
            for split in entry.splits:
                if split.account_id is None:
                    if split.account_name in self.coa_name_to_id:
                        split.account_id = self.coa_name_to_id[split.account_name]
                    else:
                        print(f'Invalid account name in {entry}\nExiting.')
                        sys.exit(1)
                else:
                    if split.account_id in self.coa_id_to_name:
                        split.account_name = self.coa_id_to_name[split.account_id]
                    else:
                        print(f'Invalid account id in {entry}\nExiting.')
                        sys.exit(1)

                if split.account_id is None or not split.account_name:
                    print(f'No account specified for {entry}\nExiting.')
                    sys.exit(1)

        # validate that each entry's debits balance vs. its credits
        for entry in journal_entries:
            debits = [s.amount for s in entry.splits if s.action == 'debit']
            credits = [s.amount for s in entry.splits if s.action == 'credit']
            if sum(debits) != sum(credits):
                print(f'Splits do not balance for: {entry}\nExiting.')
                sys.exit(1)

        return journal_entries

    def _validate_journal(self, journal_entries):
        # validate that there are no duplicate CoA ids
        id_list = [int(account['id']) for account in self.chart_of_accounts]
        id_set = set(id_list)
        if len(id_list) != len(id_set):
            raise RuntimeError('Duplicate account ids found in CoA')

        # validate account references
        for entry in journal_entries:
            for split in entry.splits:
                if (split.account_id is None or
                    split.account_name != self.coa_id_to_name[split.account_id]):
                    print(f'Invalid account reference in {entry}\nExiting.')
                    sys.exit(1)

        # all entries have a unique id
        all_ids = [e.id for e in journal_entries]
        if len(all_ids) != len(set(all_ids)):
            raise RuntimeError('One or more journal entries have overlapping or missing ids')

    def generate_mergefiles(self, files_arg):
        files = [Path(f) for f in files_arg] if files_arg else self.source_dir.glob('**/*')
        input_files = [f.relative_to(self.source_dir) for f in files if f.is_file()]

        for file in input_files:
            from_posted = [e for e in JournalEntry.from_csv(self._read_csv(self.posted_dir / file))
                                   if e.input_type == 'edit']

            for e in from_posted:
                e.id = -1

            mergefile = self.pending_dir / file.parent / Path(str(file.name) + '_tomerge.csv')
            self._write_csv(JournalEntry.to_csv(from_posted), mergefile)
            print(f'- wrote mergefile {mergefile}')
            print('Mergefile generation succeeded.')

    def merge_edits(self):
        dir_files = [f.relative_to(self.pending_dir) for f in self.pending_dir.glob('**/*') if f.is_file()]
        pending_files = [f for f in dir_files if not re.findall(r'_tomerge\.csv$', f.name)]

        for pending_file in pending_files:

            to_merge_file = self.pending_dir / Path(str(pending_file) + '_tomerge.csv')
            if not to_merge_file.is_file():
                print(f'Merge file not found for {pending_file}.')
                continue

            print(f'Merging {to_merge_file} into {pending_file}\n')

            pending_entries = JournalEntry.from_csv(self._read_csv(self.pending_dir / pending_file))
            to_merge_entries = [e for e in JournalEntry.from_csv(self._read_csv(self.pending_dir / to_merge_file))
                                        if e.input_type == 'edit']

            for to_merge in to_merge_entries:
                for pending in pending_entries:
                    if ((pending.date == to_merge.date) and
                        (pending.description == to_merge.description) and
                        (pending.splits[0].amount == to_merge.splits[0].amount) and
                        (pending.splits[1].amount == to_merge.splits[1].amount)):
                        pending.splits = to_merge.splits

                print(f'- merged entry:\n{to_merge}\n')

            output = sorted(pending_entries, key=lambda e: e.id)
            self._write_csv(JournalEntry.to_csv(output), (self.pending_dir / pending_file))
            print(f'- deleted {to_merge_file}')

        print('Merge succeeded. Remember to DELETE the MERGE FILES before proceeding to post!')

    def _read_csv(self, file):
        with open(file) as f:
            return list(csv.reader(f))

    def _write_csv(self, rows, file):
        Path.mkdir(file.parent, parents=True, exist_ok=True)
        with open(file, 'w') as f:
            csv.writer(f, lineterminator='\n').writerows(rows)

class MetadataManager:
    def __init__(self, md_file):
        self.md_file = md_file

    def __enter__(self):
        Path(self.md_file).touch()
        with open(self.md_file, 'r') as f:
            data = yaml.safe_load(f)
            md = Metadata(**data) if data else Metadata(0, {})
            self.metadata = md
            return md

    def __exit__(self, type, value, traceback):
        if type is None and value is None and traceback is None:
            with open(self.md_file, 'w') as f:
                yaml.dump(self.metadata.to_dict(), f)
        else:
            return False

