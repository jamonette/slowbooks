from .datatypes import JournalEntry, Metadata
from pathlib import Path
from core.io import CHART_OF_ACCOUNTS_PATH, MASTER_JOURNAL_PATH, METADATA_PATH
import csv
import functools
import re
import sys
import yaml

class Importer:

    def __init__(self, data_dir, transaction_matcher):
        self.transaction_matcher = transaction_matcher

        self.coa_file = data_dir / CHART_OF_ACCOUNTS_PATH
        self.mj_file = data_dir / MASTER_JOURNAL_PATH
        self.md_file = data_dir / METADATA_PATH

        self.source_dir = data_dir / 'source'
        self.preprocessed_dir = data_dir / 'preprocessed'
        self.pending_dir = data_dir / 'pending'
        self.posted_dir = data_dir / 'posted'

        if not self.coa_file.is_file():
            print(f'Failed to instantiate importer: [master/chart_of_accounts.csv] not found. Exiting.')
            sys.exit(1)

        with open(self.coa_file) as f:
            self.chart_of_accounts = list(csv.DictReader(f))

        self.coa_id_to_name = {int(a['id']): a['name'] for a in self.chart_of_accounts}
        self.coa_name_to_id = {a['name']: int(a['id']) for a in self.chart_of_accounts}

    def import_transactions(self):
        with MetadataManager(self.md_file) as metadata:

            source_dir_files = [f.relative_to(self.source_dir) for f in self.source_dir.glob('**/*') if f.is_file()]
            already_imported = [Path(f) for f in metadata.get_all_imported_files()]
            files_to_import = sorted(list(set(source_dir_files) - set(already_imported)))

            self._validate_import_args(source_dir_files, metadata)
            start_id = metadata.start_import_run(files_to_import)

            for source_file in files_to_import:
                input_rows = self._read_csv(self.source_dir / source_file)
                (num_data_rows, output_rows) = self._preprocess_file(input_rows, start_id)
                start_id += num_data_rows

                self._write_csv(output_rows, (self.preprocessed_dir / source_file))
                print(f'- preprocessed {source_file}')

                input_rows = self._read_csv(self.preprocessed_dir / source_file)
                output_rows = self._match_transactions(input_rows, self.transaction_matcher)
                self._write_csv(output_rows, (self.pending_dir / source_file))
                print(f'- imported {source_file}')

            print('Import succeeded. Files staged to /pending')

    def reimport_transactions(self, files_arg):
        with MetadataManager(self.md_file) as metadata:
            files = [Path(f) for f in files_arg] if files_arg else self.source_dir.glob('**/*')
            files_to_reimport = sorted([f.relative_to(self.source_dir) for f in files if f.is_file()])

            self._validate_import_args(files_to_reimport, metadata)

            metadata.start_reimport_run(files_to_reimport)

            for file in [f for f in files_to_reimport]:
                input_rows = self._read_csv(self.preprocessed_dir / file)
                output_rows = self._match_transactions(input_rows, self.transaction_matcher)
                self._write_csv(output_rows, (self.pending_dir / file))

                print(f'- reimported {file}')

            print('Reimport succeeded. Files staged to /pending')

    def _validate_import_args(self, files, metadata):
        if list(self.pending_dir.glob('**/*')):
            print('/pending dir must be empty before starting new import. Exiting.')
            sys.exit(1)
        elif metadata.is_pending():
            print('Existing import run must be completed before running new import. Exiting.')
            sys.exit(1)
        if not files:
            print('No files to import. Exiting.')
            sys.exit(1)

    def _preprocess_file(self, input_rows, start_id):

        # header row 1: matcher directive
        # header row 2: orig. CSV header with 'id' field prepended
        header_rows = [input_rows[0], ['id'] + input_rows[1]]
        data_rows = input_rows[2:]

        # add id field to header row
        output_rows = header_rows
        for id, row in enumerate(data_rows, start_id):
            output_rows.append([id] + row)

        return (len(data_rows), output_rows)

    def _match_transactions(self, input_rows, transaction_matcher):
        [_, matcher_name, matcher_arg] = input_rows[0][0].split(':')
        data_rows = input_rows[2:]
        ids = [r[0] for r in data_rows]

        entries = []
        for id in ids:
            entries.append(JournalEntry(id=id))

        # As a convenience to the writers of matcher plugins, the matcher API assumes
        # that the CSV schema will match the _source_ file (without the prepended id
        # column from the preprocessing stage). Remove the id column here.
        for i, _ in enumerate(data_rows):
            data_rows[i].pop(0)

        transaction_matcher.\
            populate_entries(matcher_name=matcher_name,
                             matcher_arg=matcher_arg,
                             csv_rows=data_rows,
                             entries=entries)

        # sort the entries that need manual attention to the top
        # of the file for faster editing
        entries.sort(key=functools.cmp_to_key(lambda a, b: -1 if a.source == 'imported_edit' else 0))

        return JournalEntry.to_csv(entries)

    def post_to_journal(self):
        with MetadataManager(self.md_file) as metadata:
            pending_files = [f.relative_to(self.pending_dir) for f in self.pending_dir.glob('**/*') if f.is_file()]
            if not metadata.is_pending() and pending_files:
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

            metadata.finish_import_run(num_entries_posted)
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
        # validate that CoA account ids increase sequentially from 0
        all_account_ids = [int(account['id']) for account in self.chart_of_accounts]
        for i, id in enumerate(sorted(all_account_ids)):
            if id != i:
                raise RuntimeError(f'Account ids not sequential at {id}\nExiting.')

        # validate account references
        for entry in journal_entries:
            for split in entry.splits:
                if (split.account_id is None or
                    split.account_name != self.coa_id_to_name[split.account_id]):
                    print(f'Invalid account reference in {entry}\nExiting.')
                    sys.exit(1)

        # ids are sequential from 0
        for i, id in enumerate(sorted([e.id for e in journal_entries])):
           if id != i:
               raise RuntimeError(f'Entry ids not sequential at {i}')

    def generate_mergefiles(self, files_arg):
        files = [Path(f) for f in files_arg] if files_arg else self.source_dir.glob('**/*')
        input_files = [f.relative_to(self.source_dir) for f in files if f.is_file()]

        for file in input_files:
            from_posted = [e for e in JournalEntry.from_csv(self._read_csv(self.posted_dir / file))
                                   if e.source == 'imported_edited' or e.source == 'imported_edited_merged']

            for e in from_posted:
                e.source = 'imported_edited'

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
                print(f'Merge file not found for {pending_file}. Exiting.')
                sys.exit(1)

            print(f'Merging {to_merge_file} into {pending_file}\n')

            pending_entries = JournalEntry.from_csv(self._read_csv(self.pending_dir / pending_file))
            pending_by_id = {e.id: e for e in pending_entries}
            to_merge_entries = [e for e in JournalEntry.from_csv(self._read_csv(self.pending_dir / to_merge_file))
                                        if e.source == 'imported_edited']

            for e in to_merge_entries:
                e.source = 'imported_edited_merged'
                pending_by_id[e.id] = e
                print(f'- merged entry:\n{e}\n')

            output = sorted(list(pending_by_id.values()), key=lambda e: e.id)
            self._write_csv(JournalEntry.to_csv(output), (self.pending_dir / pending_file))
            Path.unlink(to_merge_file)
            print(f'- deleted {to_merge_file}')

        print('Merge succeeded.')

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
            md = Metadata.from_dict(data) if data else Metadata([])
            self.metadata = md
            return md

    def __exit__(self, type, value, traceback):
        if type is None and value is None and traceback is None:
            with open(self.md_file, 'w') as f:
                yaml.dump(self.metadata.to_dict(), f)
        else:
            return False

