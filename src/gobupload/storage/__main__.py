"""Storage commands

This command line script can be used to clear the database or truncate the
tables. For now this is intented to reduce the manual steps needed when
changing models or other database tables.

     python -m gobupload.storage drop <table> or --all
     python -m gobupload.storage truncate <table> or --all
     python -m gobupload.storage init
"""
import argparse

from gobupload.storage.handler import GOBStorageHandler


parser = argparse.ArgumentParser(
    prog='python -m gobupload.storage',
    description='Perform database mutations',
    epilog='Generieke Ontsluiting Basisregistraties')

parser.add_argument('command',
                    type=str,
                    help='the command to perform on the database')

parser.add_argument('table',
                    nargs='?',
                    type=str,
                    help='the specific table to perform the command on')

parser.add_argument('--all',
                    action='store_true',
                    help='flag to perform the command on all tables')

args = parser.parse_args()

# Return an error if no table or --all is provided when dropping or truncating a table
if args.command in ('drop', 'truncate') and not args.table and not args.all:
    parser.error('a table or --all is required when trying to drop or truncate')

storage = GOBStorageHandler()
tables = [args.table] if not args.all else storage.ALL_TABLES

if args.command == 'drop':
    storage.drop_tables(tables)

if args.command == 'truncate':
    storage.truncate_tables(tables)

if args.command == 'init':
    storage.init_storage()
