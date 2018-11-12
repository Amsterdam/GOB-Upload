"""Storage commands

This command line script can be used to clear the database or truncate the
tables. For now this is intented to reduce the manual steps needed when
changing models or other database tables.

     python -m gobupload.storage resetdb
"""
import argparse
import getpass

from gobupload.config import GOB_DB
from gobupload.storage.handler import GOBStorageHandler


parser = argparse.ArgumentParser(
    prog='python -m gobupload.storage',
    description='Perform database mutations',
    epilog='Generieke Ontsluiting Basisregistraties')

command = parser.add_subparsers(title='the command to perform',
                                dest='command',
                                metavar='command')
command.required = True
command.add_parser('resetdb', help="reset the database to it's empty state")

args = parser.parse_args()

confirm = getpass.getpass("""You have requested a reset of the database.
This will IRREVERSIBLY DESTROY all data currently in the GOB database,
and return each table to an empty state.
Are you sure you want to do this?
    Type the database password to continue, or 'no' to cancel: """)

if confirm == GOB_DB['password']:
    storage = GOBStorageHandler()

    if args.command == 'resetdb':
        # Drop all tables and re-initialize the database
        storage.drop_tables()
        storage.init_storage()
else:
    print("Database reset cancelled.")
