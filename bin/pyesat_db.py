import os
from pathlib import Path
import pathlib
import shutil
import sys
import argparse

import pyesat.earthdata
import pyesat.credentials

def backup_database(database):
    # backup the database
    if database.exists():
        if database.with_suffix('.bak').exists():
            database.with_suffix('.bak').unlink()
        shutil.move(database, database.with_suffix('.bak'))

    else:
        print("Database does not exist.")

def check_paths(database: Path, overwrite: bool) -> Path:
    if database == 'default':
        database = pyesat.earthdata.db_path
    else:
        database = pyesat.earthdata.db_path.parent / database
    
    # try to backup the database
    if overwrite:
        backup_database(database)

    # check if the database exists
    if not database.exists():
        database.parent.mkdir(parents=True, exist_ok=True)
        # move current data
        database.touch()
        database.chmod(0o600)
        
    else:
        print("Database already exists. Skipping creation.")
    return database

def init_db(database: str, overwrite: bool) -> None:
    # setup the database path
    try:
        database = check_paths(database, overwrite)
    except:
        print("Error setting up paths.")
        sys.exit(1)
    # initialize the database
    try:
        # write a decorator to handle printing before and after the function is executed
        if args.verbose:
            print("Initializing database... ", end='')
        pyesat.earthdata.create_orm_classes()
        if args.verbose:
            print("done.")
    except:
        print("Error initializing database.")
        sys.exit(1)

def main():
    # initialize the database
    if args.init:
        if args.verbose:
            print("Initializing database...")
        init_db(args.database, args.overwrite)
        if args.verbose:
            print("Database initialized.")
    # update the database
    if args.update:
        if args.verbose:
            print("Updating database...")
            earthdata.update_database()`
        # update the database
        if args.verbose:
            print("Database updated.")

    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize the database.")
    # add arguments here
    parser.add_argument( "-v", "--verbose", action="store_true", help="increase output verbosity")
    parser.add_argument('-i', '--info', action='store_true', help='print information')
    parser.add_argument('-I', '--init', action='store_true', help='init the database')
    parser.add_argument("-O", "--overwrite", action="store_true", help="overwrite existing database")
    parser.add_argument("-u", "--update", action="store_true", help="update existing database")
    parser.add_argument("-d", "--database", type=str, help="database file", default='default')
    parser.add_argument("-c", "--config", type=str, help="configuration file")
    parser.add_argument("-s", "--start", type=str, help="start date")
    parser.add_argument("-e", "--end", type=str, help="end date")
    parser.add_argument("-n", "--dry-run", action="store_true", help="dry run")


    # write argparse code here
    args = parser.parse_args()

    main()
    