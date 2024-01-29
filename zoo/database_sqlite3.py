import os
import re
import sqlite3
import pandas as pd
from baseobject import baseobject 

class database_sqlite3(baseobject):
    def __init__(self, database_path=None):
        """
        Initialize the SQLite database connection.
        """
        # Call the parent class's initializer
        super().__init__()

        # If no database path is provided, use an in-memory database
        self.database_path = ':memory:' if database_path is None else database_path

        try:
            # Establish a connection to the SQLite database
            self.connect = sqlite3.connect(self.database_path)

            # Create a cursor object for executing SQL statements
            self.cursor = self.connect.cursor()

            # Initialize a set to keep track of attached databases
            self.attach_db_set = set()

        except sqlite3.Error as e:
            # If an error occurs, set all attributes to None and re-raise the error
            self.connect = None
            self.cursor = None
            self.alias_count = None
            raise e

    def vacuum(self):
        """
        Perform a VACUUM operation to rebuild the database file, 
        repacking it into a minimal amount of disk space.
        """
        if self.connect is not None:
            self.connect.execute('VACUUM')
        return 

    def attach_database(self, database_filename: str):
        """
        Attach another database file to the current database connection.

        Args:
            database_filename (str): The path to the database file to attach.

        Returns:
            str: The alias of the attached database.
        """
        # Generate a unique alias for the attached database
        database_alias = f'attach_db_{self.alnum_uuid()}'
        
        # Add the alias to the set of attached databases
        self.attach_db_set.add(database_alias)

        # Execute the ATTACH DATABASE command
        command = f'ATTACH DATABASE "{database_filename}" AS {database_alias}'
        self.connect.execute(command)

        return database_alias

    def detach_database(self, database_alias: str):
        """
        Detach a previously attached database from the current database connection.

        Args:
            database_alias (str): The alias of the database to detach.
        """
        # Execute the DETACH DATABASE command
        command = f'DETACH DATABASE "{database_alias}"'
        self.connect.execute(command)

        # Remove the alias from the set of attached databases
        self.attach_db_set.remove(database_alias)

    def backup(self, filename=None):
        """
        Backup the current database to a new file.

        Args:
            filename (str, optional): The name of the backup file. If None, a name is generated.

        Returns:
            str: The name of the backup file.
        """
        # Generate a unique identifier
        uuid = self.alnum_uuid()

        # Determine the base name of the database file
        basename = 'memory' if self.database_path == ':memory:' else os.path.basename(self.database_path)
        basename = os.path.splitext(basename)[0]
        basename = f'{basename}.backup.{uuid}.sqlite'

        # If no filename is provided, use the base name
        filename = filename if filename is not None else basename

        # Check if the filename is valid
        if re.match(r'[a-zA-Z0-9_.-]+', filename) is None:
            raise ValueError('filename is illegal')

        # Create the path to the backup file
        database_path = os.path.join(self.download, filename)

        # Create a new database connection for the backup
        targetdb = database_sqlite3(database_path)

        # Backup the current database to the new file
        self.connect.backup(targetdb.connect, pages=20, sleep=0.0001)

        # Delete the new database connection
        del targetdb

        return filename

    def clone_database(self, srcdb):
        """
        Clone the source database to the current database.

        Args:
            srcdb (database_sqlite3): The source database to clone.
        """
        # Backup the source database to the current database
        srcdb.connect.backup(self.connect, pages=20, sleep=0.001)

    def close(self):
        """
        Close the current database connection and reset the object.
        """
        if self.connect is not None:
            # Close the connection
            self.connect.close()

            # Reset the object's attributes
            self.connect = None
            self.cursor = None
            self.attach_db_set = set()

        return

if __name__ ==  "__main__":
    db = database_sqlite3()
    db.backup()

    