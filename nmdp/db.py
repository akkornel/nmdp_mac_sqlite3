#!python3
# vim: sw=4 ts=4 et

# This file is part of the NMDP SQLite3 Demo.
# 
# The NMDP SQLite3 Demo is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# The full text of the license is available in the `LICENSE` file, which is at
# the root of this repository.  It is also available at
# <https://www.gnu.org/licenses/>.

"""
This module contains the code used to create and open NMDP SQLite3 databases.
"""

# stdlib imports
import pathlib
import sqlite3
import sys

# Our own imports!
from nmdp.nmdp import NMDPConnection

# Constants
#SCHEMA_CODE: int = struct.unpack('i', struct.pack('=cccc', b'N', b'M', b'D', b'P'))[0]
SCHEMA_CODE: int = 1346653518
SCHEMA_VER: int = 3


def setup_db(
    path: pathlib.Path
) -> None:
    """Set up a NMDP DB

    Given a path, this function creates a new, empty NMDP SQLite3 database,
    using the latest schema.

    Once complete, the newly-created database is closed, so it will need to
    opened using `open_db`.

    NOTE: This program does not check if a file already exists at this path.

    :param path: The path to where the DB will be.
    """
    con = sqlite3.connect(path,
        autocommit=False,
    )
    cur = con.cursor()
    cur.execute("PRAGMA journal_mode = DELETE;")
    cur.execute(f"PRAGMA application_id = {SCHEMA_CODE};")
    cur.execute(f"PRAGMA user_version = {SCHEMA_VER};")

    cur.execute("""
        CREATE TABLE Files(
          path     TEXT    PRIMARY KEY,
          modified TEXT,
          comment  TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE Codes(
          code               TEXT  PRIMARY KEY,
          subtype_compressed BLOB
        );
    """)
    con.commit()
    cur.execute("PRAGMA optimize;")
    con.commit()
    cur.close()
    con.close()
    return


def open_db(
    path: pathlib.Path,
) -> NMDPConnection:
    """Open a NMDP DB

    Given a path, this function opens a NMDP SQLite3 database, checks that this
    is actually a NMDP SQLite3 database, and returns a NMDPConnection object
    representing the database.

    NOTE: Right now, this codes does not use exceptions.  Instead, it prints to
    stdout and exits the program.

    :param path: The path where the DB will be.

    :returns: A SQLite3 DB connection object.
    """

    # Connect, and check our application ID & user version
    con = sqlite3.connect(path,
        autocommit=False,
    )
    cur = con.cursor()
    appid_res = cur.execute("PRAGMA application_id")
    application_ids = appid_res.fetchall()
    if len(application_ids) != 1:
        print("Incorrect number of application IDs!")
        sys.exit(1)
    if application_ids[0][0] != SCHEMA_CODE:
        print(f"Schema code for DB is incorrect!  Code was {application_ids[0][0]}; should be {SCHEMA_CODE}.")
        sys.exit(1)

    userver_res = cur.execute("PRAGMA user_version")
    user_versions = userver_res.fetchall()
    if len(user_versions) != 1:
        print("Incorrect number of user versions!")
        sys.exit(1)
    if user_versions[0][0] != SCHEMA_VER:
        print(f"Schema version for DB is incorrect!  Code was {user_versions[0][0]}; should be {SCHEMA_VER}.")
        sys.exit(1)

    cur.close()
    return NMDPConnection(db=con)
