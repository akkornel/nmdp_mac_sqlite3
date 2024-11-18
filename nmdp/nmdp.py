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
This module contains the classes used to work with an existing NMDP SQLite3
database.  It does not do anything related to database creation, upgrading, or
updating.
"""

# stdlib imports
import collections.abc
from collections.abc import Iterator
import dataclasses
import datetime
import sqlite3
import typing
import zlib

# If someone does `from nmdp.nmdp import *`, what do they get?
__all__ = [
    "NMDPCode", "NMDPCodes", "NMDPFile", "NMDPFiles", "NMDPConnection",
]

# Our classes!

@dataclasses.dataclass(frozen=True)
class NMDPCode():
    """A single NMDP MAC code"""

    subtype: str
    """The subtype"""


class NMDPCodes(collections.abc.MutableMapping[str, NMDPCode]):
    """A mapping of NMDP codes to their subtypes.

    You can treat this as a Python dictionary, where the keys are MACs and the
    values are subtypes.  This 'dictionary' is linked to a SQLite3 database, so
    any changes you make cause updates in the database.

    Implementation Note: This class does not commit.  It's the responsibility
    of the client to commit, should any changes be made.
    """

    db: sqlite3.Connection
    """Provides direct access to the underlying SQLite3 database.

    WARNING: Do not use unless you know what you are doing!
    """

    def __getitem__(self, key: str) -> NMDPCode:
        """Return a subtype for a given code."""
        # WARNING: We cannot use the `in` test here, because the `__contains__`
        # method calls us!
        cur = self.db.cursor()
        res = cur.execute(
            """
            SELECT subtype_compressed
            FROM Codes
            WHERE code = ?
            """,
            (
                key,
            ),
        )
        raw_val = res.fetchone()
        if raw_val is None:
            raise KeyError(key)
        return NMDPCode(
            subtype=zlib.decompress(raw_val[0]).decode('ASCII')
        )

    def __setitem__(self, key: str, value: NMDPCode) -> None:
        """Set a given code to a subtype, creating or overwriting as needed."""
        cur = self.db.cursor()
        params = {
            "code": key,
            "subtype_compressed": zlib.compress(value.subtype.encode('ASCII')),
        }
        res = cur.execute(
            """
            INSERT INTO Codes (
                code,
                subtype_compressed)
            VALUES (
                :code,
                :subtype_compressed)
            ON CONFLICT (code) DO
            UPDATE
            SET
                subtype_compressed = :subtype_compressed
            WHERE
                code = :code
            """,
            params,
        )
        cur.close()

    def __delitem__(self, key: str) -> None:
        """Delete a code"""
        if key not in self:
            raise KeyError(key)
        cur = self.db.cursor()
        res = cur.execute(
            "DELETE FROM Codes WHERE code = ?",
            (
                key,
            ),
        )
        cur.close()
        return

    def __iter__(self) -> Iterator[str]:
        """Iterate through all codes"""
        cur = self.db.cursor()
        res = cur.execute(
            """
            SELECT (code)
            FROM Codes
            """
        )
        for row in res:
            yield row[0]
        return

    def __len__(self) -> int:
        """Return the number of codes in the database"""
        cur = self.db.cursor()
        res = cur.execute("SELECT COUNT(*) FROM Codes")
        item_count = res.fetchone()[0]
        cur.close()
        return typing.cast(int, item_count)


@dataclasses.dataclass(frozen=True)
class NMDPFile():
    """A single file from the NMDP zip file"""

    modified: datetime.datetime
    """The last-modified time of the file, as a naive datetime."""

    comment: str
    """The first line of the file, without its newline."""


class NMDPFiles(collections.abc.MutableMapping[str, NMDPFile]):
    """A mapping of NMDP zip-file names to their metadata.

    You can treat this as a Python dictionary, where the keys are filenames and
    the values are the metadata.  This 'dictionary' is linked to a SQLite3
    database, so any changes you make cause updates in the database.

    Implementation Note: This class does not commit.  It's the responsibility
    of the client to commit, should any changes be made.
    """

    db: sqlite3.Connection
    """Provides direct access to the underlying SQLite3 database.

    WARNING: Do not use unless you know what you are doing!
    """

    def __getitem__(self, key: str) -> NMDPFile:
        """Return the metadata for a file from the NMDP zip file."""
        # WARNING: We cannot use the `in` test here, because the `__contains__`
        # method calls us!
        cur = self.db.cursor()
        res = cur.execute(
            """
            SELECT modified, comment
            FROM Files
            WHERE path = ?
            """,
            (
                key,
            ),
        )
        raw_val = res.fetchone()
        if raw_val is None:
            raise KeyError(key)
        return NMDPFile(
            modified=datetime.datetime.fromisoformat(raw_val[0]),
            comment=raw_val[1],
        )

    def __setitem__(self, key: str, value: NMDPFile) -> None:
        """Set the metadata for a given NMDP zip-file entry"""
        cur = self.db.cursor()
        params = {
            "path": key,
            "modified": value.modified.isoformat(),
            "comment": value.comment,
        }
        res = cur.execute(
            """
            INSERT INTO Files (
                path,
                modified,
                comment)
            VALUES (
                :path,
                :modified,
                :comment)
            ON CONFLICT (path) DO
            UPDATE
            SET
                modified = :modified,
                comment  = :comment
            WHERE
                path = :path
            """,
            params,
        )
        cur.close()

    def __delitem__(self, key: str) -> None:
        """Delete a NMDP zip-file entry"""
        if key not in self:
            raise KeyError(key)
        cur = self.db.cursor()
        res = cur.execute(
            "DELETE FROM Files WHERE path = ?",
            (
                key,
            ),
        )
        cur.close()
        return

    def __iter__(self) -> Iterator[str]:
        """Iterate through all NMDP zip-file names"""
        cur = self.db.cursor()
        res = cur.execute(
            """
            SELECT (path)
            FROM Files
            """
        )
        for row in res:
            yield row[0]
        return

    def __len__(self) -> int:
        """Return the number of files in the NMDP zip file"""
        cur = self.db.cursor()
        res = cur.execute("SELECT COUNT(*) FROM Files")
        item_count = res.fetchone()[0]
        cur.close()
        return typing.cast(int, item_count)


@dataclasses.dataclass
class NMDPConnection:
    """A connection to the NMDP SQLite3 database.

    This gives you access to the contents of a NMDP SQLite3 database.  You can
    certainly query the database directly, but this is often more convenient.

    This is instantiated and returned by `nmdp.db.open_db`.

    When an instance of this class is cleaned up by the interpreter, that is
    when the database file is cleaned up and closed.
    """

    files: NMDPFiles = dataclasses.field(init=False)
    """The list of files from the NMDP zip file.
    """

    codes: NMDPCodes = dataclasses.field(init=False)
    """The mapping of MAC codes to subtypes.
    """

    db: sqlite3.Connection
    """Provides direct access to the underlying SQLite3 database.

    WARNING: Do not use unless you know what you are doing!
    """

    need_optimize: bool = False
    """True if a commit has been made to the database.

    WARNING: Do not change unless you know what you are doing!
    """

    def __post_init__(self) -> None:
        self.files = NMDPFiles()
        self.files.db = self.db
        self.codes = NMDPCodes()
        self.codes.db = self.db

    def commit(self) -> None:
        """Commit to the database."""
        self.db.commit()
        self.need_optimize = True

    def rollback(self) -> None:
        """Roll back an open commit."""
        self.db.rollback()

    def __del__(self) -> None:
        self.db.rollback()
        # No need to optimize if we made no changes
        if self.need_optimize:
            self.db.cursor().execute("PRAGMA OPTIMIZE")
            self.db.commit()
        self.db.close()



