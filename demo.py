#!/usr/bin/env python3
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
This program demonstreates using the NMDP SQLite3 database.

The program takes one command-line argument, which is the path to a NMDP
SQLite3 database file.  If the database does not exist, it is created.

The program downloads the latest NMDP zip file (from the path in `NMDP_URL`).
It looks inside the zip file for the MAC file (looking for the file name in
`NMDP_FILENAME`).  A temporary file is used to store the zip file, which should
be cleaned up automatically when the program finishes running.
"""

# stdlib imports
import argparse
import datetime
import io
import os
import pathlib
import sys
import tempfile
import zipfile

# Third-party libraries
import requests

# Our own imports!
from nmdp import *

# Constants
NMDP_URL: str = 'https://hml.nmdp.org/mac/files/alpha.v3.zip'
NMDP_FILENAME: str = 'alpha.v3.txt'


def get_zipfile(
    url: str,
) -> zipfile.ZipFile:
    """Return a Zipfile from a given URL.

    If the web server runs into an error (like connection timed out), or a
    non-200 HTTP code is returned, the appropriate exception from the Requests
    package will be raised to the caller.

    :param url: The URL to download.

    :returns: The ZipFile instance for the downloaded Zip file.
    """

    # Make a temporary file to hold the NMDP Zip file
    tempfile_obj = tempfile.TemporaryFile(mode='w+b')

    # Request the download, and write into the tempfile, in 1MiB chunks.
    download_obj = requests.get(NMDP_URL, stream=True)
    for chunk in download_obj.iter_content(chunk_size=1024*1024):
        tempfile_obj.write(chunk)
    
    # If there was a problem with the download, then throw exception now.
    download_obj.raise_for_status()

    
    # Move our tempfile pointer back to the start, and get our Zipfile
    tempfile_obj.seek(0, os.SEEK_SET)
    tempfile_zip = zipfile.ZipFile(tempfile_obj, 'r')

    return tempfile_zip

#
# MAIN CODE
#

if __name__ == "__main__":
    #
    # Prep the DB
    #

    # Parse command-line arguments, to get our DB path.
    argp = argparse.ArgumentParser()
    argp.add_argument('filename',
        type=pathlib.Path,
    )
    args = argp.parse_args()

    if not args.filename.exists():
        # The path doesn't exist, so we need to create it!
        setup_db(args.filename)

    # Open our DB
    nmdp_db = open_db(args.filename)

    #
    # Prep the Zip file
    #

    # Download and open our NMDP zip file
    print("Downloading and opening NMDP zip file")
    tempfile_zip = get_zipfile(url=NMDP_URL)

    # Build a list of files in the Zip.
    zipfile_files: dict[str, datetime.datetime] = dict()

    # For each file, we need to reocrd the file's path and last-modified
    # datetime (which does not have a timezone).
    for tempfile_item in tempfile_zip.infolist():
        tempfile_timetuple = tempfile_item.date_time
        tempfile_datetime = datetime.datetime(
            year=tempfile_timetuple[0],
            month=tempfile_timetuple[1],
            day=tempfile_timetuple[2],
            hour=tempfile_timetuple[3],
            minute=tempfile_timetuple[4],
            second=tempfile_timetuple[5],
        )
        zipfile_files[tempfile_item.filename] = tempfile_datetime

    # We should have just one file, named "alpha.v3.txt"
    if len(zipfile_files) != 1:
        print("ERROR: More or less than 1 file in the Zip file")
        sys.exit(1)
    elif NMDP_FILENAME not in zipfile_files:
        print(f"ERROR: {NMDP_FILENAME} is missing from the Zip file")
        sys.exit(1)

    # Open our `alpha.v3.txt` file, and extract the comment & header rows.
    # The `zipfile` module lets us open files, instead of first extracting them
    # to disk.  But, it opens the files as binary, and we want the files as
    # text, so we need to wrap the binary file objct.
    nmdp_file_bin_obj = tempfile_zip.open(NMDP_FILENAME, mode='r')
    nmdp_file_obj = io.TextIOWrapper(
        nmdp_file_bin_obj,
        encoding='ASCII',
    )
    # Remember to strip off the newlines.
    nmdp_file_comment_line = nmdp_file_obj.readline().rstrip()
    nmdp_file_header_line = nmdp_file_obj.readline().rstrip()

    # The header line should split on the tab character into three fields.
    nmdp_file_header_split = nmdp_file_header_line.split("\t")
    nmdp_file_fields = ('*', 'CODE', 'SUBTYPE')

    # Make sure we have the expected number of fields, in the correct order.
    if len(nmdp_file_header_split) != len(nmdp_file_fields):
        print(f"ERROR: Header line did not split into {len(nmdp_file_fields)} fields")
        sys.exit(1)
    for (field_expected, field_actual) in zip(nmdp_file_fields, nmdp_file_header_split):
        if field_expected != field_actual:
            print(f"ERROR: Header line field that should be '{field_expected}' is '{field_actual}'")
            sys.exit(1)

    # Our text file looks good to go!

    #
    # Check if we need to update
    #

    # Do we need to update the DB?  We can skip the update if…
    # * The TSV file is in the DB; and
    # * The DB entry's last-modified time is the same as the zipfile's.
    # * The DB entry's comment line matchches the zipfile's.
    if (
            (NMDP_FILENAME in nmdp_db.files)
            and (
                nmdp_db.files[NMDP_FILENAME].modified ==
                zipfile_files[NMDP_FILENAME]
            )
            and (
                nmdp_db.files[NMDP_FILENAME].comment == nmdp_file_comment_line
            )
    ):
        need_to_update = False
    else:
        need_to_update = True
        
    # Now let's do what we need to do!
    if not need_to_update:
        print("No need to update, data are current!")
    else:
        print("We need to update!")

        # NOTE: Throughout all of this, we will not commit until the very end.
        # That way, if we hit an error, the database remains in a consistent
        # state.

        # First, update the file's datetime in the DB
        nmdp_db.files[NMDP_FILENAME] = NMDPFile(
            modified=zipfile_files[NMDP_FILENAME],
            comment=nmdp_file_comment_line,
        )

        # Make a list of all the codes currently in our DB, so we can determine
        # which need to be removed.
        db_codes_to_remove: set[str] = set(nmdp_db.codes.keys())

        # Also keep track of codes added and changed
        db_codes_added: set[str] = set()
        db_codes_changed: dict[str, tuple[NMDPCode, NMDPCode]] =  dict()

        # Now, update our DB using the contents of the Zip file!
        for line in nmdp_file_obj:
            line = line.rstrip()
            if len(line) == 0:
                continue
            line_parts = line.split("\t")
            
            # We want the code and the subtype
            file_star = line_parts[0]
            file_code = line_parts[1]
            file_subtype = line_parts[2]
            file_code_obj = NMDPCode(
                subtype=file_subtype,
            )

            # Is this a new code?
            if file_code not in db_codes_to_remove:
                # It's a new code!  Add code to our set, and add to DB
                db_codes_added.add(file_code)
                nmdp_db.codes[file_code] = file_code_obj
            else:
                # It's a code we've seen before.

                # Remove it from the list of codes to delete.
                db_codes_to_remove.remove(file_code)

                # Is this code changed?
                if nmdp_db.codes[file_code] != file_code_obj:
                    # The code changed!
                    # Add the code to the changed list, and update the DB
                    db_codes_changed[file_code] = (
                        nmdp_db.codes[file_code],
                        file_code_obj
                    )
                    nmdp_db.codes[file_code] = file_code_obj
                else:
                    # No code change, so there's nothing else to do.
                    pass

        # We've now looped through the entire file.
        # So, we can close out our text and Zip file stuff
        del nmdp_file_obj
        nmdp_file_bin_obj.close()
        tempfile_zip.close()

        # db_codes_to_remove now contains our list of codes that have been
        # deleted from the NMDP file.  Delete from our DB.
        # But before deleting, record the actual code, so we can print it later.
        db_codes_removed: dict[str, NMDPCode] = dict()
        for code_to_remove in db_codes_to_remove:
            db_codes_removed[code_to_remove] = nmdp_db.codes[code_to_remove]
            del nmdp_db.codes[code_to_remove]

        # We're done modifying the database, so commit and close the DB file.
        nmdp_db.commit()
        del nmdp_db

        # db_codes_added is the list of codes added, and db_codes_changed is
        # the list of codes changed.  Output some stats!
        # NOTE: Since it's extremely unusual for codes to change or be deleted,
        # if we see one of those, then print details.
        print(f"Codes Added:   {len(db_codes_added)}")
        print(f"Codes Changed: {len(db_codes_changed)}")
        if len(db_codes_changed) > 0:
            for code_changed in db_codes_changed:
                old_subtype = db_codes_changed[code_changed][0].subtype
                new_subtype = db_codes_changed[code_changed][1].subtype
                print(f"{code_changed}: {old_subtype} -> {new_subtype}")
        print(f"Codes Deleted: {len(db_codes_removed)}")
        if len(db_codes_removed) > 0:
            for code_removed in db_codes_removed:
                old_subtype = db_codes_removed[code_removed].subtype
                print(f"{code_removed}: {old_subtype}")

        # All done!
        sys.exit(0)
