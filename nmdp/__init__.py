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
This module is a convenience.  It lets someone do `import nmdp` and get access
to the stuff they probably want.
"""

from nmdp.nmdp import *
from nmdp.db import *

# If someone does `from nmdp import *`, what do they get?
__all__ = [
    # From nmdp.nmdp
    "NMDPCode", "NMDPCodes", "NMDPFile", "NMDPFiles", "NMDPConnection",

    # From nmdp.db
    'setup_db', 'open_db',
]
