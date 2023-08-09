"""

Exception classes. The specific exception classes like "FileNotFound" are dynamically generated from the server response
This has the consequence that

from samweb_client.exceptions import *

won't import these names because they aren't created until the server has been contacted.

Instead you have to do

catch samweb_client.exceptions.UserNotFound as ex:

The base Error, SAMWebConnectionError, SAMWebHTTPError classes are always available
Some more common Exceptions, like FileNotFound are always available

Be warned that this will not catch typos - any name in samweb_client.exceptions will be treated as valid as the client has 
no way of knowing all the exception types that can be produced in advance

"""
from __future__ import absolute_import


# The real magic happens in the _Exceptions class in the _exceptions module
# which is substituted for this in sys.modules
# All the code lives in _exceptions otherwise there were issues when shutting
# down the interpreter
from . import _exceptions
import sys
sys.modules[__name__] = _exceptions._Exceptions()


