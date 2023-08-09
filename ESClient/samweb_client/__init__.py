from __future__ import absolute_import

import os, pwd

# Get a json library if available. Try the standard library if available;
# simplejson if that's available, else fall back to old (py2.4 compatible simplejson)
import json
import sys
import six

if six.PY2:
    # In python 2 return a plain byte string (just returns the input and ignores the encoding)
    # In python 3 return unicode string
    def convert_to_string(input, encoding=None):
        return str(input)

    # In python 2 recursively convert ascii strings to byte strings
    # In python 3 do nothing - all strings are unicode
    def convert_from_unicode(input):
        """ convert an object structure (specifically those returned via json
        to use plain strings insead of unicode """
        if isinstance(input, dict):
            return type(input)( (convert_from_unicode(key), convert_from_unicode(value)) for key, value in six.iteritems(input))
        elif isinstance(input, list):
            return [convert_from_unicode(element) for element in input]
        elif isinstance(input, six.text_type):
            try:
                return input.encode('ascii')
            except UnicodeEncodeError:
                # can't be represented as ascii; leave it as unicode
                return input
        else:
            return input
        
else:
    def convert_to_string(input, encoding='utf-8'):
        if isinstance(input, bytes):
            return input.decode(encoding)
        return str(input)
    
    def convert_from_unicode(input):
        return input
    


from .exceptions import *

from . import http_client
from .client import SAMWebClient, ExperimentNotDefined, get_version
from . import projects
from . import files
from . import definitions
from . import admin
from . import misc
