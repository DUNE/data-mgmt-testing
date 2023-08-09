
from __future__ import absolute_import
from samweb_client import json, convert_from_unicode
from samweb_client.client import samweb_method
from samweb_client.http_client import escape_url_path
from samweb_client.exceptions import *

@samweb_method
def serverInfo(samweb, authenticate=False):
    """ Get information about the server.
    arguments:
        authenticate : force SSL connection (now ignored)
    """
    kwargs = {}
    response = samweb.getURL('', **kwargs)
    if 'json' in response.headers.get('Content-Type',''):
        return response.json()
    else:
        return response.text.rstrip()
