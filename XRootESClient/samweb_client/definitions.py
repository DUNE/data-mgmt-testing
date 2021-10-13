""" Client methods relating to definitions """

from __future__ import absolute_import
from samweb_client import json, convert_from_unicode, convert_to_string
from samweb_client.client import samweb_method
from samweb_client.http_client import escape_url_path, escape_url_component
from samweb_client.exceptions import *


from six.moves import filter

@samweb_method
def listDefinitions(samweb, stream=False, **queryCriteria):
    """ List definitions matching given query parameters:
    arguments:
        one or more key=string value pairs to pass to server
        stream: boolean: if True, the results will be returned progressively
    """
    params = dict(queryCriteria)
    params['format'] = 'plain'
    result = samweb.getURL('/definitions/list', params, stream=True, compress=True)
    output = filter( None, (convert_to_string(l).strip() for l in result.iter_lines()) )
    if stream: return output
    else: return list(output)

def _descDefinitionURL(defname):
    return '/definitions/name/' + escape_url_component(defname)

@samweb_method
def descDefinitionDict(samweb, defname):
    """ Describe a dataset definition
    arguments:
        definition name
    """
    result = samweb.getURL(_descDefinitionURL(defname))
    return convert_from_unicode(result.json())

@samweb_method
def descDefinition(samweb, defname):
    """ Describe a dataset definition
    arguments:
        definition name
    """
    result = samweb.getURL(_descDefinitionURL(defname), {'format':'plain'})
    return result.text.rstrip()

@samweb_method
def createDefinition(samweb, defname, dims, user=None, group=None, description=None):
    """ Create a new dataset definition
    arguments:
        definition name
        dimensions string
        user: username (default None)
        group: group name (default None)
        description: description of new definition (default None)
    """

    params = { "defname": defname,
             "dims": dims,
             "group": group or samweb.group,
             }
    if description:
        params["description"] = description
    if user:
        params["user"] = user

    result = samweb.postURL('/definitions/create', params)
    return result.json()

@samweb_method
def modifyDefinition(samweb, existing_defname, defname=None, description=None):
    """ Modify a dataset definition
    arguments:
        existing definition name
        defname: new name for definition
        description: new description
    """
    params = {}
    if defname:
        params["defname"] = defname
    if description:
        params["description"] = description
    samweb.putURL('/definitions/name/%s' % escape_url_component(existing_defname), params, role='*')

@samweb_method
def deleteDefinition(samweb, defname):
    """ Delete a dataset definition
    arguments:
        definition name

    (Definitions that have already been used cannot be deleted)
    """
    result = samweb.deleteURL('/definitions/name/%s' % escape_url_component(defname), {})
    return result.text.rstrip()

@samweb_method
def takeSnapshot(samweb, defname, group=None, max_age=None, allow_empty=None):
    """ Create a snapshot for a existing definition
    arguments:
        definition name
        group: group (default experiment name)
    """
    if not group: group = samweb.group
    params = {"group": group}
    if max_age is not None:
        params['max_age'] = max_age
    if allow_empty is not None:
        params['allow_empty'] = allow_empty
    result = samweb.postURL('/definitions/name/%s/snapshot?format=plain' % escape_url_component(defname), params)
    return int(result.text.rstrip())

@samweb_method
def snapshotInfo(samweb, defname=None, snapshot_id=None, as_text=False):
    """ Get info on either the most recent snapshot for a definition
    or a specific snapshot id
    arguments:
        defname: definition name (default None)
        snapshot_id: snapshot_id (default None)
        as_text: return the result as human readable string (default False)
    """
    if as_text:
        fmt='plain'
    else:
        fmt='json'

    if snapshot_id is not None:
        result = samweb.getURL('/definitions/snapshots/id/%d' % snapshot_id, {'format': fmt})
    else:
        result = samweb.getURL('/definitions/name/%s/snapshot' % escape_url_component(defname), {'format': fmt})

    if as_text:
        return result.text.rstrip()
    else:
        return result.json()
