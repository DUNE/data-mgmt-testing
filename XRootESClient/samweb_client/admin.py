
from __future__ import absolute_import
from samweb_client import json, convert_from_unicode, Error
from samweb_client.client import samweb_method
from samweb_client.http_client import escape_url_path, SAMWebHTTPError, HTTPNotFound

@samweb_method
def listApplications(samweb, **queryCriteria):
    result = samweb.getURL('/values/applications', queryCriteria)
    return convert_from_unicode(result.json())

@samweb_method
def addApplication(samweb, family, name, version):
    return samweb.postURL('/values/applications', {"family":family, "name":name, "version":version}, role='*').text.rstrip()

@samweb_method
def listUsers(samweb):
    result = samweb.getURL('/users', compress=True)
    return convert_from_unicode(result.json())

@samweb_method
def listUsersByGroup(samweb, group):
    result = samweb.getURL('/values/groups/%s' % escape_url_path(group))
    return convert_from_unicode(result.json())

@samweb_method
def _describeUser(samweb, username, format=None):
    params = {}
    if format:
        params['format'] = format
    return samweb.getURL('/users/name/%s' % escape_url_path(username), params)

@samweb_method
def describeUser(samweb, username):
    result = samweb._describeUser(username)
    return convert_from_unicode(result.json())

@samweb_method
def describeUserText(samweb, username):
    result = samweb._describeUser(username, format='plain')
    return result.text.rstrip()

@samweb_method
def addUser(samweb, username, firstname=None, lastname=None, email=None, uid=None, groups=None):

    userdata = { 'username' : username }
    if firstname: userdata['first_name'] = firstname
    if lastname: userdata['last_name'] = lastname
    if email: userdata['email'] = email
    if uid is not None:
        try:
            uid = int(uid)
        except ValueError:
            raise ArgumentError("Invalid value for uid: %s" % uid)
        else:
            userdata['uid'] = uid
    if groups:
        userdata["groups"] = groups
    return samweb.postURL('/users', data=json.dumps(userdata), content_type='application/json', role='*').text.rstrip()

@samweb_method
def modifyUser(samweb, username, **args):
    return samweb.postURL('/users/name/%s' % escape_url_path(username), data=json.dumps(args), content_type='application/json').text.rstrip()

@samweb_method
def getAvailableValues(samweb):
    """ get the available names that can be used with listValues, addValues """
    return convert_from_unicode(samweb.getURL('/values?list=generic').json())

@samweb_method
def listValues(samweb, vtype):
    """ list values from database. This method tries to be generic, so vtype is
    passed directly to the web server
    arguments:
        vtype: string with values to return (ie data_tiers, groups)
    """
    try:
        return convert_from_unicode(samweb.getURL('/values/%s' % escape_url_path(vtype), compress=True).json())
    except HTTPNotFound as ex:
        raise Error("Unknown value type '%s'" % vtype)

@samweb_method
def addValue(samweb, vtype, *args, **kwargs):
    """ Add a new value to the database
    arguments:
        vtype: string giving the type of value to add ( ie data_tiers, groups)
        *args: arguments to pass to server
        **kwargs: keyword arguments to pass to server
    """
    # build the provided arguments into a parameter list. The args sequence is turned into multiple
    # "value" parameters
    postdata = dict(kwargs)
    if args: postdata["value"] = [str(i) for i in args]
    try:
        return samweb.postURL('/values/%s' % escape_url_path(vtype), postdata, role='*')
    except HTTPNotFound as ex:
        raise Error("Unknown value type '%s'" % vtype)

@samweb_method
def listParameters(samweb):
    """ list defined parameters """
    return convert_from_unicode(samweb.getURL('/values/parameters', compress=True).json())

@samweb_method
def listParameterValues(samweb, param):
    """ list the values for the given parameter
    arguments:
        param: parameter name
    """
    result = samweb.getURL('/values/parameters/%s?format=json' % escape_url_path(param), compress=True)
    return convert_from_unicode(result.json())

@samweb_method
def addParameter(samweb, name, data_type, category=None):
    """ Add a new parameter to the database
    arguments:
        name: Parameter name (either just the name, or category.name)
        datatype: data type for new parameter
    """
    data = { 'name' : name, 'data_type' : data_type }
    if category: data['category'] = category
    return samweb.postURL('/values/parameters', data, role='*')

@samweb_method
def listDataDisks(samweb):
    """ list defined data disks """
    return convert_from_unicode(samweb.getURL('/values/data_disks', compress=True).json())

@samweb_method
def addDataDisk(samweb, mount_point):
    """ Add a new data disk to the database
    arguments:
        mount_point: The mount point for the new disk
    """
    return samweb.postURL('/values/data_disks', {'mountpoint': mount_point}, role='*')
