from __future__ import absolute_import

from samweb_client import json, convert_from_unicode, convert_to_string
from samweb_client.client import samweb_method
from samweb_client.http_client import escape_url_path, escape_url_component
from samweb_client.exceptions import *


import six
from six.moves import filter

try:
    from collections import namedtuple
except ImportError:
    def fileinfo(*args): return tuple(args)
else:
    fileinfo = namedtuple("fileinfo", ["file_name", "file_id", "file_size", "event_count"])

def _make_file_info(lines):
    for l in lines:
        values = convert_to_string(l).split()
        if values:
            try:
                yield fileinfo( values[0], int(values[1]), int(values[2]), int(values[3])  )
            except Exception:
                raise Error("Error while decoding file list output from server")

def _chunk(iterator, chunksize):
    """ Helper to divide an iterator into chunks of a given size """
    from itertools import islice
    while True:
        l = list(islice(iterator, chunksize))
        if l: yield l
        else: return

@samweb_method
def getAvailableDimensions(samweb):
    """ List the available dimensions """
    result = samweb.getURL('/files/list/dimensions?format=json&descriptions=1')
    return convert_from_unicode(result.json())

@samweb_method
def _callDimensions(samweb, url, dimensions, params=None,stream=False, compress=True):
    """ Call the requested method with a dimensions string, 
    automatically switching from GET to POST as needed """
    if params is None: params = {}
    else: params = params.copy()
    kwargs = {'params':params, 'stream':stream, 'compress': compress}
    if len(dimensions) > 1024:
        kwargs['data'] = {'dims':dimensions}
        method = samweb.postURL
    else:
        params['dims'] = dimensions
        method = samweb.getURL
    return method(url, **kwargs)

@samweb_method
def listFiles(samweb, dimensions=None, defname=None, fileinfo=False, stream=False):
    """ list files matching either a dataset definition or a dimensions string
    arguments:
      dimensions: string (default None)
      defname: string definition name (default None)
      fileinfo: boolean; if True, return file_id, file_size, event_count 
      stream: boolean: if True the return value will be a generator and the results will
                       be progressively returned to the client. Note that this keeps the network connection open until
                       all the response has been read. (default False)
    
    returns:
      a generator producing file names (note that the network connection may not be closed
        until you have read the entire list). If fileinfo is true, it will produce
        (file_name, file_id, file_size, event_count) tuples
    """

    # This can return a potentially long list, so don't preload the result
    # instead return a generator which reads it progressively
    params = {'format':'plain'}
    if fileinfo:
        params['fileinfo'] = 1
    if defname is not None:
        result = samweb.getURL('/definitions/name/%s/files/list' % escape_url_component(defname), params=params,stream=True,compress=True)
    else:
        result = samweb._callDimensions('/files/list', dimensions, params, stream=True)
    if fileinfo:
        output = _make_file_info(result.iter_lines())
    else:
        output = filter( None, (convert_to_string(l).strip() for l in result.iter_lines()) )
    if stream: return output
    else: return list(output)

@samweb_method
def listFilesSummary(samweb, dimensions=None, defname=None):
    """ return summary of files matching either a dataset definition or a dimensions string
    arguments:
      dimensions: string (default None)
      defname: string definition name (default None)"""
    if defname is not None:
        result = samweb.getURL('/definitions/name/%s/files/summary' % escape_url_component(defname))
    else:
        result = samweb._callDimensions('/files/summary', dimensions)
    return convert_from_unicode(result.json())

@samweb_method
def expandDims(samweb, dimensions):
    """ Expand the given dimensions """
    result = samweb._callDimensions('/files/expand_query', dimensions)
    return result.json()

@samweb_method
def parseDims(samweb, dimensions, mode=False):
    """ For debugging only """
    if not mode:
        params = { "parse_only": "1"}
        result = samweb._callDimensions('/files/list', dimensions, params)
    else:
        params = { "diagnostics" : "1" }
        if mode=='count':
            path = '/files/count'
        elif mode=='summary':
            path = '/files/summary'
        elif mode=='fileinfo':
            params['fileinfo']="1"
            path = '/files/list'
        else:
            path = '/files/list'
        result = samweb._callDimensions(path, dimensions, params)
    return result.text.rstrip()

@samweb_method
def countFiles(samweb, dimensions=None, defname=None):
    """ return count of files matching either a dataset definition or a dimensions string
    arguments:
      dimensions: string (default None)
      defname: string definition name (default None)"""
    if defname is not None:
        result = samweb.getURL('/definitions/name/%s/files/count' % escape_url_component(defname))
    else:
        result = samweb._callDimensions('/files/count', dimensions, compress=False)
    return int(result.text.strip())

@samweb_method
def listFilesAndLocations(samweb, filter_path=None, dimensions=None, defname=None, checksums=False, schema=None, structured_output=True):
    """ List files and their locations based on location path, and dimensions or defname
    arguments:
        path_filter: string or list of strings (default None)
        dimensions: string (default None)
        defname: string definition name (default None)"""
    params = {}
    if filter_path:
        params['filter_path'] = filter_path
    if dimensions:
        params['dims'] = dimensions
    if defname:
        params['defname'] = defname
    if checksums:
        params['checksums'] = True
    if schema:
        params['schema'] = schema
    result = samweb.postURL('/files/list/locations?format=plain', params, stream=True, compress=True)
    if structured_output:
        def _format_output():
            for l in result.iter_lines():
                l = convert_to_string(l).strip()
                if l:
                    fields = l.split('\t')
                    if checksums:
                        if fields[-1]: fields[-1] = fields[-1].split(';')
                        else: fields[-1] = []
                    yield tuple(fields)
        output = _format_output()
    else:
        output = filter( None, (convert_to_string(l).strip() for l in result.iter_lines()) )
    return output


def _make_file_path(filenameorid):
    try:
        fileid = int(filenameorid)
        path = '/files/id/%d' % fileid
    except ValueError:
        path = '/files/name/%s' % escape_url_component(filenameorid)
    return path

@samweb_method
def locateFile(samweb, filenameorid):
    """ return locations for this file
    arguments:
        name or id of file
    """
    url = _make_file_path(filenameorid) + '/locations'
    result = samweb.getURL(url, compress=False)
    return convert_from_unicode(result.json())

@samweb_method
def locateFiles(samweb, filenameorids):
    """ return the locations of multiple files
    The return value is a dictionary of { file_name_or_id : location } pairs
    """

    file_names = []
    file_ids = []
    for filenameorid in filenameorids:
        try:
            file_ids.append(int(filenameorid))
        except ValueError:
            file_names.append(filenameorid)

    data = [] 
    if file_names: data = [ ('file_name', fn) for fn in file_names ]
    if file_ids: data = [ ('file_id', fid) for fid in file_ids ]
    response = samweb.postURL("/files/locations", data=data, compress=True)
    return convert_from_unicode(response.json())

@samweb_method
def locateFilesIterator(samweb, iterable, chunksize=50):
    """ Given an iterable of file names or ids, return an iterable object with
    (name or id, locations) pairs. This is a convenience wrapper around locateFiles
    arguments:
        iterable: iterable object of file names or ids
        chunksize: the number of files to query on each pass
    """

    for fs in _chunk(iter(iterable), chunksize):
        for j in six.iteritems(samweb.locateFiles(fs)):
            yield j

@samweb_method
def addFileLocation(samweb, filenameorid, location):
    """ Add a location for a file
    arguments:
        name or id of file
        location
    """
    url = _make_file_path(filenameorid) + '/locations'
    data = { "add" : location }
    return samweb.postURL(url, data=data, role='*')

@samweb_method
def removeFileLocation(samweb, filenameorid, location):
    """ Remove a location for a file
    arguments:
        name or id of file
        location
    """
    url = _make_file_path(filenameorid) + '/locations'
    data = { "remove" : location }
    return samweb.postURL(url, data=data, role='*')

@samweb_method
def getFileAccessUrls(samweb, filenameorid, schema, locationfilter=None):
    """ return urls by which this file may be accessed
    arguments:
        name or id of file
        schema
        locationfilter (default None)
    """
    params = { "schema": schema }
    if locationfilter:
        params["location"] = locationfilter
    response = samweb.getURL(_make_file_path(filenameorid) + '/locations/url', params=params)
    return convert_from_unicode(response.json())

@samweb_method
def getMetadata(samweb, filenameorid, locations=False, basic=False):
    """ Return metadata as a dictionary 
    arguments:
        name or id of file
        locations: if True, also return file locations
        basic: if True, only return basic metadata
    """
    params = {}
    if locations: params['locations'] = True
    if basic: params['basic'] = 1
    response = samweb.getURL(_make_file_path(filenameorid) + '/metadata', params=params, compress=True)
    return convert_from_unicode(response.json())

@samweb_method
def getMultipleMetadata(samweb, filenameorids, locations=False, asJSON=False, basic=False):
    """ Return a list of metadata dictionaries
    (This method does not return an error if a
    file does not exist; instead it returns no
    result for that file)
    arguments:
        list of file names or ids
        locations: if True include location information
        asJSON: return the undecoded JSON string instead of python objects
        basic: if true, return basic metadata only
    """
    file_names = []
    file_ids = []
    for filenameorid in filenameorids:
        try:
            file_ids.append(int(filenameorid))
        except ValueError:
            file_names.append(filenameorid)
    
    params = {}
    if file_names: params["file_name"] = file_names
    if file_ids: params["file_id"] = file_ids
    if locations: params["locations"] = 1
    if basic: params['basic'] = 1
    # use post because the lists cab be large
    response = samweb.postURL("/files/metadata", data=params, compress=True)
    if asJSON:
        return response.text.rstrip()
    else:
        return convert_from_unicode(response.json())

@samweb_method
def getMetadataIterator(samweb, iterable, locations=False, basic=False, chunksize=50):
    """ Given an iterable of file names or ids, return an iterable object with
    their metadata. This is a convenience wrapper around getMultipleMetadata
    arguments:
        iterable: iterable object of file names or ids
        locations: if True include location information
        basic: if True, return basic metadata only
        chunksize: the number of files to query on each pass
    """

    for fs in _chunk(iter(iterable), chunksize):
        for md in samweb.getMultipleMetadata(fs, locations=locations, basic=basic):
            yield md

@samweb_method
def getMetadataText(samweb, filenameorid, format=None, locations=False, basic=False):
    """ Return metadata as a string
    arguments:
        name or id of file
        locations: if True include location information
        basic: if True, return basic metadata only
    """
    if format is None: format='plain'
    params = {'format':format}
    if locations: params['locations'] = 1
    if basic: params['basic'] = 1
    result = samweb.getURL(_make_file_path(filenameorid) + '/metadata', params=params, compress=True)
    return result.text.rstrip()

@samweb_method
def getFileLineage(samweb, lineagetype, filenameorid):
    """ Return lineage information for a file
    arguments:
        lineagetype (ie "parents", "children")
        name or id of file
    """
    result = samweb.getURL(_make_file_path(filenameorid) + '/lineage/' + escape_url_component(lineagetype))
    return convert_from_unicode(result.json())

@samweb_method
def validateFileMetadata(samweb, md=None, mdfile=None):
    """ Check the metadata for validity
    arguments:
        md: dictionary containing metadata (default None)
        mdfile: file object containing metadata (must be in json format)
    """
    if md:
        data = json.dumps(md)
    elif mdfile:
        data = mdfile.read()
    else:
        raise ArgumentError('Must specify metadata dictionary or file object')
    return samweb.postURL('/files/validate_metadata', data=data, content_type='application/json').text

@samweb_method
def declareFile(samweb, md=None, mdfile=None):
    """ Declare a new file
    arguments:
        md: dictionary containing metadata (default None)
        mdfile: file object containing metadata (must be in json format)
    """
    if md:
        data = json.dumps(md)
    elif mdfile:
        data = mdfile.read()
    else:
        raise ArgumentError('Must specify metadata dictionary or file object')
    return samweb.postURL('/files', data=data, content_type='application/json').text

@samweb_method
def modifyFileMetadata(samweb, filename, md=None, mdfile=None):
    """ Modify file metadata
    arguments:
        filename
        md: dictionary containing metadata (default None)
        mdfile: file object containing metadata (must be in json format)
    """
    if md:
        data = json.dumps(md)
    else:
        data = mdfile.read()
    url = _make_file_path(filename)
    return samweb.putURL(url + "/metadata", data=data, content_type='application/json', role='*').text

@samweb_method
def modifyMetadata(samweb, md=None, mdfile=None, continue_on_error=False):
    """ Modify metadata for one or more files
The metadata must be in the form of a list of metadata objects containing the file_name and the fields to change
    arguments:
        md: list of dictionaries containing metadata
        mdfile: file object containing metadata (must be in json format)
        continue_on_error: If true an error in a file's metadata will not prevent the other files from being modified
    """
    if md:
        data = json.dumps(md)
    else:
        data = mdfile.read()
    return samweb.putURL('/files', params={'continue_on_error': continue_on_error}, data=data, content_type='application/json', role='*').json()

@samweb_method
def retireFile(samweb, filename):
    """ Retire a file:
    arguments:
        filename
    """
    url = _make_file_path(filename) + '/retired_date'
    return samweb.postURL(url, role='*').text

@samweb_method
def verifyFileChecksum(samweb, path, checksum=None, algorithms=None):
    """ Verify a file checksum for a file
Throws a ChecksumMismatch exception if they don't match
    arguments:
        path: either the filename or the path to the physical file. The path must be given is checksum is None (default None)
        checksum: a list of checksums to verify against. If given the file will not be opened (default None)
        algorithms: a list of preferred algorithms, in order
    """

    import os.path
    filename = os.path.basename(path)

    # get the checksum from the database
    md = samweb.getMetadata(filename, basic=True)
    db_checksum = md.get('checksum')

    if not db_checksum:
        raise MissingChecksum("File has no checksum")

    input_checksums = {}
    for c in db_checksum:
        algo = c.split(':',1)[0]
        input_checksums[algo] = c

    # make a preference list of algorithms
    if algorithms is None:
        algorithms = ['sha512','sha256','sha1','md5','adler32','enstore']
    possible_algorithms = list(set(algorithms).intersection(input_checksums))
    if not possible_algorithms:
        raise MissingChecksum("No compatible checksum algorithm")
    # this isn't a very efficient sort - the key function is O(n) in the number of algorithms, but that is small
    possible_algorithms.sort(key=lambda k: algorithms.index(k))

    # if we are provided with an input checksum, then use that
    # else read the file to calculate one
    if checksum:
        local_checksums = {}
        for local_checksum in checksum:
            algo = local_checksum.split(':',1)[0]
            local_checksums[algo] = local_checksum
        def get_local_checksum(algorithm):
            return local_checksums.get(algorithm)
    else:
        def get_local_checksum(algorithm):
            from . import utility
            try:
                return utility.fileChecksum(path, checksum_types=[algorithm])[0]
            except utility.UnknownHashAlgorithm:
                return None

    # try algorithms until we find one that works
    for algorithm in possible_algorithms:
        localchecksum = get_local_checksum(algorithm)
        if localchecksum is None:
            continue
        if localchecksum != input_checksums[algorithm]:
            raise ChecksumMismatch("Checksum mismatch: expecting %s, got %s" % (input_checksums[algorithm], localchecksum))
        return True
    else:
        raise MissingChecksum("Unable to calculate checksum - no matching algorithms available")
