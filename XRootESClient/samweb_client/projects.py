from __future__ import print_function
from __future__ import absolute_import

import time, os, re
from samweb_client import json, convert_from_unicode, convert_to_string
from samweb_client.client import samweb_method, get_version
from samweb_client.http_client import escape_url_path
from .exceptions import *
from six.moves import range


from six.moves import filter

@samweb_method
def listProjects(samweb, stream=False, **queryCriteria):
    """ List projects matching query parameters
        keyword arguments: passed as parameters to server
    """
    params = dict(queryCriteria)
    params['format'] = 'plain'
    result = samweb.getURL('/projects', params, stream=True, compress=True)
    output = filter( None, (convert_to_string(l).strip() for l in result.iter_lines()) )
    if stream: return output
    else: return list(output)

@samweb_method
def makeProjectName(samweb, description, prefix=None, username=None):
    """ Make a suitable project name from the provided string """

    # remove anything after a colon (defname modifier)
    if ':' in description:
        description = description[:description.index(':')]
    description = description.replace(' ','_')
    import time
    now = time.strftime("%Y%m%d%H%M%S")
    # check for the username, offset by _ or -
    # if it's not there prepend it
    if username is None:
        username = samweb.user
    if username and not re.search(r'(\A|[_-])%s(\Z|[_-])' % username, description) and not (prefix and username in prefix):
        username += '_'
    else:
        username = ''
    if prefix is not None:
        prefix = '%s_' % prefix
    else:
        prefix = ''

    return '%s%s%s_%s' % (prefix, username, description, now)

@samweb_method
def startProject(samweb, project, defname=None, station=None, group=None, user=None, snapshot_id=None):
    """ Start a project on a station. One of defname or snapshotid must be given
    arguments:
        project: project name
        defname: definition name (default None)
        station: station name (defaults to experiment name)
        group: group name (defaults to experiment name)
        user: user name (default is username from certificate)
        snapshot_id: snapshot id (default None)
    """

    if bool(defname) + bool(snapshot_id) != 1:
        raise ArgumentError("Exactly one of definition name or snapshot id must be provided")

    if not station: station = samweb.get_station()
    if not group: group = samweb.group
    args = {'name':project, 'station':station, "group":group}
    if   defname: args["defname"] = defname
    elif snapshot_id: args["snapshot_id"] = snapshot_id
    if user: args["username"] = user
    result = samweb.postURL('/startProject', args, compress=False)
    projecturl = result.text.strip()
    if projecturl.startswith('https'):
        # prefer to use unencrypted project urls
        projecturl = samweb.findProject(project, station)

    # could look up definition name/snapshot id instead
    rval = {'project':project,'projectURL':projecturl}
    if defname: rval["definition_name"] = defname
    elif snapshot_id: rval["snapshot_id"] = snapshot_id
    return rval

@samweb_method
def findProject(samweb, project, station=None):
    args = {'name':project}
    if station: args['station'] = station
    result = samweb.getURL('/findProject', args, compress=False)
    return result.text.strip()

@samweb_method
def startProcess(samweb, projecturl, appfamily, appname, appversion, deliveryLocation=None, node=None,
        user=None, maxFiles=None, description=None, schemas=None, unique_id=None):
    if not node:
        # default for the node is the local hostname
        import socket
        node = socket.getfqdn()

    # if the user isn't given and we aren't using client certs, set it to the default
    if not user and not projecturl.startswith('https:'):
        user = samweb.user

    args = { "appname":appname, "appversion":appversion, "node" : node, }
    if user:
        args["username"] = user
    if appfamily:
        args["appfamily"] = appfamily
    if maxFiles:
        args["filelimit"] = maxFiles
    if deliveryLocation:
        args["deliverylocation"] = deliveryLocation
    if description:
        args["description"] = description
    if schemas:
        args["schemas"] = schemas
    if unique_id:
        args["unique_id"] = unique_id

    result = samweb.postURL(projecturl + '/establishProcess', args, compress=False)
    return result.text.strip()

@samweb_method
def makeProcessUrl(samweb, projecturl, processid):
    """ Make the process url from a project url and process id """
    if not '://' in projecturl:
        projecturl = samweb.findProject(projecturl)
    return projecturl + '/process/' + str(processid)

@samweb_method
def getNextFile(samweb, processurl, timeout=None):
    """ get the next file from the project
    arguments:
        processurl: the process url
        timeout: timeout after not obtaining a file in this many seconds. -1 to disable; 0 to return immediately; default is None (disabled)
    """
    url = processurl + '/getNextFile'
    starttime = time.time()
    while True:
        result= samweb.postURL(url, data={}, compress=False)
        code = result.status_code
        data = result.text.strip()
        if code == 202:
            retry_interval = 10
            retry_after = result.headers.get('Retry-After')
            if retry_after:
                try:
                    retry_interval = int(retry_after)
                except ValueError: pass
            if timeout is not None:
                if timeout == 0:
                    return None
                elif timeout > 0 and time.time() - starttime > timeout:
                    raise Timeout('Timed out after %d seconds' % (time.time() - starttime))
            time.sleep(retry_interval)
        elif code == 204:
            raise NoMoreFiles()
        else:
            if 'application/json' in result.headers['Content-Type']:
                return result.json()
            lines = data.split('\n')
            output = { "url" : lines[0] }
            if len(lines) > 1: output["filename"] = lines[1]
            else:
                output["filename"] = os.path.basename(output["url"])
            return output

# old method
@samweb_method
def releaseFile(samweb, processurl, filename, status="ok"):
    if status == "ok": status = "consumed"
    else: status = "skipped"
    return samweb.setProcessFileStatus(processurl, filename, status)

# new method
@samweb_method
def setProcessFileStatus(samweb, processurl, filename, status="consumed"):
    args = { 'filename' : filename, 'status':status }
    return samweb.postURL(processurl + '/updateFileStatus', args, compress=False).text.rstrip()

@samweb_method
def stopProcess(samweb, processurl):
    """ End an existing process """
    samweb.postURL(processurl + '/endProcess', compress=False)

@samweb_method
def stopProject(samweb, projecturl):
    if not '://' in projecturl:
        projecturl = samweb.findProject(projecturl)
    args = { "force" : 1 }
    return samweb.postURL(projecturl + "/endProject", args, compress=False).text.rstrip()

@samweb_method
def projectSummary(samweb, projecturl):
    if not '://' in projecturl:
        projecturl = '/projects/name/%s' % escape_url_path(projecturl)
    return convert_from_unicode(samweb.getURL(projecturl + "/summary", compress=True).json())

@samweb_method
def projectSummaryText(samweb, projecturl):
    if not '://' in projecturl:
        projecturl = '/projects/name/%s' % escape_url_path(projecturl)
    return samweb.getURL(projecturl + "/summary", params=dict(format='plain'), compress=True).text.rstrip()

@samweb_method
def projectRecoveryDimension(samweb, projectnameorurl, useFileStatus=None, useProcessStatus=None):
    """Get the dimensions to create a recovery dataset
    arguments:
        projectnameorurl : name or url of the project
        useFileStatus : use the status of the last file seen by a process (default unset)
        useProcessStatus : use the status of the process (default unset)
    """
    if not '://' in projectnameorurl:
        projectnameorurl = "/projects/name/%s" % escape_url_path(projectnameorurl)
    params = { "format" : "plain" }
    if useFileStatus is not None: params['useFiles'] = useFileStatus
    if useProcessStatus is not None: params['useProcess'] = useProcessStatus
    return convert_from_unicode(samweb.getURL(projectnameorurl + "/recovery_dimensions", params=params, compress=True).text.rstrip())

@samweb_method
def setProcessStatus(samweb, status, projectnameorurl, processid=None, process_desc=None):
    """ Mark the final status of a process

    Either the processid or the process description must be provided. If the description is
    used it must be unique within the project

    arguments:
        status: completed or bad
        projectnameorurl: project name or url
        processid: process identifier
        process_desc: process description
    """
    if '://' not in projectnameorurl:
        url = '/projects/name/%s' % escape_url_path(projectnameorurl)
    else: url = projectnameorurl
    args = { "status" : status }
    if processid is not None:
        url += '/processes/%s' % processid
    elif process_desc is not None:
        url += '/process_description/%s' % escape_url_path(process_desc)
    else:
        # assume direct process url
        pass

    return samweb.putURL(url + "/status", args, compress=False).text.rstrip()

@samweb_method
def runProject(samweb, projectname=None, defname=None, snapshot_id=None, callback=None,
        deliveryLocation=None, node=None, station=None, maxFiles=0, schemas=None, group=None,
        application=('runproject','runproject',get_version()), nparallel=1, quiet=False, name_prefix=None, user=None ):
    """ Run a project

    arguments (use keyword arguments, all default to None):
        projectname: the name for the project
        defname: the defname to use
        snapshot_id: snapshot_id to use
        callback: a single argument function invoked on each file
        deliveryLocation
        node
        station
        maxFiles
        schemas
        application: a three element sequence of (family, name, version)
        nparallel: number of processes to run in parallel
        quiet: If true, suppress normal output
        name_prefix: prepend to a generated project name (ignored if projectname is set)
        user: Username to run the project as. Defaults to current OS user if not provided.
    """

    if callback is None:
        def _print(fileurl):
            print(fileurl)
            return True
        callback = _print
    if not projectname:
        if defname:
            projectname = samweb.makeProjectName(defname, prefix=name_prefix)
        elif snapshot_id:
            projectname = samweb.makeProjectName('snapshot_id_%d' % snapshot_id, prefix=name_prefix)
    if quiet:
        def write(s): pass
    else:
        import sys
        write=sys.stdout.write

    if not user:
        user = samweb.user

    project = samweb.startProject(projectname, defname=defname, snapshot_id=snapshot_id, station=station, group=group)
    write("Started project %s\n" % projectname)

    projecturl = project['projectURL']
    process_description = ""
    appFamily, appName, appVersion = application

    if nparallel is None or nparallel < 2:
        nparallel=1
    if nparallel > 1:
        import threading
        maxFiles=(maxFiles+nparallel-1)//nparallel


    def runProcess():
        cpid = samweb.startProcess(projecturl, appFamily, appName, appVersion, deliveryLocation, node=node, description=process_description, maxFiles=maxFiles, schemas=schemas, user=user)
        write("Started consumer processs ID %s\n" % (cpid,))
        if nparallel > 1:
            threading.currentThread().setName('CPID-%s' % cpid)
            log_prefix = '%s: ' % threading.currentThread().getName()
        else: log_prefix=''

        processurl = samweb.makeProcessUrl(projecturl, cpid)

        while True:
            try:
                newfile = samweb.getNextFile(processurl)['url']
                try:
                    rval = callback(newfile)
                except Exception as ex:
                    write('%s%s\n' % (log_prefix,ex))
                    rval = 1
            except NoMoreFiles:
                break
            if rval: status = 'ok'
            else: status = 'bad'
            samweb.releaseFile(processurl, newfile, status)

        samweb.setProcessStatus('completed', processurl)

    if nparallel < 2:
        runProcess()
    else:
        threads = []
        for i in range(nparallel):
            t = threading.Thread(target=runProcess, name='Thread-%02d' % (i+1,))
            t.start()
            threads.append(t)

        for t in threads: t.join()

    samweb.stopProject(projecturl)
    write("Stopped project %s\n" % projectname)
    return projectname

@samweb_method
def prestageDataset(samweb, projectname=None, defname=None, snapshot_id=None, maxFiles=0, station=None, deliveryLocation=None, node=None, nparallel=1):
    """ Prestage the given dataset. This is really the same as run-project with names set appropriately.

Prestaging is not complete until the command exits, so it must continue to run until then.
    """

    if nparallel is None or nparallel < 2: nparallel = 1

    def prestage(fileurl):
        if nparallel > 1:
            import threading
            prefix = '%s: ' % threading.currentThread().getName()
        else:
            prefix = ''
        print("%sFile %s is staged" % (prefix, os.path.basename(fileurl)))
        return True

    samweb.runProject(projectname=projectname, defname=defname, snapshot_id=snapshot_id,
            application=('prestage','prestage',get_version()), callback=prestage, maxFiles=maxFiles,
            station=station, deliveryLocation=deliveryLocation, node=node, nparallel=nparallel, name_prefix='prestage')
