from __future__ import print_function

from __future__ import absolute_import
from six.moves.urllib.parse import urlencode, quote, quote_plus
import six.moves.urllib.request, six.moves.urllib.error, six.moves.urllib.parse,six.moves.http_client
from six.moves.urllib.request import urlopen, Request
from six.moves.urllib.error import URLError, HTTPError

import time
import os
import socket
import sys
import optparse
import pwd
import signal

from samweb_client import *
import six

class CmdError(Error): pass

class CmdBase(object):
    cmdgroup = None
    description = None
    hidden = False # if true don't show in list of commands
    args = None
    options = ()

    def __init__(self, samweb):
        self.samweb = samweb

    def addOptions(self, parser):
        pass

class serverInfoCmd(CmdBase):
    name = 'server-info'
    description = 'Display information about the server'
    cmdgroup = 'utility'

    def run(self, options, args):
        print(self.samweb.serverInfo())

def _help_dimensions(samweb):
    """ helper to display the available dimensions """
    maxlen = 1
    dims = samweb.getAvailableDimensions()
    for dim, desc in dims:
        maxlen = max(len(dim), maxlen)
    for dim, desc in dims:
        print('%-*s %s' % (maxlen, dim, desc))

def _file_list_summary_str(summary):
    return "File count:\t%(file_count)s\nTotal size:\t%(total_file_size)s\nEvent count:\t%(total_event_count)s" % summary

def _file_list_str_gen(g, fileinfo):
    if fileinfo:
        for result in g:
            yield '\t'.join(str(e) for e in result)
    else:
        for result in g:
            yield result

class listFilesCmd(CmdBase):
    name = "list-files"
    options = [
                ("fileinfo", "Return additional information for each file"),
                ("summary", "Return a summary of the results instead of the full list"),
                ("expand-query", "Return an expanded and normalized version of the query"),
                ("help-dimensions", "Return information on the available dimensions"),
                ("dump-query", "Return query information for these dimensions instead of evaluating them"),
              ]

    description = "List files by dimensions query"
    cmdgroup = 'datafiles'
    args = "<dimensions query>"

    def run(self, options, args):
        if options.help_dimensions:
            _help_dimensions(self.samweb)
            return
        dims = (' '.join(args)).strip()
        if not dims:
            raise CmdError("No dimensions specified")
        if options.expand_query:
            print(self.samweb.expandDims(dims).get('query',''))
        elif options.dump_query:
            if options.summary: mode = 'summary'
            else: mode = None
            print(self.samweb.parseDims(dims, mode))
        elif options.summary:
            summary = self.samweb.listFilesSummary(dims)
            print(_file_list_summary_str(summary))
        else:
            fileinfo = options.fileinfo
            for l in _file_list_str_gen(self.samweb.listFiles(dims,fileinfo=fileinfo, stream=True), fileinfo):
                print(l)

class countFilesCmd(CmdBase):
    name = "count-files"
    options = [ ("dump-query", "Return parser output for these dimensions instead of evaluating them"), ]
    description = "Count files by dimensions query"
    args = "<dimensions query>"
    cmdgroup = 'datafiles'
    def run(self, options, args):
        dims = (' '.join(args)).strip()
        if not dims:
            raise CmdError("No dimensions specified")
        if options.dump_query:
            print(self.samweb.parseDims(dims, mode='count'))
        else:
            print(self.samweb.countFiles(dims))

class listFileLocationsCmd(CmdBase):
    name = "list-file-locations"
    description = "List locations and files matching a query"
    options = [ ("dimensions=", "Dimension query"), ("defname=", "Definition name"), ("filter-path=", "Comma separated list of path prefixes to filter on"),
                ("checksums", "Include checksums in output"), ("schema=", "File access url schema")]
    cmdgroup = 'datafiles'
    def run(self, options, args):
        if options.filter_path:
            filter_path = options.filter_path.split(',')
        else:
            filter_path = None
        if options.schema:
            schema = options.schema.split(',')
        else:
            schema = None
        output = self.samweb.listFilesAndLocations(dimensions=options.dimensions, defname=options.defname, filter_path=filter_path, checksums=options.checksums, schema=schema, structured_output=False)
        for l in output:
            print(l)

class locateFileCmd(CmdBase):
    name = "locate-file"
    description = "List locations for a file"
    args = "<file name>"
    cmdgroup = 'datafiles'
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("No filename specified")
        filename = args[0]
        for loc in ( l.get('location') or l['full_path'] for l in self.samweb.locateFile(filename)):
            print(loc)

class getFileAccessUrlCmd(CmdBase):
    name = "get-file-access-url"
    description = ("Get urls by which files can be accessed.\n\nNote that using this command does no data movement or prestaging and is not recommended for large scale data access.")
    args = "<file name>"
    cmdgroup = 'datafiles'

    def addOptions(self, parser):
        parser.add_option("--schema", action="store", dest="schema", default="gsiftp",
                help="Access schema for file")
        parser.add_option("--location", action="store", dest="location",
                help="Filter returned urls by location prefix")

    def run(self, options, args):
        if len(args) != 1:
           raise CmdError("No filename specified")
        filename = args[0]
        urls = self.samweb.getFileAccessUrls(filename, schema=options.schema, locationfilter=options.location)
        for url in urls:
            print(url)

class addFileLocationCmd(CmdBase):
    name='add-file-location'
    description="add a location for a file"
    args = "<file_name> <location>"
    cmdgroup = 'datafiles'
    def run(self, options, args):
        if len(args) != 2:
            raise CmdError("Incorrect arguments")
        self.samweb.addFileLocation(args[0],args[1])

class removeFileLocationCmd(CmdBase):
    name='remove-file-location'
    description="remove a location for a file"
    args = "<file_name> <location>"
    cmdgroup = 'datafiles'
    def run(self, options, args):
        if len(args) != 2:
            raise CmdError("Incorrect arguments")
        self.samweb.removeFileLocation(args[0],args[1])

class getMetadataCmd(CmdBase):
    name = 'get-metadata'
    description = "Get metadata for a file"
    args = "<file name>"
    options = [ ("locations", "Include locations in output (requires --json)"), ("basic", "Basic metadata only")]
    cmdgroup = 'datafiles'

    def addOptions(self, parser):
        parser.add_option("--json", action="store_const", const="json", dest="format", help="Return output in JSON format" )

    def run(self, options, args):
        if len(args) < 1:
            raise CmdError("No argument specified")
        elif len(args) == 1:
            print(self.samweb.getMetadataText(args[0],format=options.format, locations=options.locations, basic=options.basic))
        else:
            if options.format != 'json': raise CmdError("Multiple metadata requires --json format")
            print(self.samweb.getMultipleMetadata(args, locations=options.locations, basic=options.basic, asJSON=True))

class fileLineage(CmdBase):
    name = 'file-lineage'
    description = 'Get lineage for a file'
    args = '<parents|children|ancestors|descendants|rawancestors> <file name>'
    options = [ ('showretired', 'Show retired files') ]
    cmdgroup = 'datafiles'

    def run(self, options, args):
        if len(args) != 2:
            raise CmdError("Invalid or no argument specified")
        def _printLineage(results, indent=0):
            for r in results:
                if r.get('retired'):
                    if options.showretired:
                        print('%s(Retired file %s - %d)' % (' '*indent, r['file_name'], r['file_id']))
                    else: continue # skip over retired files and their lineage completely
                else:
                    print('%s%s' % (' '*indent, r['file_name']))
                for k in ('children','parents'):
                    if k in r:
                        _printLineage(r[k], indent+4)
                        break
        _printLineage(self.samweb.getFileLineage(args[0], args[1]))

class calculateChecksumCmd(CmdBase):
    name = 'file-checksum'
    description = ('Calculate a checksum for a file using the enstore algorithm (sometimes inaccurately described as a "CRC"). '
            'This command reads the file from a path on the local system and so the file must be available on a local or shared filesystem. '
            'If a single argument of \'-\' is provided then the command will read from standard input.'
            'The "enstore", "adler32", "md5", and "sha1" types are always supported. Other hash types may be available depending on the '
            'combination of python and openssl version. '
            )
    args = "<path to file> [<path to file> [...]]"
    options = [ ('old', 'Return output in old metadata format'),
            ('type=', 'Comma separated list of checksum types'),
            ('list-types', 'List available algorithms. (Depending on the underlying version of python this list may not be exhaustive).')
            ]
    cmdgroup = 'utility'

    def addOptions(self, parser):
        parser.add_option("--type", action="append", dest="type", help="Comma separated list of checksum types" )

    def run(self, options, args):
        if options.list_types:
            from samweb_client.utility import list_checksum_algorithms
            for a in list_checksum_algorithms():
                print(a)
            return
        if not args:
            raise CmdError("No file paths provided")

        if options.type:
            algorithms = []
            for t in options.type:
                algorithms.extend(t.split(','))
        else:
            algorithms=None
        from samweb_client.utility import fileChecksum, calculateChecksum
        if len(args) == 1:
            if args[0] == '-':
                print(json.dumps(calculateChecksum(sys.stdin, checksum_types=algorithms, oldformat=options.old)))
            else:
                print(json.dumps(fileChecksum(args[0], checksum_types=algorithms, oldformat=options.old)))
        else:
            for a in args:
                try:
                    print("%s: %s" % (a, json.dumps(fileChecksum(a, checksum_types=algorithms, oldformat=options.old))))
                except Error as ex:
                    print("%s: %s" % (a, ex))

class verifyFileChecksumCmd(CmdBase):
    name = 'verify-file-checksum'
    description = ( 'Verify a checksum for a file against the database' )
    args = "<path to file> [<path to file> [...]]"
    options = [ ('checksum=', 'A comma separated list of checksums for a file to be verified. If this is provided only one filename can be given.') ]
    cmdgroup = 'datafiles'

    def run(self, options, args):
        success = True
        if options.checksum:
            if len(args) != 1:
                raise CmdError("If checksum option is given then exactly one filename must be provided")
            checksums = options.checksum.split(',')
        else:
            checksums = None

        for a in args:
            try:
                self.samweb.verifyFileChecksum(a, checksum=checksums)
                print('%s: Valid' % a)
            except ChecksumMismatch as ex:
                print('%s: %s' % (a, ex))
                success = False
        if not success: return 1

class validateFileMetadata(CmdBase):
    name = 'validate-metadata'
    description = "Check file metadata for correctness"
    args = "<name of metadata file (json format)>"
    cmdgroup = 'datafiles'

    def run(self, options, args):
        if not args:
            raise CmdError("No metadata files provided")
        rval = 0
        for name in args:
            if len(args) > 1:
                sys.stdout.write("%s: " % name)
            try:
                try:
                    f = open(name)
                except IOError as ex:
                    raise CmdError("Failed to open file: %s" % (str(ex)))
                try:
                    self.samweb.validateFileMetadata(mdfile=f)
                finally:
                    f.close()
            except Error as ex:
                sys.stdout.write("%s\n" % ex)
                rval = 1
            else:
                sys.stdout.write("Metadata is valid\n")
        return rval

class declareFileCmd(CmdBase):
    name = 'declare-file'
    description = "Declare new files into the database. The metadata file should be either a single JSON object with metadata or a list of them."
    args = "<name of metadata file (json format)>"
    cmdgroup = 'datafiles'

    def run(self, options, args):
        if not args:
            raise CmdError("No metadata files provided")
        for name in args:
            try:
                f = open(name)
            except IOError as ex:
                raise CmdError("Failed to open file: %s: %s" % (name, str(ex)))
            try:
                self.samweb.declareFile(mdfile=f)
            finally:
                f.close()

class modifyMetadataCmd(CmdBase):
    name = 'modify-metadata'
    description = """Modify metadata for an existing file. If no file name is given as an argument the metadata file must contain a list of JSON objects containing the file name and fields to modify
If the file name is given the metadata file must be a single JSON object containing the fields to modify"""
    args = "[<file name>] <name of file containing metadata parameters to modify (json format)>"
    cmdgroup = 'datafiles'

    def run(self, options, args):
        if len(args) == 1:
            mdfile = args[0]
            file_name = None
        elif len(args) == 2:
            file_name = args[0]
            mdfile = args[1]
        else:
            raise CmdError("Invalid arguments provided")
        try:
            f = open(mdfile)
        except IOError as ex:
            raise CmdError("Failed to open file: %s: %s" % (mdfile, str(ex)))
        try:
            if file_name:
                self.samweb.modifyFileMetadata(file_name, mdfile=f)
                print("Metadata has been updated for file '%s'" % file_name)
            else:
                self.samweb.modifyMetadata(mdfile=f)
                print("Metadata has been updated")
        finally:
            f.close()

class retireFileCmd(CmdBase):
    name = 'retire-file'
    description = "Mark a file as retired"
    args = "<file name> [file name] ..."
    cmdgroup = 'datafiles'
    def run(self, options, args):
        if not args:
            raise CmdError("No file names provided")
        for filename in args:
            rval = 0
            try:
                self.samweb.retireFile(filename)
            except Error as ex:
                rval = 1
                print(ex)
            else:
                print("%s has been retired" % filename)
        return rval

class listDefinitionsCmd(CmdBase):
    name = "list-definitions"
    options = [ "defname=", "user=", "group=", "after=", "before=" ]
    description = "List existing dataset definitions"
    cmdgroup = 'definitions'

    def run(self, options, args):
        if len(args) > 0:
            raise CmdError("Invalid arguments")
        args = {}
        if options.defname:
            args['defname'] = options.defname
        if options.user:
            args['user'] = options.user
        if options.group:
            args['group'] = options.group
        if options.after:
            args['after'] = options.after
        if options.before:
            args['before'] = options.before
        for l in self.samweb.listDefinitions(stream=True, **args):
            print(l)

class descDefinitionCmd(CmdBase):
    name = "describe-definition"
    description = "Describe an existing dataset definition"
    args = "<dataset definition>"
    cmdgroup = 'definitions'
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        print(self.samweb.descDefinition(args[0]))

class listDefinitionFilesCmd(CmdBase):
    name = "list-definition-files"
    description = "List files in a dataset definition"
    args = "<dataset definition>"
    options = [ ("fileinfo", "Return additional information for each file"),
                ("expand-query", "Return an expanded and normalized version of the query"),
                ("summary", "Return a summary of the results instead of the full list"),
                ]
    cmdgroup = 'definitions'
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        if options.expand_query:
            print(self.samweb.expandDims(defname=args[0]).get('query',''))
        elif options.summary:
            print(_file_list_summary_str(self.samweb.listFilesSummary(defname=args[0])))
        else:
            fileinfo = options.fileinfo
            for l in _file_list_str_gen(self.samweb.listFiles(defname=args[0],fileinfo=fileinfo), fileinfo):
                print(l)

class countDefinitionFilesCmd(CmdBase):
    name = "count-definition-files"
    description = "Count number of files in a dataset definition"
    args = "<dataset definition>"
    cmdgroup = 'definitions'
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        print(self.samweb.countFiles(defname=args[0]))

class createDefinitionCmd(CmdBase):
    name = "create-definition"
    description = "Create a new dataset definition"
    args = "<new definition name> <dimensions>"
    options = [ "user=", "group=", "description=", ("help-dimensions", "Return information on the available dimensions"),]
    cmdgroup = 'definitions'

    def run(self, options, args):
        if options.help_dimensions:
            _help_dimensions(self.samweb)
            return
        try:
            defname = args[0]
        except IndexError:
            raise CmdError("No definition name specified")
        dims = ' '.join(args[1:])
        if not dims:
            raise CmdError("No dimensions specified")
        result = self.samweb.createDefinition(defname, dims, options.user, options.group, options.description)
        print("Dataset definition '%s' has been created with id %s" % (result["defname"], result["defid"]))

class modifyDefinitionCmd(CmdBase):
    name = "modify-definition"
    description = "Modify an existing dataset definition"
    args = "<existing dataset definition>"
    options = [ "defname=", "description=" ]
    cmdgroup = 'definitions'
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Invalid arguments")
        self.samweb.modifyDefinition(args[0], defname=options.defname, description=options.description )

class deleteDefinitionCmd(CmdBase):
    name = "delete-definition"
    description = "Delete an existing dataset definition"
    args = "<dataset definition>"
    cmdgroup = 'definitions'
    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        print(self.samweb.deleteDefinition(args[0]))

class takeSnapshotCmd(CmdBase):
    name = "take-snapshot"
    description = "Take a snapshot of an existing dataset definition"
    args = "<dataset definition>"
    options = ["group=", ("max-age=int", "Only make the snapshot if the existing snapshot has been evaluated more recently than this (in seconds)"),
            ("no-empty", "Refuse to create an empty snapshot" )]
    cmdgroup = 'definitions'

    def run(self, options, args):
        if len(args) != 1:
            raise CmdError("Argument should be exactly one definition name")
        kwargs = {"group" : options.group}
        if options.max_age is not None:
            kwargs['max_age'] = options.max_age
        if options.no_empty:
            kwargs['allow_empty'] = False
        snap_id = self.samweb.takeSnapshot(args[0], **kwargs)
        print(snap_id)

class snapshotInfoCmd(CmdBase):
    name = 'snapshot-info'
    description = 'Get information on a snapshot id or the current snapshot for a definition'
    options = ["defname=", "snapshot-id=int"]
    cmdgroup = 'definitions'

    def run(self, options, args):
        if options.defname is None and options.snapshot_id is None:
            raise CmdError("Must specify only one of defname and snapshot_id")
        args = { 'as_text' : True }
        if options.snapshot_id is not None: args['snapshot_id'] = options.snapshot_id
        else: args['defname'] = options.defname
        print(self.samweb.snapshotInfo(**args))

class listProjectCmd(CmdBase):
    name = "list-projects"
    description = "List projects by various query parameters"
    options = ['name=','user=','group=','defname=','snapshot-id=int', 'started-before=',
            'started-after=', 'ended-before=', 'ended-after=', 'state=', 'station=']
    cmdgroup = 'projects'

    def run(self, options, args):
        if len(args) > 0:
            raise CmdError("Invalid arguments")
        queryargs = {}
        for o in self.options:
            paramname = o[:o.find('=')].replace('-','_')
            value = getattr(options, paramname)
            if value: queryargs[paramname] = value

        for l in self.samweb.listProjects(stream=True, **queryargs):
            print(l)

class startProjectCmd(CmdBase):
    name = "start-project"
    description = "Start a new project"
    options = [ "defname=", "snapshot-id=int", "group=", "station=", ("prefix=", "Prefix for a generated project name") ]
    args = "[project name]"
    cmdgroup = 'projects'

    def run(self, options, args):
        defname = options.defname
        snapshot_id = options.snapshot_id
        try:
            project = args[0]
        except IndexError:
            if defname or snapshot_id:
                project = self.samweb.makeProjectName(defname or ('snapshot_id_%s' % snapshot_id), prefix=options.prefix)
            else:
                raise CmdError("One of definition name or snapshot id must be provided")
        rval = self.samweb.startProject(project, defname=defname, snapshot_id=snapshot_id, station=options.station, group=options.group)
        print(rval["projectURL"])

class ProjectCmdBase(CmdBase):

    options = [ "station=" ]
    cmdgroup = 'projects'

    def _getProjectUrl(self, options, args):
        try:
            projecturl = args.pop(0)
        except IndexError:
            raise CmdError("Must specify project name or url")

        if not '://' in projecturl:
            projecturl = self.samweb.findProject(projecturl, options.station)
        return projecturl

    def _getProjectNameOrUrl(self, options, args):
        try:
            projecturl = args.pop(0)
        except IndexError:
            raise CmdError("Must specify project name or url")
        return projecturl

class findProjectCmd(ProjectCmdBase):
    name = "find-project"
    description = "Return the URL for a running project"
    args = "<project name>"

    def run(self, options, args):
        rval = self._getProjectUrl(options, args)
        print(rval)

class stopProjectCmd(ProjectCmdBase):
    name = "stop-project"
    description = "Stop a running project"
    args = "<project name>"

    def run(self, options, args):

        projecturl = self._getProjectUrl(options, args)
        self.samweb.stopProject(projecturl)

class projectSummaryCmd(ProjectCmdBase):
    name = "project-summary"
    description = "Display the summary information for a project"
    args = "<project name>"

    def run(self, options, args):
        projecturl = self._getProjectNameOrUrl(options, args)
        print(self.samweb.projectSummaryText(projecturl))

class projectRecoveryDimensionCmd(ProjectCmdBase):
    name = "project-recovery"
    description = "Display the dimensions for the recovery dataset for a project"
    args = "<project name>"
    options = [ ("useFileStatus=int", "Use the status of the last file in a process"),
                ( "useProcessStatus=int", "Use the process status") ]

    def run(self, options, args):
        projecturl = self._getProjectNameOrUrl(options, args)
        result = self.samweb.projectRecoveryDimension(projecturl, useFileStatus=options.useFileStatus, useProcessStatus = options.useProcessStatus)
        if result: print(result)

class startProcessCmd(CmdBase):
    name = "start-process"
    description = "Start a consumer process within a project"
    options = [ "appfamily=", "appname=", "appversion=",
            ("url", "Return the entire process url rather than just the process id"),
            ("user=", "Username of the project runner. Default is the OS username. May need to be set if the project owner is different from the account running the process"),
            ("node=", "The current node name. The default is the local hostname, which is appropriate for most situations"),
            ("delivery-location=", "Location to which the files should be delivered (defaults to the same as the node option)"),
            ("max-files=int", "Limit the maximum number of files to give to the process"),
            ("description=", "Text description of the process"),
            ("schemas=", "Comma separated list of url schemas this process prefers to receive"),
            ("unique-id=", "A unique identifier for this process, so the server can tell if it has seen it before. Optional, but a batch job id is a good choice if available"),
            ("batch-job-id=", "Synonym for --unique-id"),
            ]
    args = "<project name or url>"
    cmdgroup = 'projects'

    def run(self, options, args):
        if not options.appname or not options.appversion:
            raise CmdError("Application name and version must be specified")
        try:
            projecturl = args[0]
        except IndexError:
            raise CmdError("Must specify project url")
        if not '://' in projecturl:
            projecturl = self.samweb.findProject(projecturl)

        kwargs={}
        if options.user:
            kwargs["user"] = options.user
        if options.node:
            kwargs["node"]= options.node
        if options.delivery_location:
            kwargs["deliveryLocation"]= options.delivery_location
        if options.max_files:
            kwargs['maxFiles'] = options.max_files
        if options.description:
            kwargs['description'] = options.description
        if options.schemas:
            kwargs['schemas'] = options.schemas
        if options.unique_id:
            kwargs["unique_id"] = options.unique_id
        elif options.batch_job_id:
            kwargs["unique_id"] = options.batch_job_id

        rval = self.samweb.startProcess(projecturl, options.appfamily, options.appname,
                options.appversion, **kwargs)
        if options.url:
            print(self.samweb.makeProcessUrl(projecturl, rval))
        else:
            print(rval)

class ProcessCmd(CmdBase):
    args = "(<process url> | <project url> <process id>)"
    cmdgroup = 'projects'

    def makeProcessUrl(self, args):
        # note that this modifies args
        try:
            url = args.pop(0)
            if args:
                processid = args.pop(0)
                url = self.samweb.makeProcessUrl(url, processid)
        except IndexError: url = None
        if not url:
            raise CmdError("Must specify either process url or project url and process id")
        return url

class getNextFileCmd(ProcessCmd):
    name = "get-next-file"
    description = "Get the next file from a process"
    options = [ ("timeout=int", "Timeout in seconds waiting for file. -1 to disable it; 0 to return immediately if no file. The default is 1 hour")  ]

    def run(self, options, args):
        processurl = self.makeProcessUrl(args)
        kwargs = {}
        if options.timeout is not None:
            if options.timeout < 0: kwargs['timeout'] = None
            else: kwargs['timeout'] = options.timeout
        else:
            kwargs['timeout'] = 3600
        try:
            rval = self.samweb.getNextFile(processurl, **kwargs)
            if not rval:
                return 100
            if not rval["url"].endswith(rval["filename"]):
                print("%s\t%s" % (rval["url"], rval["filename"]))
            else:
                print(rval["url"])
        except NoMoreFiles:
            return 0

class releaseFileCmd(ProcessCmd):
    name = "release-file"
    description = "Release a file from a process. If status is 'ok' his is the same as update-file-status with status = 'consumed', "\
            "else it is the same as update-file-status with status = 'skipped'"
    args = "(<process url> | <project url> <process id>) <file name>"
    options = [ ("status=", "The status to set; the default is 'ok'") ]

    def run(self, options, args):
        try:
            filename = args.pop()
        except IndexError:
            raise CmdError("No project and file name arguments")
        processurl = self.makeProcessUrl(args)
        status = options.status
        if not status: status = 'ok'
        self.samweb.releaseFile(processurl, filename, status)

class setProcessFileStatusCmd(ProcessCmd):
    name = "set-process-file-status"
    description = "Update the status of a file in a process"
    args = "(<process url> | <project url> <process id>) <file name> <status>"

    def run(self, options, args):
        try:
            status = args.pop()
        except IndexError:
            raise CmdError("No status argument")
        try:
            filename = args.pop()
        except IndexError:
            raise CmdError("No project and file name arguments")
        processurl = self.makeProcessUrl(args)
        self.samweb.setProcessFileStatus(processurl, filename, status)

class stopProcessCmd(ProcessCmd):
    name = 'stop-process'
    description = "End an existing process"
    args = "(<process url> | <project url> <process id>)"

    def run(self, options, args):
        processurl = self.makeProcessUrl(args)
        self.samweb.stopProcess(processurl)

class setProcessStatusCmd(CmdBase):
    name = 'set-process-status'
    description = "Set the process status"
    args = "(<process url> | <project name or url> [process id]) <status>"
    options = ['description=']
    cmdgroup='projects'

    def run(self, options, args):
        process_description = options.description
        processid = None
        try:
            nameorurl = args.pop(0)
            if process_description is None:
                if len(args) == 2:
                    processid = args.pop(0)
            status = args[0]

        except IndexError:
            raise CmdError("Invalid arguments")
        self.samweb.setProcessStatus(status, nameorurl, processid=processid, process_desc=process_description)

class runProjectCmd(CmdBase):
    name = 'run-project'
    description = """Run a project"""
    cmdgroup = 'projects'
    options = ['defname=','snapshot-id=int','max-files=int', 'station=',
            ("name=", "Project name"),
            ("schemas=", "Comma separated list of url schemas this process prefers to receive"),
            ("parallel=int", "Number of parallel processes to run"),
            ("delivery-location=", "Location to which the files should be delivered (defaults to the same as the node option)"),
            ("node=", "The current node name. The default is the local hostname, which is appropriate for most situations"),
            "quiet",
            ("group=", "The group that the project will be run under on the SAMWeb Server"),
            ("prefix=", "Prefix for a generated project name"),
            ("user=", "User to run the project as. Defaults to current OS user.")
            ]
    args = '<command to run (%fileurl will be replaced by file url)>'
    def run(self, options, args):
        if options.max_files:
            max_files = options.max_files
        else: max_files=0
        callback = None
        if args:
            cmd = ' '.join(args)
            def callback(fileurl):
                import subprocess
                realcmd = cmd.replace('%fileurl', fileurl)
                try:
                    rval = subprocess.call(realcmd,shell=True)
                    return (rval==0)
                except Exception as ex:
                    print(ex)
                    return False

        self.samweb.runProject(projectname=options.name, defname=options.defname, snapshot_id=options.snapshot_id, maxFiles=max_files,
                callback=callback, schemas=options.schemas, station=options.station,
                deliveryLocation=options.delivery_location, node=options.node, nparallel=options.parallel, quiet=options.quiet, group=options.group,
                name_prefix=options.prefix, user=options.user)

class prestageDatasetCmd(CmdBase):
    name = 'prestage-dataset'
    description = """Prestage a dataset"""
    cmdgroup = 'projects'
    options = ['defname=','snapshot-id=int','max-files=int', 'station=',
            ("name=", "Project name"),
            ("parallel=int", "Number of parallel processes to run"),
            ("delivery-location=", "Location to which the files should be delivered (defaults to the same as the node option)"),
            ("node=", "The current node name. The default is the local hostname, which is appropriate for most situations"),
            ]

    def run(self, options, args):
        if options.max_files:
            max_files = options.max_files
        else: max_files=0
        self.samweb.prestageDataset(projectname=options.name, defname=options.defname,snapshot_id=options.snapshot_id,maxFiles=max_files,
                station=options.station, deliveryLocation=options.delivery_location,node=options.node, nparallel=options.parallel)

class listParametersCmd(CmdBase):
    name = 'list-parameters'
    description = """With no arguments, list the defined parameters.
If a single argument is provided, list all the values for that parameter name."""
    cmdgroup = 'admin'
    args = "[category.name]"

    def run(self, options, args):

        if not args:
            for p in self.samweb.listParameters():
                if isinstance(p, six.string_types): line = p
                elif isinstance(p, dict):
                    p_converted = {}
                    p_converted['name'] = p['name']
                    p_converted['data_type'] = p['data_type']
                    line = "%(name)s (%(data_type)s)" % p_converted
                else: line = str(p)
                print(line)
        elif len(args) == 1:
            for v in self.samweb.listParameterValues(args[0]):
                print(v)
        else:
            raise CmdError("Command takes either zero or one arguments")

class addParameterCmd(CmdBase):
    name = 'add-parameter'
    description = "Add new parameter"
    cmdgroup = 'admin'
    args = "<category.name> <data type>"
    def run(self, options, args):
        try:
            name, data_type = args
        except ValueError:
            raise CmdError("Invalid arguments")

        self.samweb.addParameter(name, data_type)

class listDataDisks(CmdBase):
    name = 'list-data-disks'
    description = "List defined data disks"
    cmdgroup = 'admin'

    def run(self, options, args):
        for d in self.samweb.listDataDisks():
            print(d["mount_point"])

class addDataDiskCmd(CmdBase):
    name = 'add-data-disk'
    description = "Add a new data disk"
    cmdgroup = 'admin'
    args = "<mount point>"

    def run(self, options, args):
        try:
            mount_point, = args
        except ValueError:
            raise CmdError("Invalid arguments")

        self.samweb.addDataDisk(mount_point)

class _DBValuesCmdBase(CmdBase):
    cmdgroup = 'admin'
    args = "<see --help-categories>"
    options = [('help-categories', 'list the database categories that can be used')]
    def _listCategories(self, include_args=False):
        print('Available database categories:')
        print()
        names = self.samweb.getAvailableValues()
        for name, details in six.iteritems(names):
            if include_args:
                print(name, details['args'])
            else:
                print(name)
            print('    %s' % (details['description'], ))

class listValuesCmd(_DBValuesCmdBase):

    name = 'list-values'
    description = "List values from the database"
    def run(self, options, args):
        if options.help_categories:
            return self._listCategories()
        if len(args) != 1:
            raise CmdError("Invalid arguments")

        vtype = args[0]
        for i in self.samweb.listValues(vtype):
            if isinstance(i, six.string_types): line = i
            elif isinstance(i, dict):
                # if the dict contains a key which is a prefix of the vtype
                # then print that value (ie for data_tiers print the value of the data_tier key)
                # else join the values with tabs
                for k in i:
                    if vtype.startswith(str(k)):
                        line = i[k]
                        break
                else:
                    line = '\t'.join(str(i[k]) for k in sorted(six.iterkeys(i)))
            else: line = '\t'.join(str(v) for v in i)
            print(line)

class addValueCmd(_DBValuesCmdBase):
    name = 'add-value'
    description = "Add value to the database"
    def run(self, options, args):
        if options.help_categories:
            return self._listCategories(include_args=True)
        if not args:
            raise CmdError("Invalid arguments")
        vtype = args.pop(0)
        self.samweb.addValue(vtype, *args)
        print("Added value to %s" % vtype)

class listApplicationsCmd(CmdBase):
    name = 'list-applications'
    description = "List defined applications"
    options = [ "family=", "name=", "version=" ]
    cmdgroup = 'admin'
    def run(self, options, args):
        queryparams = {}
        if options.family: queryparams["family"] = options.family
        if options.name: queryparams["name"] = options.name
        if options.version: queryparams["version"] = options.version
        for app in self.samweb.listApplications(**queryparams):
            print("%(family)s\t%(name)s\t%(version)s" % app)

class addApplicationCmd(CmdBase):
    name = 'add-application'
    description = "Add a new application to the database"
    args = "<family> <name> <version>"
    cmdgroup = 'admin'
    def run(self, options, args):

        try:
            family, name, version = args
        except ValueError:
            raise CmdError("Invalid arguments: must specify family, name, and version")
        self.samweb.addApplication(family, name, version)

class listUsersCmd(CmdBase):
    name = 'list-users'
    description = "List registered users"
    cmdgroup = 'user'
    def run(self, options, args):
        for user in self.samweb.listUsers():
            print(user)

class listUsersByGroup(CmdBase):
    name = 'list-users-by-group'
    description = 'List users of the specified role'
    args = "<group name>"
    cmdgroup = 'user'
    def run(self, options, args):
        try:
            group, = args
        except ValueError:
            raise CmdError("Invalid argument: must specify group name")
        for user in self.samweb.listUsersByGroup(group):
            print(user)

class describeUserCmd(CmdBase):
    name = 'describe-user'
    description = 'List user information'
    cmdgroup = 'user'
    args = "<username>"
    def run(self, options, args):
        try:
            username, = args
        except ValueError:
            raise CmdError("Invalid argument: must specify username")

        print(self.samweb.describeUserText(username))

class addUserCmd(CmdBase):
    name = 'add-user'
    description = "Add new user"
    cmdgroup = 'user'
    options = ( 'first-name=', 'last-name=', 'email=', 'uid=', 'groups=' )
    args = "<username>"
    def run(self, options, args):
        try:
            username, = args
        except ValueError:
            raise CmdError("Invalid argument: must specify username")

        if options.groups:
            groups = options.groups.split(',')
        else:
            groups = None

        self.samweb.addUser(username, firstname=options.first_name, lastname=options.last_name, email=options.email, uid=options.uid, groups=groups)

class modifyUserCmd(CmdBase):
    name = 'modify-user'
    description = "Modify user"
    cmdgroup = 'user'
    options = ( 'email=',
            ('groups=', "Set the user's groups to this comma separated list"),
            ('addgroups=', "Add the comma separated list of groups to the user"),
            ('removegroups=', "Remove the comma separated list of groups from the user"),
            'status=',
            ( "addgridsubject=", "A grid subject to add to the user"),
            ( "removegridsubject=", "A grid subject to remove from the user"),
            )
    args = "<username>"
    def run(self, options, args):
        try:
            username, = args
        except ValueError:
            raise CmdError("Invalid argument: must specify username")

        args = {}
        if options.email: args['email'] = options.email
        if options.status: args['status'] = options.status
        if options.groups:
            args['groups'] = options.groups.split(',')
        if options.addgroups:
            args['addgroups'] = options.addgroups.split(',')
        if options.removegroups:
            args['removegroups'] = options.removegroups.split(',')
        if options.addgridsubject:
            args["addgridsubject"] = options.addgridsubject
        if options.removegridsubject:
            args["removegridsubject"] = options.removegridsubject
        self.samweb.modifyUser(username, **args)
        if not args:
            raise CmdError("No actions given to modify user")
        print("User '%s' has been updated" % username)


commands = {
       }
command_groups = {}

class listCommandsCmd(CmdBase):
    name = 'list-cli-commands'
    description = 'list available commands'
    hidden = True
    def run(self, options, args):
        for cmd in (c for c, co in sorted(commands.items()) if not co.hidden):
            print(cmd)

group_descriptions = {
        # The admin group should go last, as normal users don't care
        # name : ( display text, sort order )
        "datafiles": ("Data file commands", 1),
        "definitions" : ("Definition commands", 2),
        "projects" : ("Project commands", 3),
        "admin": ("Admin commands", 100),
        "user": ("User account commands", 90),
        "utility": ("Utility commands", 80),
        }

# add all commands that define a name attribute to the list
for o in list(six.itervalues(locals())): #locals().values():
    try:
        if issubclass(o, CmdBase) and hasattr(o, 'name') and o.name not in commands:
            commands[o.name] = o
            command_groups.setdefault(o.cmdgroup,[]).append(o)
    except TypeError: pass

def command_list():
    import operator
    s = ["Available commands:",]
    def sort_groups(g):
        try:
            return group_descriptions[g][1]
        except KeyError:
            return 1000
    for g in sorted(command_groups, key=sort_groups):
        if g is None: group_desc = 'Uncategorized'
        else: group_desc = group_descriptions.get(g, (g,) )[0]
        # only display groups where the contents aren't all hidden
        gs = ["  %s:" % group_desc]
        for c in ( c for c in sorted(command_groups[g], key=operator.attrgetter('name')) if not c.hidden):
            gs.append("    %s" % c.name)
        if len(gs) > 1:
            s.extend(gs)
            s.append('')
    return '\n'.join(s)

def _list_commands(option, opt, value, parser):
    print(command_list())
    parser.exit()

class SAMWebOptionParser(optparse.OptionParser):
    """ There was some inconsistency with '-' and '_' in option names
        So handle both """
    def _match_long_opt(self, opt):
        try:
            return optparse.OptionParser._match_long_opt(self, opt)
        except optparse.BadOptionError:
            if '_' not in opt:
                raise
        opt = opt.replace('_', '-')
        return optparse.OptionParser._match_long_opt(self, opt)


def main(args=None):

    # Allow passing arguments for testing purposes
    if args is None:
        args = sys.argv[1:]

    usage = "%prog [base options] <command> [command options] ..."
    parser = SAMWebOptionParser(usage=usage, version="%prog " + get_version())
    parser.disable_interspersed_args()
    parser.add_option('--help-commands', action="callback", callback=_list_commands, help="list available commands")
    base_options = optparse.OptionGroup(parser, "Base options")
    base_options.add_option('-e','--experiment',dest='experiment', help='use this experiment server. If not set, defaults to $SAM_EXPERIMENT if set, else $EXPERIMENT.')
    base_options.add_option('--dev', action="store_true", dest='devel', default=False, help='use development server')
    base_options.add_option('-s','--secure', action="store_true", dest='secure', default=False, help='Ignored for backwards compatibility')
    base_options.add_option('--cert', dest='cert', help='x509 certificate for authentication. If not specified, use $X509_USER_PROXY, $X509_USER_CERT/$X509_USER_KEY or standard grid proxy location')
    base_options.add_option('--key', dest='key', help='x509 key for authentication (defaults to same as certificate)')
    base_options.add_option('--max-timeout', dest='max_timeout', type=int, help='set the max timeout in seconds (After this period even retriable errors will fail. This setting will not interrupt an active connection; use socket-timeout for that.)')
    base_options.add_option('--socket-timeout', dest='socket_timeout', type=int, help='set the socket timeout in seconds (max time for data to be sent or received)')
    base_options.add_option('-r', '--role', dest='role', help='specific role to use for authorization')
    base_options.add_option('-z', '--timezone', dest='timezone', help='set time zone for server responses')
    base_options.add_option('-v','--verbose', action="store_true", dest='verbose', default=False, help="Verbose mode")
    parser.add_option_group(base_options)

    (options, args) = parser.parse_args(args)
    if not args:
        print("No command specified", file=sys.stderr)
        parser.print_help(sys.stderr)
        print('\n',command_list(), file=sys.stderr)
        return 1

    try:
        cmd = commands[args[0]]
    except KeyError:
        print("Unknown command %s" % args[0], file=sys.stderr)
        parser.print_help(sys.stderr)
        print('\n',command_list(), file=sys.stderr)
        return 1

    # if any commands add options that conflict with the previous set,
    # we'd like them to override the old ones
    parser.set_conflict_handler("resolve")

    # set up client
    samweb = SAMWebClient()
    command = cmd(samweb)

    usage = "%%prog [base options] %s [command options]" % (args[0])
    if command.args: usage += ' ' + command.args
    parser.usage = usage
    if command.description: parser.description = command.description
    parser.enable_interspersed_args()
    cmd_options = optparse.OptionGroup(parser, "%s options" % args[0])

    for opt in command.options:
        attribs = {}
        if isinstance(opt, (tuple, list)):
            optname, description = opt
        else:
            optname, description = opt, None

        # if the name contains '=', then the option takes an argument
        # if the '=' is followed by something, use this as the type
        idx = optname.find('=')
        if idx != -1:
            # value
            typ = optname[idx+1:]
            optname = optname[:idx]
            if typ: attribs['type'] = typ
        else:
            # flag
            attribs.update({"action":"store_true", "default":False})
        if description:
            attribs['help'] = description
        attribs["dest"] = optname.replace('-','_')

        cmd_options.add_option('--%s' % optname, **attribs)

    command.addOptions(cmd_options)
    parser.add_option_group(cmd_options)

    (cmdoptions, args) = parser.parse_args(args[1:])


    # configure https settings
    cert = options.cert or cmdoptions.cert
    key = options.key or cmdoptions.key or cert
    if cert: samweb.set_client_certificate(cert, key)

    if options.max_timeout is not None: samweb.max_timeout = options.max_timeout
    if cmdoptions.max_timeout is not None: samweb.max_timeout = cmdoptions.max_timeout

    if options.socket_timeout is not None: samweb.socket_timeout = options.socket_timeout
    if cmdoptions.socket_timeout is not None: samweb.socket_timeout = cmdoptions.socket_timeout

    if options.role: samweb.role = options.role
    if cmdoptions.role: samweb.role = cmdoptions.role

    timezone = cmdoptions.timezone or options.timezone
    if timezone: samweb.timezone = timezone

    # configure the url
    experiment = options.experiment or cmdoptions.experiment
    if experiment is not None:
        samweb.experiment = experiment

    if options.devel or cmdoptions.devel:
        samweb.devel = True

    # verbose mode
    samweb.verbose = (options.verbose or cmdoptions.verbose)

    # since the commands may be used in unix filters, don't fail on SIGPIPE
    signal.signal(signal.SIGPIPE,signal.SIG_DFL)

    try:
        return command.run(cmdoptions, args)
    except ExperimentNotDefined:
        print("Experiment name is not defined. Use -e/--experiment command line option or set SAM_EXPERIMENT or EXPERIMENT environment variable.", file=sys.stderr)
        return 3
    except (ArgumentError, CmdError) as ex:
        print(six.text_type(ex), file=sys.stderr)
        print()
        parser.print_help()
        return 2
    except Error as ex:
        print(six.text_type(ex), file=sys.stderr)
        return 1
