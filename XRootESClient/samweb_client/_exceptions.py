""" The exception classes live here, along with a class that manages the on-demand generation of new classes """

from __future__ import absolute_import
import threading
import six

# pretend that we are really the exceptions module
if __package__ is None:
    __name__ = __name__.replace('_', '')
else:
    __name__ = __package__ + '.exceptions'

class Error(Exception):
  pass

class PlaceHolderError(Error):
    """ Placeholder for generated exceptions where we don't know the real type """
    def __str__(self):
        return 'Placeholder exception for %s' % self.__class__.__name__

class ArgumentError(Error):
    """ For methods called with incorrect arguments """
    pass

class NoMoreFiles(Exception):
  pass

class ChecksumMismatch(Error):
    """ For mismatched checksums """

class MissingChecksum(ChecksumMismatch):
    """ Can't verify checksum because there's nothing to compare against """

class Timeout(Exception): pass

class SAMWebConnectionError(Error):
    """ Connection failure """
    pass

class SAMWebSSLError(SAMWebConnectionError):
    """ SSL connection failure """
    pass

class SAMWebHTTPError(Error):
    def __init__(self, method, url, code, msg):
        self.method = method
        self.url = url
        self.code = code
        self.msg = msg

    def __str__(self):
        if 400 <= self.code < 500:
            return self.msg
        else:
            return "HTTP error: %(code)d %(msg)s\nURL: %(url)s" % self.__dict__

class GenericHTTPError(SAMWebHTTPError):
    _code = None
    def __init__(self, method, url, msg):
        SAMWebHTTPError.__init__(self, method, url, self._code, msg)

class HTTPBadRequest(SAMWebHTTPError):
    def __init__(self, method, url, msg):
        SAMWebHTTPError.__init__(self, method, url, 400, msg)

class InvalidMetadata(HTTPBadRequest): pass
class DimensionError(HTTPBadRequest): pass
class HTTPForbidden(SAMWebHTTPError):
    def __init__(self, method, url, msg):
        SAMWebHTTPError.__init__(self, method, url, 403, msg)

class HTTPNotFound(SAMWebHTTPError):
    def __init__(self, method, url, msg):
        SAMWebHTTPError.__init__(self, method, url, 404, msg)

class FileNotFound(HTTPNotFound): pass
class DefinitionNotFound(HTTPNotFound): pass
class ProjectNotFound(HTTPNotFound): pass

class NoAuthorizationCredentials(HTTPForbidden):
    def __init__(self, method, url):
        HTTPForbidden.__init__(self, method, url, "No grid certificate or proxy provided and this method requires authorization")

class HTTPConflict(SAMWebHTTPError):
    def __init__(self, method, url, msg):
        SAMWebHTTPError.__init__(self, method, url, 409, msg)

class _Exceptions(object):

    # This class is substituted for the exceptions module
    # it looks up exceptions in this module, and if it doesn't
    # find them, creates a placeholder class that can be used in
    # an except clause
    # When an exception is thrown it will be created if it doesn't
    # exist, or if it's a placeholder then the base class is mutated
    # to the correct one for the type that was received from the server
    
    __all__ = ['makeHTTPError']

    def __init__(self):
        self._dynamic_exceptions = {}
        self._base_classes_by_code = {
            400: self.HTTPBadRequest,
            403: self.HTTPForbidden,
            404: self.HTTPNotFound,
            409: self.HTTPConflict,
            }

        self.lock = threading.Lock()

    def __getattr__(self, attr):
        if attr.startswith('_'): 
            # no underscores
            raise AttributeError()
        self.lock.acquire()
        try:
            try:
                return self._dynamic_exceptions[attr]
            except KeyError:
                o = self._makeClass(attr, PlaceHolderError)
                self._dynamic_exceptions[attr] = o
            return o
        finally:
            self.lock.release()

    def _makeClass(self, exctype, basecls):
        return type(exctype, (basecls,), dict())

    def makeHTTPError(self, method, url, code, msg, exctype=None):
        ''' Make a new error object. If the exctype is given but there is no existing
        class, create a new one to throw and add it to the module namespace '''
        self.lock.acquire()
        try:
            return self._makeHTTPError(method, url, code, msg, exctype)
        finally:
            self.lock.release()

    def _makeHTTPError(self, method, url, code, msg, exctype=None):
        if exctype and not ' ' in exctype:
            exctype = str(exctype)
            exccls = globals().get(exctype) or self._dynamic_exceptions.get(exctype)
            if not exccls:
                basecls = self._get_exception_class(code)
                if basecls:
                    exccls = self._makeClass(exctype, basecls)
                    self._dynamic_exceptions[exctype] = exccls
            else:
                # the exception already exists - check if it's a placeholder
                if issubclass(exccls, self.PlaceHolderError):
                    # mutate the base class
                    newbase = self._get_exception_class(code)
                    exccls.__bases__ = (newbase, )
        else:
            exccls = self._get_exception_class(code)
        if exccls:
            o = exccls(method, url, msg)
            return o
        else:
            return self.SAMWebHTTPError(method, url, code, msg)

    def _get_exception_class(self, code):
        try:
            return self._base_classes_by_code[code]
        except KeyError:
            # make a new base
            clsname = "HTTP%dError" % code
            try:
                # check if anybody already referred to this class
                base = self._dynamic_exceptions[clsname]
                # set the base to the correct type
                base.__bases__ = (GenericHTTPError, )
            except KeyError:
                # else create it
                base = self._makeClass(clsname, GenericHTTPError)
                self._base_classes_by_code[code] = base
                self._dynamic_exceptions[clsname] = base
            base._code = code
            return base

    def make_ssl_error(self, ex, cert=None):
        """ Try to make sense of ssl errors and return a suitable exception object """
        if isinstance(ex, IOError):
            msg = "%s: unable to load certificate and/or key file" % str(ex)
        else:
            msg = str(ex)
            if 'sslv3 alert handshake failure' in msg or 'error:14094410' in msg or 'CERTIFICATE_VERIFY_FAILED' in msg:
                if cert:
                    msg = "%s: is client certificate or proxy valid?" % msg
                else:
                    msg = "%s: no client certificate or proxy found" % msg
            elif 'SSL_CTX_use_PrivateKey_file' in msg:
                msg = "%s: unable to open private key file" % msg
            elif 'PEM lib' in msg:
                msg = "%s: unable to load certificate and/or key file" % msg
        return SAMWebSSLError("SSL error: " + msg)

# Add the exception classes the _Exceptions.__all__ so that "from exceptions import *" will work
import types
for k,v in list(six.iteritems(globals())):
    if isinstance(v, type) and issubclass(v, Error):
        setattr(_Exceptions, k, v)
        _Exceptions.__all__.append(k)
# These don't inherit from Error
_Exceptions.NoMoreFiles = NoMoreFiles
_Exceptions.Timeout = Timeout
_Exceptions.__all__.extend( ('NoMoreFiles','Timeout', 'make_ssl_error') )

