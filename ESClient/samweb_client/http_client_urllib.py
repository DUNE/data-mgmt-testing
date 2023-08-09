from __future__ import print_function
from __future__ import absolute_import
from six.moves.urllib.parse import urlencode, quote, quote_plus
import six.moves.urllib.request, six.moves.urllib.error, six.moves.urllib.parse,six.moves.http_client
from six.moves.urllib.request import urlopen, Request
from six.moves.urllib.error import URLError, HTTPError

import time
import socket
import ssl
import os
import sys
import zlib
import collections

from samweb_client import Error, json, convert_to_string
from .http_client import SAMWebHTTPClient, SAMWebConnectionError, makeHTTPError, SAMWebHTTPError, make_ssl_error
import six

def make_client(*args, **kwargs):
    # There is no local state, so just return the module
    return URLLibHTTPClient()

def _read_response(response, chunk_size=128*1024):
    while True:
        data = response.read(chunk_size)
        if not data: break
        #print>>sys.stderr, 'Read %d bytes' % len(data)
        yield data
    response.close()

def _gzip_decoder(iterator):
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    for chunk in iterator:
        decoded = decoder.decompress(chunk)
        if decoded:
            #print>>sys.stderr, 'Decompressed %d bytes' % len(decoded)
            yield decoded
    else:
        decoded = decoder.flush()
        if decoded:
            #print>>sys.stderr, 'Decompressed %d bytes' % len(decoded)
            yield decoded

class Response(object):
    """ Wrapper for the response object. Provides a text attribue that contains the body of the response.
    If stream = False, then the body is read immediately and the connection closed, else the data is not
    read from the server until you try to access it

    The API tries to be similar to that of the requests library, since it'd be nice if we could replace urllib2
    with that. However, it doesn't work with python 2.4, which we need for SL5 support.
    """

    def __init__(self, wrapped, logger, stream=False):
        self._wrapped = wrapped
        self._data = _read_response(self._wrapped)

        # handle zipped content
        if self.headers.get('Content-Encoding','').lower() == 'gzip':
            self._data = _gzip_decoder(self._data)
            logger("Decompressing gzipped response body")

        if hasattr(self.headers, 'get_content_encoding'):
            self.encoding = self.headers.get_content_encoding() or 'iso-8859-1'
        else:
            # This probably only gets used in py2, and we don't use the encoding there
            # so it probably doesn't matter and we won't bother trying to extract the actual encoding
            self.encoding = 'iso-8859-1'

        # if not streaming, load the whole thing now
        if not stream:
            self._data = tuple(self._data)

    @property
    def text(self):
        # in python 2 this returns str, not unicode, unlike the requests method
        # in python 3 you get str (ie unicode, like requests)
        try:
            return ''.join(convert_to_string(d, encoding=self.encoding) for d in self._data)
        except Exception as ex:
            raise Error("Error reading response body: %s" % str(ex))

    @property
    def status_code(self):
        return self._wrapped.code
    @property
    def headers(self):
        return self._wrapped.headers

    def json(self):
        # json just does a read() on the file object, so we aren't losing anything
        # by reading the whole thing into a string
        return json.loads(b''.join(self._data))

    def iter_lines(self):
        pending = None
        try:
            for chunk in self.iter_content():
                if pending is not None:
                    chunk = pending + chunk
                lines = chunk.splitlines()
                if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
                    pending = lines.pop()
                else:
                    pending = None

                for line in lines:
                    yield line
            if pending is not None:
                yield pending
        except Exception as ex:
            raise Error("Error reading response body: %s" % str(ex))

    def iter_content(self, chunk_size=1):
        # chunk size is currently ignored
        try:
            return iter(self._data)
        except Exception as ex:
            raise Error("Error reading response body: %s" % str(ex))

    def __del__(self):
        try:
            self._wrapped.close()
        except: pass



# handler to cope with client certificate auth
# Note that this does not verify the server certificate
# Since the main purpose is for the server to authenticate
# the client. However, you should be cautious about sending
# sensitive infomation (not that SAM deals with that)
# as there's no protection against man-in-the-middle attacks
class HTTPSClientAuthHandler(six.moves.urllib.request.HTTPSHandler):
    def __init__(self, cert):
        six.moves.urllib.request.HTTPSHandler.__init__(self)
        if cert:
            if isinstance(cert, six.string_types):
                self.cert = self.key = cert
            else:
                self.cert, self.key = cert
        else:
            self.cert = self.key = None

    def _get_https_connection_args(self):
        try:
            # python 2.7.9+ support
            """ could allow verification with something like
            context = ssl.create_default_context(capath="/etc/grid-security/certificates")
            """
            context = ssl.create_default_context()
            # disable certificate verification
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

            # load the client cert
            if self.cert:
                try:
                    context.load_cert_chain(self.cert, self.key)
                except IOError as ex:
                    raise make_ssl_error(ex, self.cert)
            return { 'context' : context }
        except AttributeError:
            # older python
            if self.cert:
                return { "key_file" : self.key, "cert_file" : self.cert }
            else:
                return {}


    def https_open(self, req):
        # Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.getConnection, req)

    def getConnection(self, host, timeout=300):
        return six.moves.http_client.HTTPSConnection(host, **self._get_https_connection_args())

class HTTP307RedirectHandler(six.moves.urllib.request.HTTPRedirectHandler):

    # We want to keep trying if the redirect provokes an error
    max_repeats = sys.maxsize

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        m = req.get_method()
        try:
            logger = req.logger
        except AttributeError:
            pass

        if code in (301, 302, 303):
            # Strictly (according to RFC 2616), 301 or 302 in response
            # to a POST MUST NOT cause a redirection without confirmation
            # from the user (of urllib, in this case).  In practice,
            # essentially all clients do redirect in this case, so we
            # do the same.
            logger("HTTP", code, "redirect to GET", newurl)
            return LoggingRequest(newurl,
                           headers=req.headers,
                           origin_req_host=req.origin_req_host,
                           unverifiable=True, logger=logger)
        elif code in (307, 308):
            logger("HTTP", code, "redirect to", req.get_method(), newurl)
            newreq = RequestWithMethod(newurl, method=req.get_method(), headers=req.headers, origin_req_host=req.origin_req_host, unverifiable=True, logger=logger)
            newreq.data = req.data
            return newreq
        else:
            raise HTTPError(req.get_full_url(), code, msg, headers, fp)

    # 308 should be handles the same way as 307
    http_error_308 = six.moves.urllib.request.HTTPRedirectHandler.http_error_307

class LoggingRequest(six.moves.urllib.request.Request):
    def __init__(self, *args, **kwargs):
        self.logger = kwargs.pop('logger', self._nulllogger)
        six.moves.urllib.request.Request.__init__(self, *args, **kwargs)
    @staticmethod
    def _nulllogger(*args):
        return

class RequestWithMethod(LoggingRequest):
    def __init__(self, *args, **kwargs):
        self._method = kwargs.pop('method', None)
        LoggingRequest.__init__(self, *args, **kwargs)

    def get_method(self):
        return self._method or six.moves.urllib.request.Request.get_method(self)

class URLLibHTTPClient(SAMWebHTTPClient):
    """ HTTP client using standard urllib implementation """

    def __init__(self, *args, **kwargs):
        SAMWebHTTPClient.__init__(self, *args, **kwargs)
        self._opener = six.moves.urllib.request.build_opener(HTTP307RedirectHandler())

    def _doURL(self, url, method='GET', params=None, data=None, content_type=None, stream=False, compress=False, headers=None,role=None):
        request_headers = self.get_default_headers()
        if headers is not None:
            request_headers.update(headers)
        if content_type:
            request_headers['Content-Type'] = content_type

        if compress or stream:
            # enable gzipped encoding for streamed data since that might be large
            request_headers['Accept-Encoding'] = 'gzip'

        if role is not None:
            request_headers['SAM-Role'] = str(role)

        if method in ('POST', 'PUT'):
            # these always require body data
            if data is None:
                data = b''

        self._logMethod(method, url, params=params, data=data)

        if params is not None:
            if '?' not in url: url += '?'
            else: url += '&'
            url += urlencode(params, doseq=True)
        tmout = time.time() + self.max_timeout
        retryinterval = 1

        request = RequestWithMethod(url, method=method, headers=request_headers, logger=self._logger)
        if data is not None:
            if isinstance(data, collections.Mapping):
                data = urlencode(data, doseq=True).encode('ascii')
            elif isinstance(data, collections.Sequence) and not isinstance(data, six.string_types):
                data = urlencode(data).encode('ascii')
            if isinstance(data, six.text_type):
                data = data.encode('utf-8') # the most likely encoding
            request.data = data

        kwargs = {}
        if self.socket_timeout is not None:
            kwargs['timeout'] = self.socket_timeout
        while True:
            try:
                return Response(self._opener.open(request, **kwargs), stream=stream, logger=self._logger)
            except HTTPError as x:
                #python 2.4 treats 201 and up as errors instead of normal return codes
                err_response = Response(x, logger=self._logger)
                if 201 <= x.code <= 299:
                    return err_response
                if x.code == 403 and self._cert is None:
                    from samweb_client import NoAuthorizationCredentials
                    raise NoAuthorizationCredentials(method, url)
                elif x.headers.get('Content-Type') == 'application/json':
                    err = err_response.json()
                    errmsg = err['message']
                    errtype = err['error']
                else:
                    if x.code >= 500:
                        errmsg = "HTTP error: %d %s" % (x.code, x.msg)
                    else:
                        errmsg = err_response.text.rstrip()
                    errtype = None
                x.close() # ensure that the socket is closed (otherwise it may hang around in the traceback object)
                # retry server errors
                if x.code >= 500 and time.time() < tmout:
                    pass
                else:
                    raise makeHTTPError(method, url, x.code, errmsg, errtype)
            except URLError as x:
                if isinstance(x.reason, ssl.SSLError):
                    raise self.make_ssl_error(str(x.reason))
                else:
                    errmsg = str(x.reason)
                if time.time() >= tmout:
                    raise SAMWebConnectionError(errmsg)
            except six.moves.http_client.HTTPException as x:
                # I'm not sure exactly what circumstances cause this
                # but assume that it's a retriable error
                try:
                    errmsg = str(x.reason)
                except AttributeError:
                    errmsg = str(x)
                if time.time() >= tmout:
                    raise SAMWebConnectionError(errmsg)

            if self.verboseretries:
                print('%s: retrying in %d s' %( errmsg, retryinterval), file=sys.stderr)
            time.sleep(retryinterval)
            retryinterval*=2
            if retryinterval > self.maxretryinterval:
                retryinterval = self.maxretryinterval

    def use_client_certificate(self, cert=None, key=None):
        """ Use the given certificate and key for client ssl authentication """
        SAMWebHTTPClient.use_client_certificate(self, cert, key)
        self._opener = six.moves.urllib.request.build_opener(HTTPSClientAuthHandler(self._cert), HTTP307RedirectHandler )

__all__ = [ 'make_client' ]
