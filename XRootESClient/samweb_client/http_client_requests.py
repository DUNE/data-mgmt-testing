from __future__ import print_function
from __future__ import absolute_import
# HTTP client using requests library

import requests
import time

from samweb_client import Error, json
from .http_client import SAMWebHTTPClient, SAMWebConnectionError, makeHTTPError, SAMWebHTTPError

def make_client(*args, **kwargs):
    return RequestsHTTPClient(*args, **kwargs)

import sys

# it might be a good idea to verify the server, but at the moment we never send sensitive data. So shut it up.
from requests.packages import urllib3
urllib3.disable_warnings()

# we want to trap requests errors and return a samweb error
# this can only happen when in streaming mode, so use a proxy object
class StreamingResponseMethodWrapper(object):
    def __init__(self, method):
        self._method = method
    def __call__(self, *args, **kwargs):
        try:
            return self._method(*args, **kwargs)
        except requests.exceptions.RequestException as ex:
            raise Error("Error reading response body: %s" % str(ex))

class StreamingResponseWrapper(object):
    """ Wrap methods that may use streaming to trap exceptions and convert them """
    def __init__(self, response):
        self.__dict__['_response'] = response

    def __getattr__(self, name):
        # get the wrapped attribute, handling any errors
        # if it's a method, return a proxy that also traps
        # errors when called
        from types import MethodType
        try:
            attr = getattr(self._response, name)
        except requests.exceptions.RequestException as ex:
            raise Error("Error reading response body: %s" % str(ex))
        if isinstance(attr, MethodType):
            return StreamingResponseMethodWrapper(attr)
        else:
            return attr

    def __setattr__(self, name, value):
        raise AttributeError("Can't set attributes on proxy")
    def __delattr__(self, name):
        raise AttributeError("Can't delete attributes on proxy")

def _wrap_response(response, stream):
    if stream:
        return StreamingResponseWrapper(response)
    else:
        return response

class RequestsHTTPClient(SAMWebHTTPClient):

    def __init__(self, verbose=False, *args, **kwargs):
        SAMWebHTTPClient.__init__(self, *args, **kwargs)
        self._session = None
        self.verbose = verbose

    def _make_session(self):
        if self._session is None:
            self._session = requests.Session()
            self._session.verify = False
            self._session.cert = self._cert

    def use_client_certificate(self, cert=None, key=None):
        SAMWebHTTPClient.use_client_certificate(self, cert, key)
        self._session = None # This will clear any existing session with a different cert

    def _doURL(self, url, method="GET", content_type=None, role=None, stream=False, compress=True, *args, **kwargs):
        headers = self.get_default_headers()
        if 'headers' in kwargs:
            headers.update(kwargs['headers'])

        if content_type is not None:
            headers['Content-Type'] = content_type

        if role is not None:
            headers['SAM-Role'] = str(role)

        # requests uses compression by default
        if not compress:
            headers['Accept-Encoding'] = None
        
        kwargs['headers'] = headers

        if self.socket_timeout is not None:
            kwargs['timeout'] = self.socket_timeout

        self._logMethod( method, url, params=kwargs.get("params"), data=kwargs.get("data"))

        self._make_session()

        tmout = time.time() + self.max_timeout
        retryinterval = 1

        while True:
            try:
                response = self._session.request(method, url, stream=stream, *args, **kwargs)
                if response.history:
                    # if multiple redirects just report the last one
                    self._logger("HTTP", response.history[-1].status_code, "redirect to", response.url)
                if 200 <= response.status_code < 300:
                    return _wrap_response(response, stream)
                else:
                    # something went wrong
                    if response.status_code == 403 and self._cert is None:
                        from samweb_client import NoAuthorizationCredentials
                        raise NoAuthorizationCredentials(method, url)
                    elif 'application/json' in response.headers['Content-Type']:
                        jsonerr = response.json()
                        errmsg = jsonerr['message']
                        errtype = jsonerr['error']
                    else:
                        if response.status_code >= 500:
                            errmsg = "HTTP error: %d %s" % (response.status_code, response.reason)
                        else:
                            errmsg = response.text.rstrip()
                        errtype = None
                    exc = makeHTTPError(response.request.method, url, response.status_code, errmsg, errtype)
                    if 400 <= response.status_code < 500:
                        # For any 400 error, don't bother retrying
                        raise exc
            except requests.exceptions.SSLError as ex:
                errmsg = str(ex)
                raise self.make_ssl_error(errmsg)
            except requests.exceptions.Timeout as ex:
                errmsg = "%s: Timed out waiting for response" % (url,)
                exc = SAMWebConnectionError(errmsg)
            except requests.exceptions.ConnectionError as ex:
                errmsg = "%s: %s" % (url, str(ex))
                exc = SAMWebConnectionError(errmsg)
            
            if time.time() >= tmout:
                raise exc
                
            if self.verboseretries:
                print('%s: retrying in %d s' %( errmsg, retryinterval))
            time.sleep(retryinterval)
            retryinterval*=2
            if retryinterval > self.maxretryinterval:
                retryinterval = self.maxretryinterval

    def postURL(self, url, data=None, **kwargs):
        # httplib isn't sending a Content-Length: 0 header for empty bodies
        # even though the latest HTTP revision says this is legal, cherrypy and
        # some other servers don't like it
        # this may be fixed in python 2.7.4
        if not data:
            kwargs.setdefault('headers',{})['Content-Length'] = '0'
        return SAMWebHTTPClient.postURL(self, url, data, **kwargs)

    def putURL(self, url, data=None, **kwargs):
        # httplib isn't sending a Content-Length: 0 header for empty bodies
        # even though the latest HTTP revision says this is legal, cherrypy and
        # some other servers don't like it
        # this may be fixed in python 2.7.4
        if not data:
            kwargs.setdefault('headers',{})['Content-Length'] = '0'
        return SAMWebHTTPClient.putURL(self, url, data, **kwargs)

    def _get_user_agent(self):
        ua = SAMWebHTTPClient._get_user_agent(self)
        ua += " requests/%s" % requests.__version__
        return ua

__all__ = [ 'make_client' ]
