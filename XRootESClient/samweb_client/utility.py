from __future__ import absolute_import

from .exceptions import *
import six

# Calculate the enstore style CRC for a file
# Raises a standard python IOError in case of failures
# Uses the adler32 algorithm from zlib, except with an initial
# value of 0, instead of 1, and adler32 returns a signed int (ie 32 bits)
# while we want an unsigned value

class UnknownHashAlgorithm(Error):
    """ Unknown checksum algorithm """

def fileChecksum(path, checksum_types=None, oldformat=False):
    """Calculate enstore compatible CRC value"""
    try:
        f =open(path,'rb')
    except (IOError, OSError) as ex:
        raise Error(str(ex))
    try:
        return calculateChecksum(f, checksum_types=checksum_types, oldformat=oldformat)
    finally:
        f.close()

def calculateChecksum(fileobj, checksum_types=None, oldformat=False):
    algorithms = {}
    if oldformat:
        if not (checksum_types is None or checksum_types==['enstore']):
            raise Error("Old format checksums only support enstore type")
        algorithms["enstore"] = _get_checksum_algorithm("enstore")
    else:
        if checksum_types is None: checksum_types = ["adler32"]
        for ct in checksum_types:
            algorithms[ct] = _get_checksum_algorithm(ct)

    readblocksize = 1024*1024
    while 1:
        try:
            s = fileobj.read(readblocksize)
        except (OSError, IOError) as ex:
            raise Error(str(ex))
        if not s: break
        for a in six.itervalues(algorithms):
            a.update(s)

    if oldformat:
        return { "crc_value" : algorithms["enstore"].value(), "crc_type" : "adler 32 crc type" }
    else:
        return [ "%s:%s" % (a,v.value()) for a,v in six.iteritems(algorithms) ]

# for compatibility
def fileEnstoreChecksum(path):
    return fileChecksum(path, oldformat=True)
def enstoreChecksum(fileobj):
    return calculateChecksum(fileobj, oldformat=True)

# Don't create the algorithm classes unless they are actually needed

_Adler32 = None
def _make_adler32(startval=None, hex_output=True):
    global _Adler32
    import zlib

    if not _Adler32:
        class _Adler32(object):
            def __init__(self, startval=None, hex_output=hex_output):
                if startval is not None:
                    self._value = zlib.adler32(b'', startval)
                else:
                    self._value = zlib.adler32(b'')
                self._hex = hex_output
            def update(self, data):
                self._value = zlib.adler32(data, self._value)
            def value(self):
                crc = int(self._value)
                if crc < 0:
                    # Return 32 bit unsigned value
                    crc  = (crc & 0x7FFFFFFF) | 0x80000000
                if self._hex:
                    return '%08x' % crc
                else:
                    return crc

    return _Adler32(startval, hex_output)

_Hasher = None
def _make_hash(algorithm):

    global _Hasher
    if not _Hasher:
        class _Hasher(object):
            def __init__(self, hasher):
                self.hash = hasher
            def update(self, data):
                self.hash.update(data)
            def value(self):
                return self.hash.hexdigest()

    try:
        from hashlib import md5,sha1,new
    except ImportError:
        from md5 import new as md5
        from sha import new as sha1
        new = None

    if algorithm == 'md5':
        return _Hasher(md5())
    elif algorithm == 'sha1':
        return _Hasher(sha1())
    elif new:
        try:
            return _Hasher(new(algorithm))
        except ValueError:
            pass

    raise UnknownHashAlgorithm("No checksum algorithm for %s" % algorithm)

def _get_checksum_algorithm(algorithm):

    if algorithm == 'enstore':
        return _make_adler32(0, hex_output=False)
    elif algorithm == 'adler32':
        return _make_adler32()
    else:
        return _make_hash(algorithm)

def list_checksum_algorithms():
    algos = set(['enstore', 'adler32'])
    try:
        import hashlib
        if hasattr(hashlib, 'algorithms_available'):
            algos.update(hashlib.algorithms_available)
        elif hasattr(hashlib, 'algorithms'):
            algos.update(hashlib.algorithms)
        else:
            algos.update(['md5','sha1','sha224', 'sha256', 'sha384', 'sha512'])
    except ImportError:
        algos.update(['md5','sha1',])
    return algos
