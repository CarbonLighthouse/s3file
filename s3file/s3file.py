import errno
import contextlib
import os
import io
import re
from ssl import SSLError
from tempfile import TemporaryFile

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

s3 = boto3.resource('s3')


@contextlib.contextmanager
def s3errors(path):
    """Translate S3 errors to IOErrors

    Acts as a context manager to catch boto3 errors associated with s3 Object requests.
    Converts expected errors to IOErrors which are compliant with errors thrown by file
    like objects.

    Args:
        path (str): The path of the s3 object being accessed

    Yields:
        Nothing, just wraps code to catch boto3 errors and reraise them

    Raises:
        IOError: IO errors associated with s3 object requests

    """
    try:
        yield
    except ClientError as e:
        error = e.response.get('Error', {})
        error_code = error.get('Code', None)
        response_meta = e.response.get('ResponseMetadata', {})
        http_status = response_meta.get('HTTPStatusCode', 200)

        if error_code == 'NoSuchBucket':
            raise IOError(errno.EPERM, os.strerror(errno.EPERM), path)
        if http_status == 404:
            raise IOError(errno.ENOENT, os.strerror(errno.ENOENT), path)
        elif http_status == 403:
            raise IOError(errno.EACCES, os.strerror(errno.EACCES), path)
        else:
            raise IOError(errno.EOPNOTSUPP, os.strerror(errno.EOPNOTSUPP), path)
    except SSLError:
        raise IOError(errno.EOPNOTSUPP, os.strerror(errno.EOPNOTSUPP), path)
    except EndpointConnectionError:
        raise IOError(errno.ETIMEDOUT, os.strerror(errno.ETIMEDOUT), path)


class S3File(io.IOBase):
    """File like proxy for s3 files, manages upload and download of locally managed temporary file
    """

    def __init__(self, bucket, key, mode='w+b', *args, **kwargs):
        super(S3File, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.key = key
        self.mode = mode
        self.path = self.bucket + '/' + self.key

        # converts mode to readable/writable to enable the temporary file to have S3 data
        # read or written to it even if the S3File is read/write/append
        # i.e. "r" => "r+", "ab" => "a+b"
        updatable_mode = re.sub(r'^([rwa]+)(b?)$', r'\1+\2', mode)
        self._tempfile = TemporaryFile(updatable_mode)

        try:
            with s3errors(self.path):
                if 'a' in mode:
                    # File is in an appending mode, start with the content in file
                    s3.Object(bucket, key).download_fileobj(self._tempfile)
                    self.seek(0, os.SEEK_END)
                elif 'a' not in mode and 'w' not in mode and 'x' not in mode:
                    # file is not in a create mode, so it is in read mode
                    # start with the content in the file, and seek to the beginning
                    s3.Object(bucket, key).download_fileobj(self._tempfile)
                    self.seek(0, os.SEEK_SET)
        except Exception:
            self.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        try:
            if self.writable():
                self.seek(0)
                with s3errors(self.path):
                    s3.Object(self.bucket, self.key).upload_fileobj(self._tempfile)
        finally:
            self._tempfile.close()

    @property
    def closed(self):
        return self._tempfile.closed

    def fileno(self):
        return self._tempfile.fileno()

    def flush(self):
        return self._tempfile.flush()

    def isatty(self):
        return self._tempfile.isatty()

    def readable(self):
        return 'r' in self.mode or '+' in self.mode

    def read(self, n=-1):
        if not self.readable():
            raise IOError('not open for reading')
        return self._tempfile.read(n)

    def readinto(self, b):
        return self._tempfile.readinto(b)

    def readline(self, limit=-1):
        if not self.readable():
            raise IOError('not open for reading')
        return self._tempfile.readline(limit)

    def readlines(self, hint=-1):
        if not self.readable():
            raise IOError('not open for reading')
        return self._tempfile.readlines(hint)

    def seek(self, offset, whence=os.SEEK_SET):
        self._tempfile.seek(offset, whence)
        return self.tell()

    def seekable(self):
        return True

    def tell(self):
        return self._tempfile.tell()

    def writable(self):
        return 'w' in self.mode or 'a' in self.mode or '+' in self.mode or 'x' in self.mode

    def write(self, b):
        if not self.writable():
            raise IOError('not open for writing')
        self._tempfile.write(b)
        return len(b)

    def writelines(self, lines):
        if not self.writable():
            raise IOError('not open for writing')
        return self._tempfile.writelines(lines)

    def truncate(self, size=None):
        if not self.writable():
            raise IOError('not open for writing')

        if size is None:
            size = self.tell()

        self._tempfile.truncate(size)
        return size
