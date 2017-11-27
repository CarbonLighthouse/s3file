from __future__ import absolute_import

import os

from six.moves.urllib.parse import urlparse, urlencode

from .s3file import S3File


def open_url(url, mode):
    """Opens filesystem agnostic stream wrapper depending on file url schema "s3://" or "file://"

    Will open a local file using `open` when given a file path or "file://" url, and a `S3File`
    context manager when given a "s3://" url.

    Args:
        url (str): Path to file as file path, `"file://"`, or `"s3://"` url
        mode (str): Specifies the mode in which the file is opened.

    Returns:
        File like IOBase object

    """
    parsed_url = urlparse(url)

    if parsed_url.scheme == 's3':
        return S3File(parsed_url.netloc, parsed_url.path.lstrip('/'), mode)
    else:
        dir_path = os.path.dirname(os.path.realpath(parsed_url.path))

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        return open(parsed_url.path, mode)
