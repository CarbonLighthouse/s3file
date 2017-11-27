from __future__ import absolute_import

import unittest

import six
from mock import patch, Mock

from s3file.s3file import S3File


class S3FileTestCase(unittest.TestCase):
    """TestCase Helper for S3File

    Sets up a patch of s3 to simulate uploading and downloading files and provides
    assertion method to verify uploaded content

    Attributes:
        object_mock (Mock): A Mock instance for simulating the return of boto3 s3.Object

    """
    GETTYSBURG = """\
Four score and seven years ago our fathers brought forth on this continent, a new nation,
conceived in Liberty, and dedicated to the proposition that all men are created equal.
"""

    def setUp(self):
        self._uploaded_content = None
        self.object_mock = Mock()
        self.object_mock.download_fileobj.side_effect = self._download_fileobj_stub
        self.object_mock.upload_fileobj.side_effect = self._upload_fileobj_stub

        self.s3patcher = patch('s3file.s3file.s3')
        s3 = self.s3patcher.start()
        s3.Object.return_value = self.object_mock

    def tearDown(self):
        self.s3patcher.stop()

    def _download_fileobj_stub(self, stream):
        """Simulates Object.download_fileobj method by writing Gettysburg to S3File._tempfile

        Args:
            stream: S3File._tempfile which is passed to Object.download_fileobj method

        """
        stream.write(self.GETTYSBURG)

    def _upload_fileobj_stub(self, stream):
        """Simulates Object.upload_fileobj method and captures uploaded content

        Args:
            stream: S3File._tempfile which is passed to Object.upload_fileobj method

        """
        self._uploaded_content = stream.read()

    def assertContentUploaded(self, expected_content):
        """Asserts against the content most recently uploaded to S3 by an S3File instance

        Args:
            expected_content (str): The expected body of the uploaded file

        Raises:
            AssertionError: If `self._uploaded_content` does not match `expected_content`

        """
        self.assertEqual(self._uploaded_content, expected_content)

    @staticmethod
    def make_s3file(mode):
        """Makes test instance of S3File with the given `mode`

        Args:
            mode (str): Open mode for the S3File instance e.g `"r"` `"wb"`, `"a+b"`, etc.

        Returns:
            An open instance of S3File as a context manager.

        """
        return S3File('test-bucket', 'path/to/gettysburg.txt', mode)


class TestRead(S3FileTestCase):
    """
    python -m unittest -v test.ios3.test_file.TestRead
    """
    def test_read_contents(self):
        """should read all contents if file is readable"""
        with self.make_s3file('r') as subject:
            actual = subject.read()
            self.assertEqual(actual, self.GETTYSBURG)

        self.assertEqual(self.object_mock.upload_fileobj.call_count, 0)

    def test_read_bytes(self):
        """should read specified number of bytes if file is readable"""
        with self.make_s3file('r') as subject:
            actual = subject.read(24)
            self.assertEqual(actual, self.GETTYSBURG[:24])

    def test_readline(self):
        """should read a line from file"""
        keepends = True

        with self.make_s3file('r') as subject:
            actual = subject.readline()
            self.assertEqual(actual, self.GETTYSBURG.splitlines(keepends)[0])

        self.assertEqual(self.object_mock.upload_fileobj.call_count, 0)

    def test_readlines(self):
        """should read all lines from file as a list"""
        keepends = True

        with self.make_s3file('r') as subject:
            actual = subject.readlines()
            self.assertEqual(actual, self.GETTYSBURG.splitlines(keepends))

        self.assertEqual(self.object_mock.upload_fileobj.call_count, 0)

    def test_raise_not_open_for_reading(self):
        """should raise IOError if file is not open for reading"""
        with self.make_s3file('w') as subject:
            six.assertRaisesRegex(self, IOError, 'not open for reading', subject.read)
            six.assertRaisesRegex(self, IOError, 'not open for reading', subject.readline)
            six.assertRaisesRegex(self, IOError, 'not open for reading', subject.readlines)

        self.assertContentUploaded('')


class TestWrite(S3FileTestCase):
    """
    python -m unittest -v test.ios3.test_file.TestWrite
    """
    def test_write_new_content(self):
        """should write new content to the file and upload it on exit"""
        with self.make_s3file('w') as subject:
            subject.write('foo')
            subject.write(' bar')

        # Gettysburg content is overwritten
        self.assertContentUploaded('foo bar')

    def test_write_lines_to_file(self):
        """should write new lines to file from a list"""
        with self.make_s3file('w+') as subject:
            subject.writelines(['foo', '\n', 'bar', '\n', 'baz', '\n'])

        self.assertContentUploaded("""\
foo
bar
baz
""")

    def test_raises_not_open_for_writing(self):
        """should raise IOError if the file is not open for writing and is written to"""
        with self.make_s3file('r') as subject:
            six.assertRaisesRegex(self, IOError, 'not open for writing', subject.write, 'foo')
            six.assertRaisesRegex(self, IOError, 'not open for writing',
                                  subject.writelines, ['foo', 'bar'])

            self.assertEqual(self.object_mock.upload_fileobj.call_count, 0)


class TestAppend(S3FileTestCase):
    """
    python -m unittest -v test.ios3.test_file.TestAppend
    """
    def test_append_content(self):
        """should set write content to the end of the file"""
        with self.make_s3file('a+') as subject:
            subject.write(' foo')
            subject.write(' bar')

        self.assertContentUploaded(self.GETTYSBURG + ' foo bar')

    def test_read_from_eof(self):
        """should return empty string"""
        with self.make_s3file('a+') as subject:
            self.assertEqual(subject.read(), '')

        # No content is appended, but file is uploaded as it was after exit
        self.assertContentUploaded(self.GETTYSBURG)


class TestTruncate(S3FileTestCase):
    """
    python -m unittest -v test.ios3.test_file.TestTruncate
    """
    def test_truncate_file(self):
        """should truncate file to 0 bytes"""
        with self.make_s3file('w') as subject:
            actual = subject.truncate()
            expected = 0
            self.assertEqual(actual, expected)

        self.assertContentUploaded('')

    def test_truncate_file_to_size(self):
        """should truncate file to the given size"""
        with self.make_s3file('r+') as subject:
            actual = subject.truncate(24)
            expected = 24
            self.assertEqual(actual, expected)

        self.assertContentUploaded(self.GETTYSBURG[:24])

    def test_raises_not_open_for_writing(self):
        """should raise IOError if the file is not open for writing and is written to"""
        with self.make_s3file('r') as subject:
            six.assertRaisesRegex(self, IOError, 'not open for writing', subject.truncate)

            self.assertEqual(self.object_mock.upload_fileobj.call_count, 0)
