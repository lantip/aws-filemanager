"""Cloud browser cloud/boto_base.py tests."""
from django.test import TestCase

from boto.exception import BotoServerError
from boto.s3.bucket import Bucket
from boto.s3.connection import S3Connection
from boto.s3.key import Key

import mock

from cloud_browser.tests import AWSMockServiceTestCase
from cloud_browser.cloud import errors
from cloud_browser.cloud.aws import AwsObject
from cloud_browser.cloud.boto_base import BotoContainer


class TestDelete(AWSMockServiceTestCase):
    """Tests for delete."""

    boto_container = BotoContainer('fake_conn')
    connection_class = S3Connection

    def setUp(self):  # pylint: disable=invalid-name
        self.get_container_patcher = mock.patch.object(
            self.boto_container, '_get_container')
        self.get_container_fn = self.get_container_patcher.start()
        self.delete_patcher = mock.patch.object(Key, 'delete')
        self.delete_fn = self.delete_patcher.start()
        self.dt_patcher = mock.patch('cloud_browser.common.dt_from_rfc8601')
        self.dt_fn = self.dt_patcher.start()
        super(TestDelete, self).setUp()

    def tearDown(self):  # pylint: disable=invalid-name
        self.get_container_patcher.stop()
        self.delete_patcher.stop()
        self.dt_fn = self.dt_patcher.stop()
        super(TestDelete, self).tearDown()

    def test_delete_file_no_error(self):
        self.set_http_response(status_code=200)
        bucket = self.service_connection.create_bucket('mybucket')
        key_fn = bucket.new_key('foo')

        self.boto_container.native_container.get_key.return_value = key_fn

        self.boto_container.delete('foo', True)
        self.assertTrue(self.delete_fn.called)

    @mock.patch('cloud_browser.cloud.boto_base.BotoContainer._get_key_objects')
    def test_delete_directory_no_error(self, get_key_objects_fn):
        self.set_http_response(status_code=200)
        bucket = self.service_connection.create_bucket('mybucket')
        key_dir_fn = bucket.new_key('foo/')
        key_fn_1 = bucket.new_key('foo/bar')
        key_fn_2 = bucket.new_key('foo/bar/baz')
        key_fn_3 = bucket.new_key('foo/bar/baz/')

        get_key_objects_fn.return_value = [key_fn_1, key_fn_2, key_fn_3]
        self.boto_container.native_container.get_key.return_value = key_dir_fn

        self.boto_container.delete('foo/', False)
        self.assertEqual(4, self.delete_fn.call_count)

    def test_delete_file_not_exist(self):
        self.boto_container.native_container.get_key.return_value = None

        try:
            self.boto_container.delete('foo', True)
        except errors.NoObjectException as error:
            self.assertRegexpMatches(str(error), '.*foo.*')

    def test_delete_directory_not_exist(self):
        self.boto_container.native_container.get_key.return_value = None

        try:
            self.boto_container.delete('foo/', False)
        except errors.NoObjectException as error:
            self.assertRegexpMatches(str(error), '.*foo/.*')

    def test_delete_server_exception(self):
        self.set_http_response(status_code=200)
        bucket = self.service_connection.create_bucket('mybucket')
        key_fn = bucket.new_key('foo')

        self.boto_container.native_container.get_key.return_value = key_fn
        self.set_http_response(status_code=403)

        try:
            self.boto_container.delete('foo', True)
        except errors.StorageResponseException as error:
            self.assertRegexpMatches(str(error), '.*403.*')


class TestRename(AWSMockServiceTestCase):
    """Tests for rename."""

    boto_container = BotoContainer('fake_conn')
    connection_class = S3Connection

    def setUp(self):  # pylint: disable=invalid-name
        self.get_container_patcher = mock.patch.object(
            self.boto_container, '_get_container')
        self.get_container_fn = self.get_container_patcher.start()
        self.get_key_patcher = mock.patch.object(Bucket, 'get_key')
        self.get_key_fn = self.get_key_patcher.start()
        self.delete_patcher = mock.patch.object(Key, 'delete')
        self.delete_fn = self.delete_patcher.start()
        self.dt_patcher = mock.patch('cloud_browser.common.dt_from_rfc8601')
        self.dt_fn = self.dt_patcher.start()
        super(TestRename, self).setUp()

    def tearDown(self):  # pylint: disable=invalid-name
        self.get_container_patcher.stop()
        self.get_key_patcher.stop()
        self.delete_patcher.stop()
        self.dt_patcher.stop()
        super(TestRename, self).tearDown()

    @mock.patch.object(Bucket, 'copy_key')
    def test_rename_object_no_error(self, copy_key_fn):
        self.set_http_response(status_code=200)
        bucket = self.service_connection.create_bucket('mybucket')
        key_fn = bucket.new_key('foo/bar-rename')

        self.get_container_fn.return_value = bucket
        copy_key_fn.return_value = key_fn
        self.get_key_fn.return_value = key_fn

        aws_key_fn = self.boto_container.obj_cls.from_key(self, key_fn)
        self.assertEqual(
            aws_key_fn.name,
            self.boto_container.rename(
                'foo/', 'foo/bar', 'bar-rename', True).name)
        self.assertTrue(self.delete_fn.called)

    # pylint: disable=invalid-name
    def test_rename_object_copy_server_exception(self):
        self.set_http_response(status_code=200)
        bucket = self.service_connection.create_bucket('mybucket')
        key_fn = bucket.new_key('foo/bar')

        self.get_container_fn.return_value = bucket
        self.get_key_fn.return_value = key_fn

        self.set_http_response(status_code=404)
        try:
            self.boto_container.rename('foo/', 'foo/bar', 'bar-rename', True)
        except errors.NoObjectException as error:
            self.assertRegexpMatches(str(error), '.*404.*')

    @mock.patch.object(Bucket, 'copy_key')
    # pylint: disable=invalid-name
    def test_rename_object_key_not_exist(self, copy_key_fn):
        self.set_http_response(status_code=200)
        bucket = self.service_connection.create_bucket('mybucket')
        key_fn = bucket.new_key('foo/bar')

        self.get_container_fn.return_value = bucket
        self.get_key_fn.return_value = None
        copy_key_fn.return_value = key_fn
        aws_key_fn = self.boto_container.obj_cls.from_key(self, key_fn)

        self.assertEqual(
            aws_key_fn.name,
            self.boto_container.rename(
                'foo/', 'foo/bar', 'bar-rename', True).name)
        self.assertFalse(self.delete_fn.called)

    @mock.patch('cloud_browser.cloud.boto_base.BotoContainer._get_key_objects')
    # pylint: disable=invalid-name
    def test_rename_directory_subkey_not_exist(self, get_key_objects_fn):
        self.set_http_response(status_code=200)
        bucket = self.service_connection.create_bucket('mybucket')
        key_fn = bucket.new_key('foo/bar')

        get_key_objects_fn.return_value = [key_fn]
        self.get_container_fn.return_value = bucket
        self.get_key_fn.return_value = None

        self.set_http_response(status_code=404)
        try:
            # pylint: disable=protected-access
            self.boto_container._rename_directory('', 'foo/', 'foo-rename')
        except BotoServerError as error:
            self.assertRegexpMatches(str(error), '.*404.*')
        self.assertEqual(0, self.delete_fn.call_count)


class TestMove(AWSMockServiceTestCase):
    """Tests for move."""

    boto_container = BotoContainer('fake_conn')
    connection_class = S3Connection

    def setUp(self):  # pylint: disable=invalid-name
        self.get_container_patcher = mock.patch.object(
            self.boto_container, '_get_container')
        self.get_container_fn = self.get_container_patcher.start()
        self.get_key_patcher = mock.patch.object(Bucket, 'get_key')
        self.get_key_fn = self.get_key_patcher.start()
        self.delete_patcher = mock.patch.object(Key, 'delete')
        self.delete_fn = self.delete_patcher.start()
        self.dt_patcher = mock.patch('cloud_browser.common.dt_from_rfc8601')
        self.dt_fn = self.dt_patcher.start()
        super(TestMove, self).setUp()

    def tearDown(self):  # pylint: disable=invalid-name
        self.get_container_patcher.stop()
        self.get_key_patcher.stop()
        self.delete_patcher.stop()
        self.dt_patcher.stop()
        super(TestMove, self).tearDown()

    @mock.patch.object(Bucket, 'copy_key')
    def test_move_no_error(self, copy_key_fn):
        self.set_http_response(status_code=200)
        bucket = self.service_connection.create_bucket('mybucket')
        key_fn = bucket.new_key('foo')

        self.get_container_fn.return_value = bucket
        copy_key_fn.return_value = key_fn
        self.get_key_fn.return_value = key_fn

        self.boto_container.move('foo', 'bar')
        self.assertTrue(self.delete_fn.called)

    def test_move_copy_server_exception(self):
        self.set_http_response(status_code=200)
        bucket = self.service_connection.create_bucket('mybucket')
        key_fn = bucket.new_key('foo')

        self.get_container_fn.return_value = bucket
        self.get_key_fn.return_value = key_fn
        self.set_http_response(status_code=403)

        try:
            self.boto_container.move('foo', 'bar')
        except errors.StorageResponseException as error:
            self.assertRegexpMatches(str(error), '.*403.*')
        self.assertFalse(self.delete_fn.called)

    @mock.patch.object(Bucket, 'copy_key')
    def test_move_delete_error(self, copy_key_fn):
        self.set_http_response(status_code=200)
        bucket = self.service_connection.create_bucket('mybucket')
        key_fn = bucket.new_key('foo')

        self.get_container_fn.return_value = bucket
        copy_key_fn.return_value = key_fn
        self.get_key_fn.return_value = key_fn
        self.set_http_response(status_code=403)

        try:
            self.boto_container.move('foo', 'bar')
        except errors.StorageResponseException as error:
            self.assertRegexpMatches(str(error), '.*403.*')
        self.assertTrue(self.delete_fn.called)


class TestFilterObjects(TestCase):
    """Tests for filter_objects."""

    boto_container = BotoContainer('fake_conn')

    def setUp(self):  # pylint: disable=invalid-name
        self.awsobject_patcher = mock.patch.object(AwsObject, '__init__')
        self.key_patcher = mock.patch.object(Key, '__init__')
        self.aws_obj_fn = self.awsobject_patcher.start()
        self.key_fn = self.key_patcher.start()
        self.objects_fn = [self.aws_obj_fn]

    def tearDown(self):  # pylint: disable=invalid-name
        self.awsobject_patcher.stop()
        self.key_patcher.stop()

    @mock.patch.object(boto_container, '_get_container')
    # pylint: disable=invalid-name
    def test_filter_objects_is_subdir_has_key(self, _):
        from datetime import datetime

        self.aws_obj_fn.is_subdir = True
        self.boto_container.native_container.get_key. \
            return_value = self.key_fn
        self.key_fn.last_modified = str(datetime.now())
        self.assertEqual(
            [self.aws_obj_fn],
            self.boto_container.filter_objects(self.objects_fn))

    @mock.patch.object(boto_container, '_get_container')
    # pylint: disable=invalid-name
    def test_filter_objects_is_subidr_no_key(self, _):
        self.aws_obj_fn.is_subdir = True
        self.boto_container.native_container.get_key.return_value = None
        self.assertEqual(
            [self.aws_obj_fn],
            self.boto_container.filter_objects(self.objects_fn))

    @mock.patch.object(boto_container, '_get_container')
    # pylint: disable=invalid-name
    def test_filter_objects_is_file_has_key(self, _):
        self.aws_obj_fn.is_subdir = False
        self.boto_container.native_container.get_key.return_value = self.key_fn
        self.assertEqual(
            [self.aws_obj_fn],
            self.boto_container.filter_objects(self.objects_fn))

    @mock.patch.object(boto_container, '_get_container')
    # pylint: disable=invalid-name
    def test_filter_objects_is_file_no_key(self, _):
        self.aws_obj_fn.is_subdir = False
        self.boto_container.native_container.get_key.return_value = None
        self.assertEqual(
            [],
            self.boto_container.filter_objects(self.objects_fn))


class TestIsSafeBasename(TestCase):
    """Tests for is_safe_basename."""
    boto_container = BotoContainer('fake_conn')

    # pylint: disable=invalid-name
    def test_is_safe_basename_valid(self):
        self.assertEqual(
            True,
            self.boto_container.is_safe_basename('abcd-0123'))

    # pylint: disable=invalid-name
    def test_is_safe_basename_invalid(self):
        self.assertEqual(False, self.boto_container.is_safe_basename('%20'))
        # pylint: disable=anomalous-backslash-in-string
        self.assertEqual(False, self.boto_container.is_safe_basename('\100'))
        self.assertEqual(False, self.boto_container.is_safe_basename('{'))
        self.assertEqual(False, self.boto_container.is_safe_basename('^'))
        self.assertEqual(False, self.boto_container.is_safe_basename('}'))
        self.assertEqual(False, self.boto_container.is_safe_basename('`'))
        self.assertEqual(False, self.boto_container.is_safe_basename(']'))
        self.assertEqual(False, self.boto_container.is_safe_basename('['))
        self.assertEqual(False, self.boto_container.is_safe_basename('>'))
        self.assertEqual(False, self.boto_container.is_safe_basename('<'))
        self.assertEqual(False, self.boto_container.is_safe_basename('~'))
        self.assertEqual(False, self.boto_container.is_safe_basename('#'))
        self.assertEqual(False, self.boto_container.is_safe_basename('|'))
        self.assertEqual(False, self.boto_container.is_safe_basename('"""'))
