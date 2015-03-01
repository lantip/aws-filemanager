"""Abstract boto-based datastore.

The boto_ library provides interfaces to both Amazon S3 and Google Storage
for Developers. This abstract base class gets most of the common work done.

.. note::
    **Installation**: Use of this module requires the open source boto_
    package.

.. _boto: http://code.google.com/p/boto/
"""
from cloud_browser.app_settings import settings
from cloud_browser.cloud import errors, base
from cloud_browser.common import ROOT, SEP, requires, dt_from_header


###############################################################################
# Constants / Conditional Imports
###############################################################################
try:
    import boto  # pylint: disable=F0401
except ImportError:
    boto = None  # pylint: disable=C0103


###############################################################################
# Classes
###############################################################################
# pylint: disable=invalid-name
def boto_server_client_error_wrapper(operation):
    """Exception wrapper for catching BotoClientError, BotoServerError and
    NoObjectException.
    """
    import sys
    from boto.exception import BotoClientError, BotoServerError

    def wrapped(*args, **kwargs):
        try:
            return operation(*args, **kwargs)
        except BotoServerError as error:
            if error.status == 404:
                raise errors.NoObjectException, \
                    errors.NoObjectException(error), \
                    sys.exc_info()[2]
            else:
                raise errors.StorageResponseException, \
                    errors.StorageResponseException(error), \
                    sys.exc_info()[2]
        except BotoClientError as error:
            raise errors.ClientException, \
                errors.ClientException(error), \
                sys.exc_info()[2]

    return wrapped


class BotoExceptionWrapper(errors.CloudExceptionWrapper):
    """Boto :mod:`boto` exception translator."""
    error_cls = errors.CloudException

    @requires(boto, 'boto')
    def translate(self, exc):
        """Return whether or not to do translation."""
        from boto.exception import StorageResponseError

        if isinstance(exc, StorageResponseError):
            if exc.status == 404:
                return self.error_cls(unicode(exc))

        return None


class BotoKeyWrapper(errors.CloudExceptionWrapper):
    """Boto :mod:`boto` key exception translator."""
    error_cls = errors.NoObjectException


class BotoBucketWrapper(errors.CloudExceptionWrapper):
    """Boto :mod:`boto` bucket exception translator."""
    error_cls = errors.NoContainerException


class BotoObject(base.CloudObject):
    """Boto 'key' object wrapper."""
    #: Exception translations.
    wrap_boto_errors = BotoKeyWrapper()

    @classmethod
    def is_key(cls, result):
        """Return ``True`` if result is a key object."""
        raise NotImplementedError

    @classmethod
    def is_prefix(cls, result):
        """Return ``True`` if result is a prefix object."""
        raise NotImplementedError

    @wrap_boto_errors
    def _get_object(self):
        """Return native storage object."""
        return self.container.native_container.get_key(self.name)

    @wrap_boto_errors
    def _read(self):
        """Return contents of object."""
        return self.native_obj.read()

    @classmethod
    def from_result(cls, container, result):
        """Create from ambiguous result."""
        if result is None:
            raise errors.NoObjectException

        elif cls.is_prefix(result):
            return cls.from_prefix(container, result)

        elif cls.is_key(result):
            return cls.from_key(container, result)

        raise errors.CloudException("Unknown boto result type: %s" %
                                    type(result))

    @classmethod
    def from_prefix(cls, container, prefix):
        """Create from prefix object."""
        if prefix is None:
            raise errors.NoObjectException

        return cls(container,
                   name=prefix.name,
                   obj_type=cls.type_cls.SUBDIR)

    @classmethod
    def from_key(cls, container, key):
        """Create from key object."""
        if key is None:
            raise errors.NoObjectException

        last_modified = dt_from_header(key.last_modified) \
            if key.last_modified else None

        # Get Key   (1123): Tue, 13 Apr 2010 14:02:48 GMT
        # List Keys (8601): 2010-04-13T14:02:48.000Z
        return cls(container,
                   name=key.name,
                   size=key.size,
                   content_type=key.content_type,
                   content_encoding=key.content_encoding,
                   last_modified=last_modified,
                   obj_type=cls.type_cls.FILE)


class BotoContainer(base.CloudContainer):
    """Boto container wrapper."""
    #: Storage object child class.
    obj_cls = BotoObject

    #: Exception translations.
    wrap_boto_errors = BotoBucketWrapper()

    #: Maximum number of objects that can be listed or ``None``.
    #:
    #: :mod:`boto` transparently pages through objects, so there is no real
    #: limit to the number of object that can be displayed.  However, for
    #: practical reasons, we'll limit it to the same as Rackspace.
    max_list = 10000

    def get_safe_special_characters(self):
        """Object name safe characters.

        :rtype: ``str``
        """

        return "!\-_.*'()"  # pylint: disable=anomalous-backslash-in-string

    @wrap_boto_errors
    def _get_container(self):
        """Return native container object."""
        return self.conn.native_conn.get_bucket(self.name)

    @boto_server_client_error_wrapper
    def get_objects(self, path, marker=None,
                    limit=settings.CLOUD_BROWSER_DEFAULT_LIST_LIMIT):
        """Get objects."""
        from itertools import islice

        path = path.rstrip(SEP) + SEP if path else path

        result_set = self.native_container.list(path, SEP, marker)
        # Get +1 results because marker and first item can match as we strip
        # the separator from results obscuring things. No real problem here
        # because boto masks any real request limits.
        results = list(islice(result_set, limit+1))
        if results:
            if marker and results[0].name.rstrip(SEP) == marker.rstrip(SEP):
                results = results[1:]
            else:
                results = results[:limit]

        return [self.obj_cls.from_result(self, r) for r in results]

    @wrap_boto_errors
    def get_object(self, path):
        """Get single object."""
        key = self.native_container.get_key(path)
        return self.obj_cls.from_key(self, key)

    def has_directory(self, path):
        """Check the directory exists or not.

        This method checks if there are keys whose name start with "path". If
        None, raise exception.

        :param path: A string.
        """
        results = self.native_container.get_all_keys(prefix=path)
        if len(results)==0:
            raise errors.NoObjectException

        return True

    @boto_server_client_error_wrapper
    def get_directories_paths(self):
        """Get all the directories paths in the given container.

        :rtype: ``list[str]``
        """
        dirs_paths = [ROOT]
        prefixes = [ROOT]
        while prefixes:
            prefix = prefixes.pop()
            results = self.native_container.get_all_keys(
                prefix=prefix,
                delimiter=SEP)
            for result in results:
                if self.obj_cls.is_prefix(result):
                    dirs_paths.append(result.name)
                    prefixes.append(result.name)

        return sorted(set(dirs_paths))

    @boto_server_client_error_wrapper
    def _get_key_objects(self, path):
        """Get all the keys of files and subdirectories under the given path.

        This method is different from get_objects(). get_objects() returns key
        and prefix objects, and a directory is regarded as a prefix object. For
        _get_key_objects(), the elements' return type is boto Key object.

        :param path: A string.

        :return: A list of instances of boto Key objects.
        """
        keys = []
        has_more = True
        marker = path

        while has_more:
            current_keys = self.native_container.get_all_keys(marker=marker,
                                                              prefix=path)

            if len(current_keys) == 0:
                return keys

            has_more = len(current_keys) == 1000
            marker = current_keys[-1].name
            keys += current_keys

        return keys

    @boto_server_client_error_wrapper
    def filter_objects(self, objects):
        """Remove NoneType key objects from the objects list, which should be
        regarded as the parent directory. Set the user-defined the metadata.

        Django-cloud-browser wraps boto object with AwsObject. There are two
        types boto objects, Key and Prefix. Key object is regarded as a file
        and Prefix object is regarded as directory. Like AWS S3 console, we use
        a key (whose name ends with "/") to mock a directory so that we can
        have a hierarchical file system. The goal of this function is to remove
        the mock-directory key objects and set the properties of the Prefix
        objects from their corresponding Key objects, so that the browser can
        display a correct hierachical file system.

        :param objects: A list of AwsObject objects.

        :return: A list of AwsObjects objects.
        :rtype: :class:`cloud_browser.cloud.aws.AwsObject`
        """
        object_to_be_removed = []
        index = 0

        for obj in objects:
            # Set metatdata last_modified for is_subdir object which has
            # corresponding real key stored in S3.
            key = None
            if obj.is_subdir:
                key = self.native_container.get_key("{}/".format(obj.name))
                if key:
                    # is_subdir object does not have last_modified metadata
                    # when initilized, use the metadata from coressponding boto
                    # key here.
                    obj.last_modified = dt_from_header(key.last_modified) \
                        if key.last_modified else None
            else:
                key = self.native_container.get_key(obj.name)
                # Remove directory key.
                if key is None:
                    object_to_be_removed.append(index)
            # Retreive 'modified-by' for is_file and is_subdir objects.
            if key:
                obj.modified_by = key.metadata.get('modified-by', 'unknown')

            index += 1

        return [objects[i] for i in range(0, len(objects))
                if i not in object_to_be_removed]

    def is_safe_basename(self, base_name):
        """Verify that the base_name string path contains only safe
        characters ([0-9a-zA-Z], !, -, _, ., *, ', (, )).

        :param basename: A string.

        :return: ``True`` if key name string does not contain any unsafe
            characters.
        :rtype:  ``bool``
        """
        import re

        # Space is a valid character
        # pylint: disable=anomalous-backslash-in-string
        if re.match("[a-zA-Z0-9!\-_.*'() ]+$", base_name):
            return True

        return False

    @boto_server_client_error_wrapper
    def mkdir(self, dir_path, username=None):
        """Create a new subdirectory under dir_path and set user-defined
        metadata: modified-by.

        :param dir_name: A string. To differentiate file and directory, the key
            name of a directory ends with "/", for example, "foo/bar/".
        :param username: A string.

        :rtype: :class:`cloud_browser.cloud.aws.AwsObject`
        """
        from boto.s3.key import Key

        key = Key(self.native_container, dir_path)
        if username:
            key.set_metadata('modified-by', username)
        key.set_contents_from_string('It is a directory.')
        key.set_acl('public-read')

        return self.obj_cls.from_key(self, key)

    def _delete_directory(self, dir_src_path):
        """Delete the directory and all of the files and subdirectories under
        it.

        :param subdir_src_path: A string ends with "/".

        :rtype: :class:`cloud_browser.cloud.aws.AwsObject`
        """
        keys = self._get_key_objects(dir_src_path)
        # Delete all the files and sub-dirs
        # pylint: disable=expression-not-assigned
        [key.delete() for key in keys if key]
        # Delete the directory itself if all of the files and sub-dirs are
        # successfully deleted
        key = self.native_container.get_key(dir_src_path)
        if key:
            return self.obj_cls.from_key(self, key.delete())
        else:
            raise errors.NoObjectException(
                "{} does not exist".format(dir_src_path))

    @boto_server_client_error_wrapper
    def delete(self, src_path, is_file):
        """If src_path is a file, delete it. If it's a directory, delete all
        paths under it and itself.

        :param src_path: A string.
        :param is_file: A boolean indicating the target AwsObject is a file
            or not.

        :rtype: :class:`cloud_browser.cloud.aws.AwsObject`
        """
        if is_file:
            key = self.native_container.get_key(src_path)
            if key:
                return self.obj_cls.from_key(self, key.delete())
            else:
                raise errors.NoObjectException(
                    "{} does not exist".format(src_path))
        else:
            return self._delete_directory("{}/".format(src_path))

    def _rename_object(self, parent_dir_path, src_path, new_basename):
        """Rename a file. Cause AWS S3 is a key value store, this method can
        also be used to rename a directory, which key ends with "/".

        :param parent_dir_path: A string ends with "/", for example "foo/bar/",
            or ROOT directory, "".
        :param src_path: A string, for example a file "foo/bar/baz" or a
            directory "foo/bar/baz/".
        :param new_basename: A string. If renaming a file: "baz-rename", or a
            directory: "baz-rename/".

        :return: If the file is successfully renamed (copy and delete the
            original key).
        :rtype: :class:`cloud_browser.cloud.aws.AwsObject`
        """
        renamed = self.native_container.copy_key(
            parent_dir_path + new_basename,
            self.native_container.name,
            src_path,
            preserve_acl=True)
        key = self.native_container.get_key(src_path)
        if key:
            key.delete()

        return renamed

    def _rename_directory(self, parent_dir_path, dir_src_path, new_basename):
        """Rename the directory and all of the files and subdirectories under
        it.

        :param parent_dir_path. A string, for example "foo/bar/".
        :param dir_src_path. A string, for example "foo/bar/baz/".
        :param new_basename. A string, for example "baz-rename".

        :return: Renamed object, if the directory and all of the files and
            subdirectories under it are successfully renamed.
        :rtype: :class:`cloud_browser.cloud.aws.AwsObject`
        """
        from boto.exception import BotoServerError

        # Rename all the files and subdirectories under the target directory.
        # While error occurs, continue renaming only if 'key does not exist'.
        keys = self._get_key_objects(dir_src_path)
        for key in keys:
            try:
                self._rename_object(
                    "{}{}/".format(parent_dir_path, new_basename),
                    key.name,
                    key.name[len(dir_src_path):])
            except BotoServerError as error:
                if error.status == 404:
                    pass
        # If directory key exists, rename the directory itself and return the
        # renamed directory key. Otherwise, change the key name of first item
        # in '_get_key_objects' to the new directory name. Because in `rename`,
        # renaming a directory returns a Prefix object, class variable 'name'
        # is required.
        try:
            self.get_object(dir_src_path)
            return  self._rename_object(
                parent_dir_path,
                dir_src_path,
                "{}/".format(new_basename))
        except errors.NoObjectException:
            keys[0].name = "{}{}".format(parent_dir_path, new_basename)
            return keys[0]

    @boto_server_client_error_wrapper
    def rename(self, parent_dir_path, src_path, new_basename, is_file):
        """If src_path is a file, rename it. If it's a directory, rename all
        paths under it and itself.

        :param parent_dir_path: A string ends with "/" (excludes ROOT), for
            example "foo/bar/".
        :param src_path: A string, for example "foo/bar/baz". If renaming a
            directory, a delimiter "/" will be appened to the src_key_name.
            (The path of an CloudObject does not end with "/". That an
            instance of a CloudObject is a directory or is a file is
            determined by "is_file" or "is_subdir" property)
        :param new_basename: A string, for example "baz-rename". It does not
            contain key prefix.
        :param is_file: A boolean indicating the target CloudObject is a file
            or not.

        :return: If successfully rename the src key.
        :rtype: :class:`cloud_browser.cloud.aws.AwsObject`
        """
        if is_file:
            return self.obj_cls.from_key(
                self, self._rename_object(parent_dir_path,
                                          src_path,
                                          new_basename))
        else:
            return self.obj_cls.from_prefix(
                self, self._rename_directory(parent_dir_path,
                                             "{}/".format(src_path),
                                             new_basename))

    @boto_server_client_error_wrapper
    def move(self, src_file_path, target_dir_path):
        """Move the file to the target directory.

        :param src_file_path: A string ends with "/" (excludes ROOT).
        :param target_dir_path: A string.

        :return: If successfully move the key to the target directory.
        :rtype: :class:`cloud_browser.cloud.aws.AwsObject`
        """
        moved = self.native_container.copy_key(
            target_dir_path + src_file_path.split("/")[-1],
            self.native_container.name,
            src_file_path,
            preserve_acl=True)

        key = self.native_container.get_key(src_file_path)
        if key:
            key.delete()
        return self.obj_cls.from_key(self, moved)

    @classmethod
    def from_bucket(cls, connection, bucket):
        """Create from bucket object."""
        if bucket is None:
            raise errors.NoContainerException

        # It appears that Amazon does not have a single-shot REST query to
        # determine the number of keys / overall byte size of a bucket.
        return cls(connection, bucket.name)


class BotoConnection(base.CloudConnection):
    """Boto connection wrapper."""
    #: Container child class.
    cont_cls = BotoContainer

    #: Exception translations.
    wrap_boto_errors = BotoBucketWrapper()

    def _get_connection(self):
        """Return native connection object."""
        raise NotImplementedError("Must create boto connection.")

    @wrap_boto_errors
    def _get_containers(self):
        """Return available containers."""
        buckets = self.native_conn.get_all_buckets()
        return [self.cont_cls.from_bucket(self, b) for b in buckets]

    @wrap_boto_errors
    def _get_container(self, path):
        """Return single container."""
        bucket = self.native_conn.get_bucket(path)
        return self.cont_cls.from_bucket(self, bucket)

    def get_upload_form(self, *args, **kwargs):
        """Return html format upload form."""
        raise NotImplementedError
