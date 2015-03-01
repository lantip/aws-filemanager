"""Cloud datastore API base abstraction."""
import mimetypes

from cloud_browser.cloud import errors
from cloud_browser.app_settings import settings
from cloud_browser.common import SEP, \
    path_join, basename


class CloudObjectTypes(object):
    """Cloud object types helper."""
    FILE = 'file'
    SUBDIR = 'subdirectory'


class CloudObject(object):
    """Cloud object wrapper."""
    type_cls = CloudObjectTypes

    def __init__(self, container, name, **kwargs):
        """Initializer.

        :param container: Container object.
        :param name: Object name / path.
        :kwarg size: Number of bytes in object.
        :kwarg content_type: Document 'content-type'.
        :kwarg content_encoding: Document 'content-encoding'.
        :kwarg last_modified: Last modified date.
        :kwarg obj_type: Type of object (e.g., file or subdirectory).
        """
        self.container = container
        self.name = name.rstrip(SEP)
        self.size = kwargs.get('size', 0)
        self.content_type = kwargs.get('content_type', '')
        self.content_encoding = kwargs.get('content_encoding', '')
        self.last_modified = kwargs.get('last_modified', None)
        self.type = kwargs.get('obj_type', self.type_cls.FILE)
        self.modified_by = None
        self.__native = None

    @property
    def native_obj(self):
        """Native storage object."""
        if self.__native is None:
            self.__native = self._get_object()

        return self.__native

    def _get_object(self):
        """Return native storage object."""
        raise NotImplementedError

    @property
    def is_subdir(self):
        """Is a subdirectory?"""
        return self.type == self.type_cls.SUBDIR

    @property
    def is_file(self):
        """Is a file object?"""
        return self.type == self.type_cls.FILE

    @property
    def path(self):
        """Full path (including container)."""
        return path_join(self.container.name, self.name)

    @property
    def basename(self):
        """Base name from rightmost separator."""
        return basename(self.name)

    @property
    def smart_content_type(self):
        """Smart content type."""
        content_type = self.content_type
        if content_type in (None, '', 'application/octet-stream'):
            content_type, _ = mimetypes.guess_type(self.name)

        return content_type

    @property
    def smart_content_encoding(self):
        """Smart content encoding."""
        encoding = self.content_encoding
        if not encoding:
            base_list = self.basename.split('.')
            while (not encoding) and len(base_list) > 1:
                _, encoding = mimetypes.guess_type('.'.join(base_list))
                base_list.pop()

        return encoding

    def read(self):
        """Return contents of object."""
        return self._read()

    def _read(self):
        """Return contents of object."""
        raise NotImplementedError


class CloudContainer(object):
    """Cloud container wrapper."""
    #: Storage object child class.
    obj_cls = CloudObject

    #: Maximum number of objects that can be listed or ``None``.
    max_list = None

    def __init__(self, conn, name=None, count=None, size=None):
        """Initializer."""
        self.conn = conn
        self.name = name
        self.count = count
        self.size = size
        self.__native = None

    @property
    def native_container(self):
        """Native container object."""
        if self.__native is None:
            self.__native = self._get_container()

        return self.__native

    def get_safe_special_characters(self):
        """Object name safe characters.

        :rtype: ``str``
        """
        raise NotImplementedError

    def _get_container(self):
        """Return native container object."""
        raise NotImplementedError

    def get_objects(self, path, marker=None,
                    limit=settings.CLOUD_BROWSER_DEFAULT_LIST_LIMIT):
        """Get objects."""
        raise NotImplementedError

    def get_object(self, path):
        """Get single object."""
        raise NotImplementedError

    def has_directory(self, path):
        """Check directory path exists or not."""
        raise NotImplementedError

    def get_directories_paths(self):
        """Get all the directories in the given container.

        :rtype: ``list[str]``
        """
        raise NotImplementedError

    def filter_objects(self, objects):
        """Filter NoneType objects, or the invalid objects specific for the
        backend datastore. For example, key name "/foo/bar" is invalid for
        Amazon S3. This method can also be used to filter the objects depending
        on the user-defined requirements.

        :return: A list of instances of actual objects, inheritaed from
            abstract class CloudObject.
        """
        raise NotImplementedError

    def is_safe_basename(self, base_name):
        """Verifies that the base_name string path contains only safe
        characters.

        :rtype: ``bool``
        """
        raise NotImplementedError

    def mkdir(self, dir_path, username=None):
        """Create a new subdirectory under dir_path.

        :raises: :class:`StorageResponseException`
        :raises: :class:`ClientException`
        """
        raise NotImplementedError

    def delete(self, src_path, is_file):
        """If src_path is a file, delete it. If it's a directory, delete all
        paths under it and itself.

        :raises: :class:`StorageResponseException`
        :raises: :class:`ClientException`
        """
        raise NotImplementedError

    def rename(self, parent_dir_path, src_path, new_basename, is_file):
        """If src_path is a file, rename it. If it's a directory, rename all
        paths under it and itself.

        :raises: :class:`StorageResponseException`
        :raises: :class:`ClientException`
        """
        raise NotImplementedError

    def move(self, src_file_path, target_dir_path):
        """Move the file to the target directory.

        :raises: :class:`StorageResponseException`
        :raises: :class:`ClientException`
        """
        raise NotImplementedError


class CloudConnection(object):
    """Cloud connection wrapper."""
    #: Container child class.
    cont_cls = CloudContainer

    #: Maximum number of containers that can be listed or ``None``.
    max_list = None

    def __init__(self, account, secret_key):
        """Initializer."""
        self.account = account
        self.secret_key = secret_key
        self.__native = None

    @property
    def native_conn(self):
        """Native connection object."""
        if self.__native is None:
            self.__native = self._get_connection()

        return self.__native

    def _get_connection(self):
        """Return native connection object."""
        raise NotImplementedError

    def get_containers(self):
        """Return available containers."""
        permitted = lambda c: settings.container_permitted(c.name)
        return [c for c in self._get_containers() if permitted(c)]

    def _get_containers(self):
        """Return available containers."""
        raise NotImplementedError

    def get_container(self, path):
        """Return single container."""
        if not settings.container_permitted(path):
            raise errors.NotPermittedException(
                "Access to container \"%s\" is not permitted." % path)
        return self._get_container(path)

    def _get_container(self, path):
        """Return single container."""
        raise NotImplementedError

    def get_upload_form(self, *args, **kwargs):
        """Return html format upload form.

        Uploading an object requires the cloud account and secert key,
        which are class members of the CloudConnection class, so the
        get_upload_form method is put here.
        """
        raise NotImplementedError
