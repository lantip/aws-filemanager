"""Cloud browser views."""
from urlparse import urlparse
import logging

from django.contrib import messages
from django.http import HttpResponse, Http404
from django.shortcuts import render, redirect
from django.utils.importlib import import_module
from django.views.generic.base import View
import django.core.urlresolvers

from cloud_browser.app_settings import settings
from cloud_browser.cloud import get_connection, get_connection_cls, errors
from cloud_browser.common import SEP, ROOT, get_int, basename, \
    get_wd_path, path_parts, path_join, path_join_sep, path_yield, relpath

MAX_LIMIT = get_connection_cls().cont_cls.max_list
LOGGER = logging.getLogger(__name__)


def get_container_by_name(container_name):
    """Get the container object by given container_name.

    :param container_name: A string.

    :return: container. An instance of a container object, inherited from
        the abstract class CloudContainer.
    """
    # code is from broswer view
    container_path, _ = path_parts(container_name)
    conn = get_connection()
    try:
        container = conn.get_container(container_path)
    except errors.NoContainerException:
        raise Http404("No container at: %s" % container_path)
    except errors.NotPermittedException:
        raise Http404("Access denied for container at: %s" % container_path)

    return container


def browser_redirect(container, redirect_directory, permanent=False):
    """Redirect to an existing directory page of 'cloud_browser_browser' view.

    :param container: An instance of a container object, inherited from
        the abstract class CloudContainer.
    :param redirect_directory: A string indicating an directory path name.
    :param permanent: A boolean indicating it is an permanent redirect or
        temporary redirect, defaults to temporary.

    :return: If redirect_direcotry exists, redirect to given directory,
        otherwise to ROOT directory.
    """
    if redirect_directory != ROOT:
        try:
            container.has_directory(redirect_directory)
        except errors.NoObjectException:
            return redirect("cloud_browser_browser",
                            path=container.name,
                            permanent=permanent)

    return redirect("cloud_browser_browser",
                    path=path_join(container.name, redirect_directory),
                    permanent=permanent)


def settings_view_decorator(function):
    """Insert decorator from settings, if any.

    .. note:: Decorator in ``CLOUD_BROWSER_VIEW_DECORATOR`` can be either a
        callable or a fully-qualified string path (the latter, which we'll
        lazy import).
    """

    dec = settings.CLOUD_BROWSER_VIEW_DECORATOR

    # Trade-up string to real decorator.
    if isinstance(dec, basestring):
        # Split into module and decorator strings.
        mod_str, _, dec_str = dec.rpartition('.')
        if not (mod_str and dec_str):
            raise ImportError("Unable to import module: %s" % mod_str)

        # Import and try to get decorator function.
        mod = import_module(mod_str)
        if not hasattr(mod, dec_str):
            raise ImportError("Unable to import decorator: %s" % dec)

        dec = getattr(mod, dec_str)

    if dec and callable(dec):
        return dec(function)

    return function


def _breadcrumbs(path):
    """Return breadcrumb dict from path."""

    full = None
    crumbs = []
    for part in path_yield(path):
        full = path_join(full, part) if full else part
        crumbs.append((full, part))

    return crumbs


@settings_view_decorator
def browser(request, path='', template="cloud_browser/browser.html"):
    """View files in a file path.

    :param request: The request.
    :param path: Path to resource, including container as first part of path.
    :param template: Template to render.
    """
    from itertools import ifilter, islice

    # Inputs.
    container_path, object_path = path_parts(path)
    incoming = request.POST or request.GET or {}

    marker = incoming.get('marker', None)
    marker_part = incoming.get('marker_part', None)
    if marker_part:
        marker = path_join(object_path, marker_part)

    # Get and adjust listing limit.
    limit_default = settings.CLOUD_BROWSER_DEFAULT_LIST_LIMIT
    limit_test = lambda x: x > 0 and (MAX_LIMIT is None or x <= MAX_LIMIT - 1)
    limit = get_int(incoming.get('limit', limit_default),
                    limit_default,
                    limit_test)

    # Q1: Get all containers.
    #     We optimize here by not individually looking up containers later,
    #     instead going through this in-memory list.
    # TODO: Should page listed containers with a ``limit`` and ``marker``.
    conn = get_connection()
    containers = conn.get_containers()

    marker_part = None
    container = None
    objects = None
    upload_form = None
    key_prefix = ''
    if container_path != ROOT:
        # Find marked container from list.
        cont_eq = lambda c: c.name == container_path
        cont_list = list(islice(ifilter(cont_eq, containers), 1))
        if not cont_list:
            raise Http404("No container at: %s" % container_path)

        # Q2: Get objects for instant list, plus one to check "next".
        container = cont_list[0]
        try:
            objects = container.get_objects(object_path, marker, limit+1)
        except (errors.StorageResponseException,
                errors.ClientException) as error:
            LOGGER.warning(
                "Unable to get objects from container {}: {}".format(
                    container.name, error))
            return redirect("cloud_browser_index")

        marker = None

        # If over limit, strip last item and set marker.
        if len(objects) == limit + 1:
            objects = objects[:limit]
            marker = objects[-1].name
            marker_part = relpath(marker, object_path)

        key_prefix = path[len(container.name)+1:]

    if container:
        key_prefix = key_prefix.rstrip(SEP) + SEP if key_prefix else key_prefix
        # Upload form needs the full url, so we hard code it here. It consists
        # of public domain and upload url. For example:
        # request.build_absolute_uri() returns:
        #   "http://mydomain.com/cb/browser/container/foo".
        # urlparse(uri).scheme and urlparse(uri).netloc return:
        #   "http://" and "mydomain.com" respectively.
        # We can dynamically get the public domain here. Appending the upload
        # view full path, full url is obtained.
        parse_result = urlparse(request.build_absolute_uri())
        success_action_redirect = "{}://{}{}".format(
            parse_result.scheme,
            parse_result.netloc,
            django.core.urlresolvers.reverse('upload'),
        )
        # I only implement the AWS upload form here. Backend datastores
        # specify their own upload form fields, and the *args, **kwargs of
        # get_upload_form() methods vary, so corresponding upload method is
        # called here, except "Filesystem".
        datastore = settings.CLOUD_BROWSER_DATASTORE
        if datastore == "AWS":
            upload_form = conn.get_upload_form(
                container_name=container.name,
                key_prefix=key_prefix,
                success_action_redirect=success_action_redirect,
                acl="public-read",
                username=request.user.username,
            )
        elif datastore == "Google":
            upload_form = conn.get_upload_form()
        elif datastore == "Rackspace":
            upload_form = conn.get_upload_form()

        objects = container.filter_objects(objects)

    return render(request, template,
                  {'path': path,
                   'marker': marker,
                   'marker_part': marker_part,
                   'limit': limit,
                   'breadcrumbs': _breadcrumbs(path),
                   'container_path': container_path,
                   'containers': containers,
                   'container': container,
                   'object_path': object_path,
                   'objects': objects,
                   'upload_form': upload_form,
                   'mkdir_action': django.core.urlresolvers.reverse('mkdir'),
                   'delete_action': django.core.urlresolvers.reverse('delete'),
                   'wd_path': key_prefix})


@settings_view_decorator
def document(_, path=''):
    """View single document from path.

    :param path: Path to resource, including container as first part of path.
    """
    container_path, object_path = path_parts(path)
    conn = get_connection()
    try:
        container = conn.get_container(container_path)
    except errors.NoContainerException:
        raise Http404("No container at: %s" % container_path)
    except errors.NotPermittedException:
        raise Http404("Access denied for container at: %s" % container_path)

    try:
        storage_obj = container.get_object(object_path)
    except errors.NoObjectException:
        raise Http404("No object at: %s" % object_path)

    # Get content-type and encoding.
    content_type = storage_obj.smart_content_type
    encoding = storage_obj.smart_content_encoding
    response = HttpResponse(content=storage_obj.read(),
                            content_type=content_type)
    if encoding not in (None, ''):
        response['Content-Encoding'] = encoding

    return response


class UploadFileView(View):

    # pylint: disable=no-self-use, unused-argument
    def get(self, request, *args, **kwargs):

        src_path = request.GET['key']
        container_name = request.GET['bucket']

        container = get_container_by_name(container_name)

        messages.add_message(
            request, messages.INFO,
            "'{}' uploaded".format(src_path))

        return browser_redirect(container, get_wd_path(src_path))


class DeleteView(View):

    # pylint: disable=no-self-use, unused-argument
    def post(self, request, *args, **kwargs):
        container_name = request.POST['container_name']
        src_path = request.POST['src_path']
        is_file = request.POST['is_file'] == "True" or False

        container = get_container_by_name(container_name)

        try:
            container.delete(src_path, is_file)
            messages.add_message(
                request, messages.INFO,
                "'{}' deleted.".format(src_path))
        except (errors.StorageResponseException,
                errors.ClientException) as error:
            LOGGER.warning("Unable to delete '{}': {}".format(src_path, error))
        except errors.NoObjectException as error:
            LOGGER.warning(error)

        return browser_redirect(container, get_wd_path(src_path))


class MkdirView(View):

    # pylint: disable=no-self-use, unused-argument
    def post(self, request, *args, **kwargs):
        container_name = request.POST['container_name']
        wd_path = request.POST['wd_path']
        dir_basename = request.POST['dir_basename']

        container = get_container_by_name(container_name)

        # Check current directory exists or not.
        if wd_path != ROOT:
            try:
                container.has_directory(wd_path)
            except errors.NoObjectException:
                messages.add_message(
                    request, messages.INFO,
                    "'{}' does not exist.".format(wd_path))
                return browser_redirect(container, ROOT)

        # Check the correctness of the new directory name.
        if not container.is_safe_basename(dir_basename):
            messages.add_message(
                request, messages.INFO,
                "Only alphanumeric characters and special characters: \
                {} are allowed in file and directory names.".format(
                container.get_safe_special_characters()))
            return browser_redirect(container, wd_path)

        # Check new directory object exists or not.
        try:
            if container.has_directory(
                    path_join_sep(wd_path, dir_basename)):
                messages.add_message(
                    request, messages.INFO,
                    "'{}' existed.".format(dir_basename))
            return browser_redirect(container, wd_path)
        except errors.NoObjectException:
            pass

        try:
            container.mkdir(path_join_sep(wd_path, dir_basename),
                            username=request.user.username)
            messages.add_message(
                request, messages.INFO,
                "Directory '{}' created.".format(dir_basename))
        except (errors.StorageResponseException,
                errors.ClientException) as error:
            LOGGER.warning(
                "Unable to create the directory '{}': {}.".format(
                    dir_basename, error))

        return browser_redirect(container, wd_path)


class RenameView(View):

    # pylint: disable=no-self-use
    def get(self, request, template="cloud_browser/rename.html"):
        container_name = request.GET['container_name']
        src_path = request.GET['src_path']
        wd_path = request.GET['wd_path']
        is_file = request.GET['is_file'] == "True" or False

        # Check src object exists or not.
        container = get_container_by_name(container_name)

        try:
            if is_file:
                container.get_object(src_path)
            else:
                container.has_directory(src_path)
        except errors.NoObjectException:
            messages.add_message(
                request, messages.INFO,
                "'{}' does not exist.".format(src_path))
            return browser_redirect(container, wd_path)

        return render(request, template,
                      {'container_name': container_name,
                       'src_path': src_path,
                       'src_basename': basename(src_path),
                       'is_file': is_file,
                       'wd_path': wd_path,
                       'rename_action':
                       django.core.urlresolvers.reverse('rename')})

    # pylint: disable=no-self-use, unused-argument
    def post(self, request, *args, **kwargs):
        container_name = request.POST['container_name']
        new_basename = request.POST['new_basename']
        src_path = request.POST['src_path']
        wd_path = request.POST['wd_path']
        is_file = request.POST['is_file'] == "True" or False

        container = get_container_by_name(container_name)

        # Check the correctness of the new object name.
        if container.is_safe_basename(new_basename) is False:
            messages.add_message(
                request, messages.INFO,
                "Only alphanumeric characters and special characters: \
                {} are allowed in file and directory names.".format(
                container.get_safe_special_characters()))
            return browser_redirect(container, wd_path)

        # Check new object name exists or not.
        try:
            path = path_join(wd_path, new_basename)
            if ((is_file and container.get_object(path)) or
                    container.has_directory(path)):
                messages.add_message(
                    request, messages.INFO,
                    "'{}' existed.".format(
                        path_join(wd_path, new_basename)))
                return browser_redirect(container, wd_path)
        except errors.NoObjectException:
            pass

        try:
            container.rename(wd_path, src_path, new_basename, is_file)
            messages.add_message(
                request, messages.INFO,
                "'{}' was renamed as '{}'.".format(
                    src_path, path_join(wd_path, new_basename)))
        except errors.NoObjectException as error:
            messages.add_message(
                request, messages.INFO,
                "'{}' does not exist.".format(src_path))
            LOGGER.warning(
                "Unable to rename '{}': {}.".format(src_path, error))
        except (errors.StorageResponseException,
                errors.ClientException) as error:
            LOGGER.warning(
                "Unable to rename '{}': {}.".format(src_path, error))

        return browser_redirect(container, wd_path)


class MoveFileView(View):

    # pylint: disable=no-self-use, unused-argument
    def get(self, request, template="cloud_browser/move.html"):
        container_name = request.GET['container_name']
        src_path = request.GET['src_path']
        wd_path = request.GET['wd_path']

        container = get_container_by_name(container_name)

        # Check src object exists or not.
        if wd_path != ROOT:
            try:
                container.has_directory(wd_path)
            except errors.NoObjectException:
                messages.add_message(
                    request, messages.INFO,
                    "'{}' does not exist.".format(src_path))
                return browser_redirect(container, wd_path)

        # Check if only ROOT directory in container.
        all_dirs_paths = container.get_directories_paths()
        if len(all_dirs_paths) == 1:
            messages.add_message(
                request, messages.INFO,
                "Please create a directory.")
            return browser_redirect(container, wd_path)

        # The select menu shows all directories except the working directory.
        all_dirs_paths.remove(wd_path)

        return render(request, template,
                      {'container_name': container_name,
                       'src_path': src_path,
                       'wd_path': wd_path,
                       'all_dirs_paths': all_dirs_paths,
                       'move_action':
                       django.core.urlresolvers.reverse('move')})

    # pylint: disable=no-self-use, unused-argument
    def post(self, request, *args, **kwargs):
        container_name = request.POST['container_name']
        target_dir_path = request.POST['target_dir_path']
        src_path = request.POST['src_path']
        wd_path = request.POST['wd_path']

        container = get_container_by_name(container_name)

        #Check new object (target_dir_path + src_basename) exist or not.
        try:
            if container.get_object(path_join(target_dir_path,
                                              basename(src_path))):
                messages.add_message(
                    request, messages.INFO,
                    "'{}' has file '{}' .".format(
                        target_dir_path, basename(src_path)))
                return browser_redirect(container, wd_path)
        except errors.NoObjectException:
            pass

        try:
            container.move(src_path, target_dir_path)
            messages.add_message(
                request, messages.INFO,
                "'{}' was moved to '{}'.".format(
                    src_path, target_dir_path))
        except errors.NoObjectException as error:
            messages.add_message(
                request, messages.INFO,
                "'{}' does not exist.".format(src_path))
            LOGGER.warning(
                "Unable to move file '{}': {}.".format(
                    src_path, error))
        except (errors.StorageResponseException,
                errors.ClientException) as error:
            LOGGER.warning(
                "Unable to move file '{}': {}.".format(
                    src_path, error))

        return browser_redirect(container, wd_path)
