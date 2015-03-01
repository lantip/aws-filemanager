"""Cloud browser views.py tests."""
from django.test import TestCase

import mock

from cloud_browser.cloud import errors
from cloud_browser.cloud.base import CloudContainer
from cloud_browser.common import ROOT
from cloud_browser import views


class TestBrowserRedirect(TestCase):
    """Tests for browser_redirect."""

    def setUp(self):  # pylint: disable=invalid-name
        self.cloudcontainer_patcher = mock.patch.object(CloudContainer,
                                                        '__init__')
        self.redirect_patcher = mock.patch('cloud_browser.views.redirect')
        self.container_fn = self.cloudcontainer_patcher.start()
        self.redirect_fn = self.redirect_patcher.start()

    def tearDown(self):  # pylint: disable=invalid-name
        self.cloudcontainer_patcher.stop()
        self.redirect_patcher.stop()

    def test_browser_redirect(self):
        self.container_fn.name = 'redirect_test'
        self.container_fn.has_directory.return_value = True

        views.browser_redirect(self.container_fn, 'key/of/dir/')
        self.container_fn.has_directory.assert_called_with('key/of/dir/')
        self.redirect_fn.assert_called_with('cloud_browser_browser',
                                            path='redirect_test/key/of/dir',
                                            permanent=False)

        views.browser_redirect(self.container_fn, ROOT)
        self.container_fn.has_directory.assert_called_with('key/of/dir/')
        self.redirect_fn.assert_called_with('cloud_browser_browser',
                                            path='redirect_test',
                                            permanent=False)

    # pylint: disable=invalid-name
    def test_browser_redirect_no_object_exception(self):
        self.container_fn.name = 'redirect_test'
        self.container_fn.has_directory.side_effect = errors.NoObjectException

        views.browser_redirect(self.container_fn, 'key/of/dir/')
        self.container_fn.has_directory.assert_called_with('key/of/dir/')
        self.redirect_fn.assert_called_with('cloud_browser_browser',
                                            path='redirect_test',
                                            permanent=False)
