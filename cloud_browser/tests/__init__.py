# The code of class AWSMockServiceTestCase is from the boto projet:
# https://github.com/boto/boto/blob/develop/tests/unit/__init__.py
# The code of loding unittest from multiple files refers the code in:
# http://stackoverflow.com/questions/6248510
#
# Copyright (c) 2006-2011 Mitch Garnaat http://garnaat.org/
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.
import unittest

from boto.compat import http_client

import mock


class AWSMockServiceTestCase(unittest.TestCase):
    """Base class for mocking aws services."""
    # This param is used by the unittest module to display a full
    # diff when assert*Equal methods produce an error message.
    maxDiff = None
    connection_class = None

    def setUp(self):  # pylint: disable=invalid-name
        self.https_connection = mock.Mock(spec=http_client.HTTPSConnection)
        self.https_connection.debuglevel = 0
        self.https_connection_factory = (
            mock.Mock(return_value=self.https_connection), ())
        self.service_connection = self.create_service_connection(
            https_connection_factory=self.https_connection_factory,
            aws_access_key_id='aws_access_key_id',
            aws_secret_access_key='aws_secret_access_key')
        self.initialize_service_connection()

    def initialize_service_connection(self):
        self.actual_request = None
        # pylint: disable=protected-access, attribute-defined-outside-init
        self.original_mexe = self.service_connection._mexe
        # pylint: disable=protected-access
        self.service_connection._mexe = self._mexe_spy

    def create_service_connection(self, **kwargs):
        if self.connection_class is None:
            raise ValueError("The connection_class class attribute must be "
                             "set to a non-None value.")
        # pylint: disable=not-callable
        return self.connection_class(**kwargs)

    def _mexe_spy(self, request, *args, **kwargs):
        # pylint: disable=attribute-defined-outside-init
        self.actual_request = request
        return self.original_mexe(request, *args, **kwargs)

    # pylint: disable=protected-access, dangerous-default-value
    def create_response(self, status_code, reason='', header=[], body=None):
        if body is None:
            body = self.default_body()
        response = mock.Mock(spec=http_client.HTTPResponse)
        response.status = status_code
        response.read.return_value = body
        response.reason = reason

        response.getheaders.return_value = header
        response.msg = dict(header)

        def overwrite_header(arg, default=None):
            header_dict = dict(header)
            if arg in header_dict:
                return header_dict[arg]
            else:
                return default
        response.getheader.side_effect = overwrite_header

        return response

    # pylint: disable=protected-access, dangerous-default-value
    def set_http_response(self, status_code, reason='', header=[], body=None):
        http_response = self.create_response(status_code, reason, header, body)
        self.https_connection.getresponse.return_value = http_response

    def default_body(self):  # pylint: disable=no-self-use
        return ''


import pkgutil


def suite():
    return unittest.TestLoader().discover(
        "cloud_browser.tests",
        pattern="*.py")


if '__path__' in locals():
    for loader, module_name, is_pkg in pkgutil.walk_packages(__path__):
        module = loader.find_module(module_name).load_module(module_name)
        for name in dir(module):
            obj = getattr(module, name)
            if (isinstance(obj, type) and
                    issubclass(obj, unittest.case.TestCase)):
                # pylint: disable=exec-statement
                exec ('%s = obj' % obj.__name__)
