"""Amazon Simple Storage Service (S3) datastore.

.. note::
    **Installation**: Use of this module requires the open source boto_
    package.

.. _boto: http://code.google.com/p/boto/
"""
import base64
import datetime
import hashlib
import hmac
import json

from cloud_browser.cloud import boto_base as base
from cloud_browser.common import requires


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
class AwsObject(base.BotoObject):
    """AWS 'key' object wrapper."""

    @classmethod
    @requires(boto, 'boto')
    def is_key(cls, result):
        """Return ``True`` if result is a key object."""
        from boto.s3.key import Key

        return isinstance(result, Key)

    @classmethod
    @requires(boto, 'boto')
    def is_prefix(cls, result):
        """Return ``True`` if result is a prefix object."""
        from boto.s3.prefix import Prefix

        return isinstance(result, Prefix)


class AwsContainer(base.BotoContainer):
    """AWS container wrapper."""
    #: Storage object child class.
    obj_cls = AwsObject


class AwsConnection(base.BotoConnection):
    """AWS connection wrapper."""
    #: Container child class.
    cont_cls = AwsContainer

    @base.BotoConnection.wrap_boto_errors
    @requires(boto, 'boto')
    def _get_connection(self):
        """Return native connection object."""
        return boto.connect_s3(self.account, self.secret_key)

    @staticmethod
    def _get_policy(container_name, key_prefix,
                    success_action_redirect, acl, username):
        # expires.isoformat() has microseconds.
        expires = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        expiration = expires.strftime('%Y-%m-%dT%H:%M:%SZ')
        conditions = [
            {'bucket': container_name},
            ['starts-with', '$key', '{}'.format(key_prefix)],
            {'acl': acl},
            {'success_action_redirect': success_action_redirect},
            {'x-amz-meta-modified-by': username},
        ]

        policy = {
            'expiration': expiration,
            'conditions': conditions,
        }

        return base64.b64encode(json.dumps(policy))

    def _get_signature(self, policy):

        return base64.b64encode(
            hmac.new(self.secret_key, policy, hashlib.sha1).digest())

    # pylint: disable=arguments-differ, too-many-arguments
    def get_upload_form(self, container_name=None, key_prefix=None,
                        success_action_redirect=None, acl=None, username=None):
        policy = self._get_policy(container_name, key_prefix,
                                  success_action_redirect, acl, username)
        signature = self._get_signature(
            self._get_policy(
                container_name,
                key_prefix,
                success_action_redirect,
                acl,
                username,
            )
        )

        return  """
            <form action="https://{bucket}.s3.amazonaws.com" \
                  method="post" enctype="multipart/form-data">
                <input type="hidden" name="key" \
                    value="{key_prefix}${{filename}}">
                <input type="hidden" name="AWSAccessKeyId"
                    value="{access_key_id}"> \
                <input type="hidden" name="acl" value="public-read">
                <input type="hidden" name="success_action_redirect" \
                    value="{success_action_redirect}">
                <input type="hidden" name="x-amz-meta-modified-by" \
                    value="{username}">
                <input type="hidden" name="policy" value="{policy}">
                <input type="hidden" name="signature" value="{signature}">
                <input type="file" name="file">
                <input type="submit" value="UPLOAD">
            </form>
        """.format(
            bucket=container_name,
            key_prefix=key_prefix,
            access_key_id=self.account,
            success_action_redirect=success_action_redirect,
            policy=policy,
            signature=signature,
            username=username,
        )
