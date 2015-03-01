"""Cloud browser URLs."""
from django.conf.urls import patterns, url
from django.views.generic.base import RedirectView

from cloud_browser.app_settings import settings
from cloud_browser.views import UploadFileView, MkdirView, DeleteView, \
    RenameView, MoveFileView

# pylint: disable=invalid-name, no-value-for-parameter
urlpatterns = patterns(
    'cloud_browser.views',
    url(r'^$',
        RedirectView.as_view(url='browser'),
        name="cloud_browser_index"),
    url(r'^browser/(?P<path>.*)$', 'browser', name="cloud_browser_browser"),
    url(r'^document/(?P<path>.*)$', 'document', name="cloud_browser_document"),
    url(r'^upload/$', UploadFileView.as_view(), name='upload'),
    url(r'^mkdir/$', MkdirView.as_view(), name='mkdir'),
    url(r'^delete/$', DeleteView.as_view(), name='delete'),
    url(r'^rename/$', RenameView.as_view(), name='rename'),
    url(r'^move/$', MoveFileView.as_view(), name='move'),
)

if settings.app_media_url is None:
    # Use a static serve.
    urlpatterns += patterns(
        '',
        url(r'^app_media/(?P<path>.*)$', 'django.views.static.serve',
            {'document_root': settings.app_media_doc_root},
            name="cloud_browser_media"),
    )
