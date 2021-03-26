"""
Provides an APIView class that is the base of all views in REST framework.
"""
from django.http.response import HttpResponse

from WorkSpaceMS import settings
from base.rf.Request import Request
from django.db import connection, transaction
from django.core.exceptions import PermissionDenied
from django.utils.decorators import classonlymethod
# ----------------------------------------------------------------------------
from base.rf.settings import api_settings
from base.rf import formatting, exceptions, status
# ----------------------------------------------------------------------------
from django.utils.encoding import smart_str
from django.views.generic import View
from django.http import Http404
import asyncio

from base.rf.response import Response


def get_view_name(view):
    """
    Given a view instance, return a textual name to represent the view.
    This name is used in the browsable API, and in OPTIONS responses.

    This function is the default for the `VIEW_NAME_FUNCTION` setting.
    """
    # Name may be set by some Views, such as a ViewSet.
    name = getattr(view, 'name', None)
    if name is not None:
        return name

    name = view.__class__.__name__
    name = formatting.remove_trailing_string(name, 'View')
    name = formatting.remove_trailing_string(name, 'ViewSet')
    name = formatting.camelcase_to_spaces(name)

    # Suffix may be set by some Views, such as a ViewSet.
    suffix = getattr(view, 'suffix', None)
    if suffix:
        name += ' ' + suffix

    return name


def get_view_description(view, html=False):
    """
    Given a view instance, return a textual description to represent the view.
    This name is used in the browsable API, and in OPTIONS responses.

    This function is the default for the `VIEW_DESCRIPTION_FUNCTION` setting.
    """
    # Description may be set by some Views, such as a ViewSet.
    description = getattr(view, 'description', None)
    if description is None:
        description = view.__class__.__doc__ or ''

    description = formatting.dedent(smart_str(description))
    if html:
        return formatting.markup_description(description)
    return description


def set_rollback():
    atomic_requests = connection.settings_dict.get('ATOMIC_REQUESTS', False)
    if atomic_requests and connection.in_atomic_block:
        transaction.set_rollback(True)


async def exception_handler(exc, context):
    """
    Returns the response that should be used for any given exception.

    By default we handle the REST framework `APIException`, and also
    Django's built-in `Http404` and `PermissionDenied` exceptions.

    Any unhandled exceptions may return `None`, which will cause a 500 error
    to be raised.
    """
    if isinstance(exc, Http404):
        exc = exceptions.NotFound()
    elif isinstance(exc, PermissionDenied):
        exc = exceptions.PermissionDenied()

    if isinstance(exc, exceptions.APIException):
        headers = {}
        if getattr(exc, 'auth_header', None):
            headers['WWW-Authenticate'] = exc.auth_header
        if getattr(exc, 'wait', None):
            headers['Retry-After'] = '%d' % exc.wait

        if isinstance(exc.default_detail, (list, dict)):
            data = exc.default_detail
        else:
            data = {'detail': exc.default_detail}

        set_rollback()
        return Response(data, status=exc.status_code, headers=headers)

    return None


class AsyncAPIView(View):
    parser_classes = api_settings.DEFAULT_PARSER_CLASSES
    # renderer_classes = api_settings.DEFAULT_RENDERER_CLASSES
    # authentication_classes = api_settings.DEFAULT_AUTHENTICATION_CLASSES
    content_negotiation_class = api_settings.DEFAULT_CONTENT_NEGOTIATION_CLASS
    permission_classes = api_settings.DEFAULT_PERMISSION_CLASSES

    async def options(self, request, *args, **kwargs):
        response = HttpResponse()
        response['Allow'] = ', '.join(self.allowed_methods)
        response['Content-Length'] = '0'
        return response

    @classonlymethod
    def as_view(cls, **initkwargs):
        view = super().as_view(**initkwargs)
        view._is_coroutine = asyncio.coroutines._is_coroutine
        return view

    @property
    def allowed_methods(self):
        methods = self._allowed_methods()
        # if methods['HEAD']:
        # methods.remove('HEAD')
        return methods

    @property
    def default_response_headers(self):
        headers = {
            'Allow': ', '.join(self.allowed_methods),
        }
        # if len(self.renderer_classes) > 1:
        #     headers['Vary'] = 'Accept'
        return headers

    def http_method_not_allowed(self, request, *args, **kwargs):
        """
        If `request.method` does not correspond to a handler method,
        determine what kind of exception to raise.
        """
        raise exceptions.MethodNotAllowed(request.method)

    def permission_denied(self, request, message=None, code=None):
        """
        If request is not permitted, determine what kind of exception to raise.
        """
        if request.authenticators and not request.successful_authenticator:
            raise exceptions.NotAuthenticated()
        raise exceptions.PermissionDenied(detail=message, code=code)

    async def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        return [permission() for permission in self.permission_classes]

    async def check_permissions(self, request):
        """
        Check if the request should be permitted.
        Raises an appropriate exception if the request is not permitted.
        """
        for permission in await self.get_permissions():
            if not permission.has_permission(request, self):
                await self.permission_denied(
                    request,
                    message=getattr(permission, 'message', None),
                    code=getattr(permission, 'code', None)
                )

    def raise_uncaught_exception(self, exc):
        if settings.DEBUG:
            request = self.request
            renderer_format = getattr(request.accepted_renderer, 'format')
            use_plaintext_traceback = renderer_format not in ('html', 'api', 'admin')
            request.force_plaintext_errors(use_plaintext_traceback)
        raise exc

    def get_authenticators(self):
        """
        Instantiates and returns the list of authenticators that this view can use.
        """
        return [auth() for auth in self.authentication_classes]

    async def check_object_permissions(self, request, obj):
        """
        Check if the request should be permitted for a given object.
        Raises an appropriate exception if the request is not permitted.
        """
        for permission in await self.get_permissions():
            if not permission.has_object_permission(request, self, obj):
                self.permission_denied(
                    request,
                    message=getattr(permission, 'message', None),
                    code=getattr(permission, 'code', None)
                )

    async def initial(self, request, *args, **kwargs):
        try:
            await self.check_permissions(request)
        except Exception:
            return True

    async def dispatch(self, request, *args, **kwargs):
        """
        `.dispatch()` is pretty much the same as Django's regular dispatch,
        but with extra hooks for startup, finalize, and exception handling.
        """
        self.args = args
        self.kwargs = kwargs
        request = await self.initialize_request(request, *args, **kwargs)
        self.request = request
        self.headers = self.default_response_headers  # deprecate?

        if await self.initial(request, *args, **kwargs):
            return Response(messages=['Access denied'], status_code=401, status=status.HTTP_401_UNAUTHORIZED)

        if request.method.lower() in self.http_method_names:
            handler = getattr(self, request.method.lower(),
                              self.http_method_not_allowed)
        else:
            handler = self.http_method_not_allowed

        response = handler(request, *args, **kwargs)

        self.response = await self.finalize_response(request, response, *args, **kwargs)
        return self.response

    # def get_renderers(self):
    #     """
    #     Instantiates and returns the list of renderers that this view can use.
    #     """
    #     return [renderer() for renderer in self.renderer_classes]

    def get_parsers(self):
        """
        Instantiates and returns the list of parsers that this view can use.
        """
        return [parser() for parser in self.parser_classes]

    def perform_content_negotiation(self, request, force=False):
        """
        Determine which renderer and media type to use render the response.
        """
        # renderers = self.get_renderers()
        self.get_content_negotiator()

        # try:
        #     return conneg.select_renderer(request, renderers, self.format_kwarg)
        # except Exception:
        #     if force:
        #         return (renderers[0], renderers[0].media_type)
        #     raise

    def get_content_negotiator(self):
        """
        Instantiate and return the content negotiation class to use.
        """
        if not getattr(self, '_negotiator', None):
            self._negotiator = self.content_negotiation_class()
        return self._negotiator

    async def initialize_request(self, request, *args, **kwargs):
        """
        Returns the initial request object.
        """
        # parser_context = self.get_parser_context(request)
        request._user = request.user
        return Request(
            request,
            parsers=self.get_parsers(),
            negotiator=self.get_content_negotiator(),
            # parser_context=parser_context
        )

    async def finalize_response(self, request, response, *args, **kwargs):
        """
        Returns the final response object.
        """
        # Make the error obvious if a proper response is not returned
        # assert isinstance(response, HttpResponseBase), (
        #     'Expected a `Response`, `HttpResponse` or `HttpStreamingResponse` '
        #     'to be returned from the view, but received a `%s`'
        #     % type(response)
        # )

        # if isinstance(response, Response):
        # if not getattr(request, 'accepted_renderer', None):
        # neg = self.perform_content_negotiation(request, force=True)
        # request.accepted_renderer, request.accepted_media_type = neg

        # response.accepted_renderer = request.accepted_renderer
        # response.accepted_media_type = request.accepted_media_type
        # response.renderer_context = self.get_renderer_context()

        # Add new vary headers to the response instead of overwriting.
        # vary_headers = self.headers.pop('Vary', None)
        # if vary_headers is not None:
        #     patch_vary_headers(response, cc_delim_re.split(vary_headers))

        # for key, value in self.headers.items():
        #     resp[key] = value
        return await response

#
#
#
# from rest_framework.response import Response
# from rest_framework import status
#
# from asgiref.sync import sync_to_async
# import asyncio as aio
#
#
# class AsyncMixin:
#     """Provides async view compatible support for DRF Views and ViewSets.
#
#     This must be the first inherited class.
#
#         class MyViewSet(AsyncMixin, GenericViewSet):
#             pass
#     """
#
#     @classmethod
#     def as_view(cls, *args, **initkwargs):
#         """Make Django process the view as an async view.
#         """
#         view = super().as_view(*args, **initkwargs)
#
#         async def async_view(*args, **kwargs):
#             # wait for the `dispatch` method
#             return await view(*args, **kwargs)
#
#         async_view.csrf_exempt = True
#         return async_view
#
#     async def dispatch(self, request, *args, **kwargs):
#         """Add async support.
#         """
#         self.args = args
#         self.kwargs = kwargs
#         request = self.initialize_request(request, *args, **kwargs)
#         self.request = request
#         self.headers = self.default_response_headers
#
#         try:
#             await sync_to_async(self.initial)(
#                 request, *args, **kwargs)  # MODIFIED HERE
#
#             if request.method.lower() in self.http_method_names:
#                 handler = getattr(self, request.method.lower(),
#                                   self.http_method_not_allowed)
#             else:
#                 handler = self.http_method_not_allowed
#
#             # accept both async and sync handlers
#             # built-in handlers are sync handlers
#             if not aio.iscoroutinefunction(handler):  # MODIFIED HERE
#                 handler = sync_to_async(handler)  # MODIFIED HERE
#             response = await handler(request, *args, **kwargs)  # MODIFIED HERE
#
#         except Exception as exc:
#             response = self.handle_exception(exc)
#
#         self.response = self.finalize_response(
#             request, response, *args, **kwargs)
#         return self.response
#
#
# class AsyncCreateModelMixin:
#     """Make `create()` and `perform_create()` overridable.
#
#     Without inheriting this class, the event loop can't be used in these two methods when override them.
#
#     This must be inherited before `CreateModelMixin`.
#
#         class MyViewSet(AsyncMixin, GenericViewSet, AsyncCreateModelMixin, CreateModelMixin):
#             pass
#     """
#
#     async def create(self, request, *args, **kwargs):
#         serializer = self.get_serializer(data=request.data)
#         await sync_to_async(serializer.is_valid)(
#             raise_exception=True)  # MODIFIED HERE
#         await self.perform_create(serializer)  # MODIFIED HERE
#         headers = self.get_success_headers(serializer.data)
#         return Response(serializer.data, status=status.HTTP_201_CREATED, headers=headers)
#
#     async def perform_create(self, serializer):
#         await sync_to_async(serializer.save)()
#
#
# class AsyncDestroyModelMixin:
#     """Make `destroy()` and `perform_destroy()` overridable.
#
#     Without inheriting this class, the event loop can't be used in these two methods when override them.
#
#     This must be inherited before `DestroyModelMixin`.
#
#         class MyViewSet(AsyncMixin, GenericViewSet, AsyncDestroyModelMixin, DestroyModelMixin):
#             pass
#     """
#
#     async def destroy(self, request, *args, **kwargs):
#         instance = await sync_to_async(self.get_object)()  # MODIFIED HERE
#         await self.perform_destroy(instance)  # MODIFIED HERE
#         return Response(status=status.HTTP_204_NO_CONTENT)
#
#     async def perform_destroy(self, instance):
#         await sync_to_async(instance.delete)()  # MODIFIED HERE
