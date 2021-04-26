import asyncio
import os
import warnings
import json
from types import SimpleNamespace
import jwt
from base64 import b64decode

from django.utils.decorators import sync_and_async_middleware
from django import http
from django.utils.cache import patch_vary_headers
from django.utils.deprecation import RemovedInDjango40Warning

from urllib.parse import urlparse

from base.aes import AES
from base.rf.response import Response
from base.rf import status

from .cors_deafults import *

AES_KEY = bytes(os.getenv('AES_KEY').encode('utf-8'))
JWT_PUBLIC = os.getenv('JWT_PUBLIC').replace('\\n', '\n').strip().replace('"', '')
JWT_ALGORITHM = os.getenv('JWT_ALGORITHM')

ACCESS_CONTROL_ALLOW_ORIGIN = 'Access-Control-Allow-Origin'
ACCESS_CONTROL_EXPOSE_HEADERS = 'Access-Control-Expose-Headers'
ACCESS_CONTROL_ALLOW_CREDENTIALS = 'Access-Control-Allow-Credentials'
ACCESS_CONTROL_ALLOW_HEADERS = 'Access-Control-Allow-Headers'
ACCESS_CONTROL_ALLOW_METHODS = 'Access-Control-Allow-Methods'
ACCESS_CONTROL_MAX_AGE = 'Access-Control-Max-Age'


def get_data(auth):
    if auth:
        auth = auth.strip()
        auth = auth.split(' ')[-1]
        jwt_token = auth
        payload = jwt.decode(jwt_token, JWT_PUBLIC,
                             algorithms=[JWT_ALGORITHM])
        data = b64decode(payload['data'])
        iv = b64decode(payload['identifier'])

        dt = AES(AES_KEY).decrypt_ctr(data, iv)
        dt = dt.replace("'", '"').replace("None", "null")
        data1 = json.loads(dt, object_hook=lambda e: SimpleNamespace(**e))
        return data1
    return None


def authorize(request):
    access_token = request.headers.get('Authorization', None)
    if access_token:
        try:
            user_data = get_data(access_token)
        except jwt.exceptions.InvalidSignatureError as e:
            return Response(messages=[{'message': "Invalid Signature Error"}],
                            status_code=401
                            , status=status.HTTP_401_UNAUTHORIZED)
        except jwt.exceptions.ExpiredSignatureError as e:
            return Response(messages=[{'message': 'Signature is Expired'}], status_code=401,
                            status=status.HTTP_401_UNAUTHORIZED)
        except Exception:
            return Response(messages=[{'message': 'Invalid Token'}], status_code=401,
                            status=status.HTTP_401_UNAUTHORIZED)
        request.user = user_data
        request.user.is_authenticated = True
    else:
        request.user = None


@sync_and_async_middleware
def auth_middleware(get_response):
    """
        check token exists and decode token and get data for authorization
    """
    if asyncio.iscoroutinefunction(get_response):
        async def middleware(request):
            request = authorize(request)
            if isinstance(request, Response):
                return request
            response = await get_response(request)
            return response

    else:

        def middleware(request):
            request = authorize(request)
            if isinstance(request, Response):
                return request
            response = get_response(request)
            return response

    return middleware


AuthMiddleware = auth_middleware


class CORSMiddleWare:
    sync_capable = True
    async_capable = True

    # RemovedInDjango40Warning: when the deprecation ends, replace with:
    #   def __init__(self, get_response):
    def __init__(self, get_response=None):
        self._get_response_none_deprecation(get_response)
        self.get_response = get_response
        self._async_check()
        super().__init__()

    def _async_check(self):
        """
        If get_response is a coroutine function, turns us into async mode so
        a thread is not consumed during a whole request.
        """
        if asyncio.iscoroutinefunction(self.get_response):
            # Mark the class as async-capable, but do the actual switch
            # inside __call__ to avoid swapping out dunder methods
            self._is_coroutine = asyncio.coroutines._is_coroutine

    def __call__(self, request):
        # Exit out to async mode, if needed
        if asyncio.iscoroutinefunction(self.get_response):
            return self.__acall__(request)
        response = None
        if hasattr(self, 'process_request'):
            response = self.process_request(request)
        response = response or self.get_response(request)
        if hasattr(self, 'process_response'):
            response = self.process_response(request, response)
        return response

    async def __acall__(self, request):
        """
        Async version of __call__ that is swapped in when an async request
        is running.
        """
        response = None
        if hasattr(self, 'async_process_request'):
            response = await self.async_process_request(request)
        response = response or await self.get_response(request)
        if hasattr(self, 'async_process_response'):
            response = await self.async_process_response(request, response)
        return response

    def _get_response_none_deprecation(self, get_response):
        if get_response is None:
            warnings.warn(
                'Passing None for the middleware get_response argument is '
                'deprecated.',
                RemovedInDjango40Warning, stacklevel=3,
            )

    def _https_referer_replace(self, request):
        """
        When https is enabled, django CSRF checking includes referer checking
        which breaks when using CORS. This function updates the HTTP_REFERER
        header to make sure it matches HTTP_HOST, provided that our cors logic
        succeeds
        """
        origin = request.META.get('HTTP_ORIGIN')

        if (request.is_secure() and origin and
                'ORIGINAL_HTTP_REFERER' not in request.META):
            url = urlparse(origin)
            if (not CORS_ORIGIN_ALLOW_ALL and
                    self.origin_not_found_in_white_lists(origin, url)):
                return

            try:
                http_referer = request.META['HTTP_REFERER']
                http_host = "https://%s/" % request.META['HTTP_HOST']
                request.META = request.META.copy()
                request.META['ORIGINAL_HTTP_REFERER'] = http_referer
                request.META['HTTP_REFERER'] = http_host
            except KeyError:
                pass

    def process_request(self, request):
        """
        If CORS preflight header, then create an
        empty body response (200 OK) and return it
        Django won't bother calling any other request
        view/exception middleware along with the requested view;
        it will call any response middlewares
        """
        if CORS_REPLACE_HTTPS_REFERER:
            self._https_referer_replace(request)

        if (request.method == 'OPTIONS' and
                "HTTP_ACCESS_CONTROL_REQUEST_METHOD" in request.META):
            response = http.HttpResponse()
            return response
        return None

    # def process_view(self, request, callback, callback_args, callback_kwargs):
    #     """
    #     Do the referer replacement here as well
    #     """
    #     if CORS_REPLACE_HTTPS_REFERER:
    #         self._https_referer_replace(request)
    #     return None

    def process_response(self, request, response):
        """
        Add the respective CORS headers
        """
        origin = request.META.get('HTTP_ORIGIN')
        if origin:
            # todo: check hostname from db instead
            url = urlparse(origin)

            if CORS_MODEL is not None:
                response[ACCESS_CONTROL_ALLOW_ORIGIN] = origin

            if (not CORS_ORIGIN_ALLOW_ALL and
                    self.origin_not_found_in_white_lists(origin, url)):
                return response

            if CORS_ORIGIN_ALLOW_ALL and not CORS_ALLOW_CREDENTIALS:
                response[ACCESS_CONTROL_ALLOW_ORIGIN] = "*"
            else:
                response[ACCESS_CONTROL_ALLOW_ORIGIN] = origin
                patch_vary_headers(response, ['Origin'])

            if len(CORS_EXPOSE_HEADERS):
                response[ACCESS_CONTROL_EXPOSE_HEADERS] = ', '.join(
                    CORS_EXPOSE_HEADERS)

            if CORS_ALLOW_CREDENTIALS:
                response[ACCESS_CONTROL_ALLOW_CREDENTIALS] = 'true'

            if request.method == 'OPTIONS':
                response[ACCESS_CONTROL_ALLOW_HEADERS] = ', '.join(
                    CORS_ALLOW_HEADERS)
                response[ACCESS_CONTROL_ALLOW_METHODS] = ', '.join(
                    CORS_ALLOW_METHODS)
                if CORS_PREFLIGHT_MAX_AGE:
                    response[ACCESS_CONTROL_MAX_AGE] = \
                        CORS_PREFLIGHT_MAX_AGE
        return response

    async def async_process_response(self, request, response):
        """
        Add the respective CORS headers
        """
        origin = request.META.get('HTTP_ORIGIN')
        if origin:
            # todo: check hostname from db instead
            response[ACCESS_CONTROL_ALLOW_ORIGIN] = origin

            if (not CORS_ORIGIN_ALLOW_ALL and
                    self.origin_not_found_in_white_lists(origin, origin)):
                return response

            if CORS_ORIGIN_ALLOW_ALL and not CORS_ALLOW_CREDENTIALS:
                response[ACCESS_CONTROL_ALLOW_ORIGIN] = "*"
            else:
                response[ACCESS_CONTROL_ALLOW_ORIGIN] = origin
                patch_vary_headers(response, ['Origin'])

            if len(CORS_EXPOSE_HEADERS):
                response[ACCESS_CONTROL_EXPOSE_HEADERS] = ', '.join(
                    CORS_EXPOSE_HEADERS)

            if CORS_ALLOW_CREDENTIALS:
                response[ACCESS_CONTROL_ALLOW_CREDENTIALS] = 'true'

            if request.method == 'OPTIONS':
                response[ACCESS_CONTROL_ALLOW_HEADERS] = ', '.join(
                    CORS_ALLOW_HEADERS)
                response[ACCESS_CONTROL_ALLOW_METHODS] = ', '.join(
                    CORS_ALLOW_METHODS)
                if CORS_PREFLIGHT_MAX_AGE:
                    response[ACCESS_CONTROL_MAX_AGE] = \
                        CORS_PREFLIGHT_MAX_AGE
        return response

    def origin_not_found_in_white_lists(self, origin, url):
        return url.netloc not in CORS_ORIGIN_WHITELIST

    async def async_process_request(self, request):
        """
                If CORS preflight header, then create an
                empty body response (200 OK) and return it
                Django won't bother calling any other request
                view/exception middleware along with the requested view;
                it will call any response middlewares
                """
        if CORS_REPLACE_HTTPS_REFERER:
            self._https_referer_replace(request)

        if (request.method == 'OPTIONS' and
                "HTTP_ACCESS_CONTROL_REQUEST_METHOD" in request.META):
            response = http.HttpResponse()
            return response
        return None
