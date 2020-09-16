import asyncio
import functools
import ipaddress
import itertools
import logging
from asyncio import AbstractEventLoop
from contextvars import ContextVar  # noqa
from typing import Union

from sanic.exceptions import Unauthorized
from sanic.request import Request
from sanic.response import json as json_response, text as text_response
from sanic.views import HTTPMethodView
from sanic.log import logger as sanic_logger

from .... import Bot, Dispatcher
from ....dispatcher.webhook import BaseResponse
from ....types import Update
from ....utils.deprecated import warn_deprecated
from ....utils.exceptions import TimeoutWarning
from ....utils.json import json

RESPONSE_TIMEOUT = 55
TELEGRAM_SUBNET_1 = ipaddress.IPv4Network('149.154.160.0/20')
TELEGRAM_SUBNET_2 = ipaddress.IPv4Network('91.108.4.0/22')
allowed_ips = set()

log = logging.getLogger(__name__)


class SanicWebhookRequestHandler(HTTPMethodView):
    _dp: Dispatcher = None
    ctx_request: ContextVar[Request] = ContextVar('webhook_request', default=None)

    @property
    def request(self):
        return self.ctx_request.get()

    @property
    def dp(self) -> Dispatcher:
        if isinstance(self._dp, Dispatcher):
            return self._dp
        self._dp = Dispatcher.get_current()
        return self._dp

    @property
    def bot(self) -> Bot:
        return self.dp.bot

    @property
    def loop(self) -> AbstractEventLoop:
        return asyncio.get_event_loop()

    @property
    def config(self) -> dict:
        return self.request.app.config

    async def post(self, request: Request):
        """
        Process POST request

        If one of handler returns instance of
        :class:`aiogram.dispatcher.webhook.BaseResponse` return it to webhook.
        Otherwise do nothing (return 'ok')

        :return: :class:`aiohttp.web.Response`
        """
        sanic_logger.warning(f'Received request: {request}')
        self.ctx_request.set(request)
        self.validate_ip()

        update = await self.parse_update()
        log.warning(f'Update: {update}')
        results = await self.process_update(update)
        log.warning(f'Results: {results}')

        response = self.get_response(results)
        headers = None
        if self.config.get('RETRY_AFTER', None):
            headers = {'Retry-After': self.config['RETRY_AFTER']}

        if response:
            return json_response(response.get_response(),
                                 dumps=json.dumps, headers=headers)
        return text_response('OK')

    def validate_ip(self):
        """
        Check ip if that is needed. Raise web.HTTPUnauthorized for not allowed hosts.
        """
        if self.config.get('_check_ip', False):
            ip_address, accept = self.check_ip()
            if not accept:
                log.warning(f"Blocking request from an unauthorized IP: {ip_address}")
                raise Unauthorized("Unauthorized IP")

    def check_ip(self):
        """
        Check client IP. Accept requests only from telegram servers.

        :return:
        """
        # For reverse proxy (nginx)
        forwarded_for = self.request.headers.get('X-Forwarded-For', None)
        if forwarded_for:
            return forwarded_for, self._check_ip(forwarded_for)

        # For default method
        peer_name = self.request.transport.get_extra_info('peername')
        if peer_name is not None:
            host, _ = peer_name
            return host, self._check_ip(host)

        # Not allowed and can't get client IP
        return None, False

    @staticmethod
    def _check_ip(ip: str) -> bool:
        """
        Check IP in range

        :param ip:
        :return:
        """
        address = ipaddress.IPv4Address(ip)
        return address in allowed_ips

    @staticmethod
    def allow_ip(*ips: Union[str, ipaddress.IPv4Network, ipaddress.IPv4Address]):
        """
        Allow ip address.

        :param ips:
        :return:
        """
        for ip in ips:
            if isinstance(ip, ipaddress.IPv4Address):
                allowed_ips.add(ip)
            elif isinstance(ip, str):
                allowed_ips.add(ipaddress.IPv4Address(ip))
            elif isinstance(ip, ipaddress.IPv4Network):
                allowed_ips.update(ip.hosts())
            else:
                raise ValueError(f"Bad type of ipaddress: {type(ip)} ('{ip}')")

    async def parse_update(self):
        """
        Read update from stream and deserialize it.

        :return: :class:`aiogram.types.Update`
        """
        return Update(**self.request.json)

    async def process_update(self, update):
        """
        Need respond in less than 60 seconds in to webhook.

        So... If you respond greater than 55 seconds webhook automatically respond 'ok'
        and execute callback response via simple HTTP request.

        :param update:
        :return:
        """
        # Analog of `asyncio.wait_for` but without cancelling task
        loop = asyncio.get_event_loop()
        waiter = loop.create_future()
        timeout_handle = loop.call_later(RESPONSE_TIMEOUT, asyncio.tasks._release_waiter, waiter)  # noqa
        cb = functools.partial(asyncio.tasks._release_waiter, waiter)  # noqa

        fut = asyncio.create_task(self.dp.updates_handler.notify(update))
        fut.add_done_callback(cb)

        try:
            try:
                await waiter
            except asyncio.CancelledError:
                fut.remove_done_callback(cb)
                fut.cancel()
                raise

            if fut.done():
                return fut.result()
            else:
                fut.remove_done_callback(cb)
                fut.add_done_callback(self.respond_via_request)
        finally:
            timeout_handle.cancel()

    def respond_via_request(self, task):
        """
        Handle response after 55 second.

        :param task:
        :return:
        """
        warn_deprecated(
            f"Detected slow response into webhook. "
            f"(Greater than {RESPONSE_TIMEOUT} seconds)\n"
            f"Recommended to use 'async_task' decorator from Dispatcher for handler "
            f"with long timeouts.",
            TimeoutWarning
        )

        loop = asyncio.get_event_loop()

        try:
            results = task.result()
        except Exception as e:
            loop.create_task(
                self.dp.errors_handlers.notify(self.dp, Update.get_current(), e))
        else:
            response = self.get_response(results)
            if response is not None:
                asyncio.ensure_future(response.execute_response(self.bot), loop=loop)

    @staticmethod
    def get_response(results):
        """
        Get response object from results.

        :param results: list
        :return:
        """
        if results is None:
            return None
        for result in itertools.chain.from_iterable(results):
            if isinstance(result, BaseResponse):
                return result
