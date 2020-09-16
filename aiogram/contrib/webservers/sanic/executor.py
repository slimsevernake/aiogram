import asyncio
import functools
from asyncio import AbstractEventLoop
from datetime import datetime
from typing import Optional, Any, Callable, Union

from sanic import Sanic

from .webhook import SanicWebhookRequestHandler
from .... import Dispatcher
from ....dispatcher.webhook import DEFAULT_ROUTE_NAME, BOT_DISPATCHER_KEY
from ....utils.executor import Executor, APP_EXECUTOR_KEY


def _setup_callbacks(executor: 'SanicExecutor', on_startup=None, on_shutdown=None):
    if on_startup is not None:
        executor.on_startup(on_startup)
    if on_shutdown is not None:
        executor.on_shutdown(on_shutdown)


def start_polling(dispatcher, *, loop=None, skip_updates=False, reset_webhook=True,
                  on_startup=None, on_shutdown=None, timeout=20, relax=0.1, fast=True):
    """
    Start bot in long-polling mode


    :param dispatcher:
    :param loop:
    :param skip_updates:
    :param reset_webhook:
    :param on_startup:
    :param on_shutdown:
    :param timeout:
    :param relax:
    :param fast:
    """
    executor = SanicExecutor(dispatcher, skip_updates=skip_updates, loop=loop)
    _setup_callbacks(executor, on_startup, on_shutdown)

    executor.start_polling(
        reset_webhook=reset_webhook,
        timeout=timeout,
        relax=relax,
        fast=fast
    )


def set_webhook(
        dispatcher: Dispatcher,
        webhook_path: str,
        *,
        loop: Optional[AbstractEventLoop] = None,
        skip_updates: bool = None,
        on_startup: Optional[Callable] = None,
        on_shutdown: Optional[Callable] = None,
        check_ip: bool = False,
        retry_after: Optional[Union[str, int]] = None,
        route_name: str = DEFAULT_ROUTE_NAME,
        web_app: Optional[Sanic] = None):
    """
    Set webhook for bot

    :param dispatcher: Dispatcher
    :param webhook_path: str
    :param loop: Optional[asyncio.AbstractEventLoop] (default: None)
    :param skip_updates: bool (default: None)
    :param on_startup: Optional[Callable] (default: None)
    :param on_shutdown: Optional[Callable] (default: None)
    :param check_ip: bool (default: False)
    :param retry_after: Optional[Union[str, int]]
        See https://tools.ietf.org/html/rfc7231#section-7.1.3 (default: None)
    :param route_name: str (default: 'webhook_handler')
    :param web_app: Optional[Application] (default: None)
    :return:
    """
    executor = SanicExecutor(
        dispatcher,
        skip_updates=skip_updates,
        check_ip=check_ip,
        retry_after=retry_after,
        loop=loop
    )
    _setup_callbacks(executor, on_startup, on_shutdown)

    executor.set_webhook(webhook_path, route_name=route_name, web_app=web_app)
    return executor


def start_webhook(
        dispatcher: Dispatcher,
        webhook_path: str,
        *,
        loop: Optional[AbstractEventLoop] = None,
        skip_updates: bool = None,
        on_startup: Optional[Callable] = None,
        on_shutdown: Optional[Callable] = None,
        check_ip: bool = False,
        retry_after: Optional[Union[str, int]] = None,
        route_name: str = DEFAULT_ROUTE_NAME,
        **kwargs):
    """
    Start bot in webhook mode

    :param dispatcher:
    :param webhook_path:
    :param loop:
    :param skip_updates:
    :param on_startup:
    :param on_shutdown:
    :param check_ip:
    :param retry_after:
    :param route_name:
    :param kwargs:
    :return:
    """
    executor = set_webhook(dispatcher=dispatcher,
                           webhook_path=webhook_path,
                           loop=loop,
                           skip_updates=skip_updates,
                           on_startup=on_startup,
                           on_shutdown=on_shutdown,
                           check_ip=check_ip,
                           retry_after=retry_after,
                           route_name=route_name)
    executor.run_app(**kwargs)


def start(dispatcher, future, *, loop=None, skip_updates=None,
          on_startup=None, on_shutdown=None):
    """
    Execute Future.

    :param dispatcher: instance of Dispatcher
    :param future: future
    :param loop: instance of AbstractEventLoop
    :param skip_updates:
    :param on_startup:
    :param on_shutdown:
    :return:
    """
    executor = SanicExecutor(dispatcher, skip_updates=skip_updates, loop=loop)
    _setup_callbacks(executor, on_startup, on_shutdown)

    return executor.start(future)


class SanicExecutor(Executor):

    @property
    def loop(self) -> AbstractEventLoop:
        return asyncio.get_event_loop()

    @property
    def frozen(self):
        return self._freeze

    def set_web_app(self, application: Sanic):
        """
        Change instance of Sanic

        :param application: sanic instance
        """
        self._web_app = application

    @property
    def web_app(self) -> Sanic:
        if self._web_app is None:
            raise RuntimeError('Sanic() is not configured!')
        return self._web_app

    def _prepare_webhook(
            self,
            path=None,
            handler=SanicWebhookRequestHandler,
            route_name=DEFAULT_ROUTE_NAME,
            app=None
    ):
        self._check_frozen()
        self._freeze = True

        if app is not None:
            self._web_app = app
        elif self._web_app is None:
            self._web_app = app = Sanic('webapp', configure_logging=False)
        else:
            raise RuntimeError("Sanic() is already configured!")

        if self.retry_after:
            app.config['RETRY_AFTER'] = self.retry_after

        if self._identity == app.config.get(self._identity):
            # App is already configured
            return

        if path is not None:
            app.add_route(handler=handler.as_view(),
                          uri=path,
                          methods={"POST"},
                          name=route_name)

        async def _wrap_callback(cb, _):
            return await cb(self.dispatcher)

        startup_tasks = []
        for callback in self._on_startup_webhook:
            startup_tasks.append(functools.partial(_wrap_callback, callback))

        async def _on_startup(sanic: Sanic, __):
            for task in startup_tasks:
                sanic.add_task(task)

        async def _on_shutdown(_, __):
            await self._shutdown_webhook()

        app.register_listener(_on_startup, 'before_server_start')
        app.register_listener(_on_shutdown, 'after_server_stop')
        app.config[APP_EXECUTOR_KEY] = self
        app.config[BOT_DISPATCHER_KEY] = self.dispatcher
        app.config[self._identity] = datetime.now()
        app.config['_check_ip'] = self.check_ip

    def set_webhook(
            self,
            webhook_path: Optional[str] = None,
            request_handler: Any = SanicWebhookRequestHandler,
            route_name: str = DEFAULT_ROUTE_NAME,
            web_app: Optional[Sanic] = None
    ):
        """
        Set webhook for bot

        :param webhook_path: Optional[str] (default: None)
        :param request_handler: Any (default: SanicWebhookRequestHandler)
        :param route_name: str Name of webhook handler route (default: 'webhook_handler')
        :param web_app: Optional[Sanic] (default: None)
        :return:
        """
        self._prepare_webhook(webhook_path, request_handler, route_name, web_app)
        self.loop.run_until_complete(self._startup_webhook())

    def run_app(self, **kwargs):
        self._web_app.run(*kwargs, access_log=False)

    def start_webhook(
            self,
            webhook_path=None,
            request_handler=SanicWebhookRequestHandler,
            route_name=DEFAULT_ROUTE_NAME,
            **kwargs
    ):
        """
        Start bot in webhook mode

        :param webhook_path:
        :param request_handler:
        :param route_name: Name of webhook handler route
        :param kwargs:
        :return:
        """
        self.set_webhook(
            webhook_path=webhook_path,
            request_handler=request_handler,
            route_name=route_name
        )
        self.run_app(**kwargs)
