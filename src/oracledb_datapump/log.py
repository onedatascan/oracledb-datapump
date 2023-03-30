import logging

DEFAULT_LOG_FMT = "%(levelname)s:%(asctime)s:%(name)s:ctx=%(ctx)s::%(message)s"


class ContextLogAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        ctx = kwargs.pop("ctx", None)
        kwargs["extra"] = dict(ctx=ctx)
        return msg, kwargs


def get_logger(name: str) -> logging.LoggerAdapter:
    logger = logging.getLogger(name)
    return ContextLogAdapter(logger)
