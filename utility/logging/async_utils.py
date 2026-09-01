import contextvars
from concurrent.futures import ThreadPoolExecutor


class ContextAwareThreadPoolExecutor(ThreadPoolExecutor):
    def submit(self, fn, /, *args, **kwargs):
        ctx = contextvars.copy_context()
        return super().submit(ctx.run, fn, *args, **kwargs)