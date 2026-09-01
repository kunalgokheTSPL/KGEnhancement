from contextvars import ContextVar

plant_code_ctx = ContextVar("plant_code_ctx", default=None)


class DatabaseNotFoundError(Exception):
    """Raised when the target tenant/plant database does not exist."""
    pass
