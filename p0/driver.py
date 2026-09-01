"""p0's single driver entry point — selects the backend driver by DEPLOYMENT_MODE.
"""

import os as _os

_MODE = (_os.getenv("DEPLOYMENT_MODE") or "onprem").strip().lower().replace("-", "_")

if _MODE == "azure":
    from p0.drivers import azure_driver as _driver
elif _MODE == "aws":
    from p0.drivers import aws_driver as _driver
else:
    from p0.drivers import database_driver as _driver


def __getattr__(name):
    try:
        return getattr(_driver, name)
    except AttributeError:
        from p0.drivers import iotdb_helpers as _helpers

        return getattr(_helpers, name)
