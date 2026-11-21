"""Stub DB driver modules, and fastapi where it is not installed, for jobs tests.

The jobs subsystem imports psycopg, mysql.connector, pymongo, and fastapi at
module load time. The drivers are not in requirements-test.txt; CI installs only
the test deps and runs pytest. Stubbing keeps the unit tests light — anything
that actually needs to call the driver should mock at a higher level.

FastAPI is in requirements-test.txt (the web UI's tests need it), so CI runs the jobs tests against the real one; an environment without it gets the stand-ins below.
"""

from __future__ import annotations

import importlib.util
import sys
from unittest.mock import MagicMock

# DB drivers: any attribute access returns a MagicMock, which is enough for
# `import psycopg; psycopg.errors.QueryCanceled` style references at module load.
sys.modules.setdefault("psycopg", MagicMock())
sys.modules.setdefault("psycopg.errors", MagicMock())
sys.modules.setdefault("mysql", MagicMock())
sys.modules.setdefault("mysql.connector", MagicMock())
sys.modules.setdefault("mysql.connector.errors", MagicMock())

_pymongo = MagicMock()
sys.modules.setdefault("pymongo", _pymongo)
# pymongo.errors classes need to be exception types so `except` works.
_pm_errors = MagicMock()
_pm_errors.PyMongoError = type("PyMongoError", (Exception,), {})
_pm_errors.ExecutionTimeout = type("ExecutionTimeout", (Exception,), {})
_pm_errors.OperationFailure = type("OperationFailure", (Exception,), {})
sys.modules.setdefault("pymongo.errors", _pm_errors)

# sqlparse is imported by jobs/web.py for SQL pretty-printing in templates.
sys.modules.setdefault("sqlparse", MagicMock())

# fastapi is imported by jobs/auth.py for type references. The stand-ins are installed only where FastAPI is not installed: where it is (the web UI's tests need the real one), the jobs tests run against it, and a stand-in put into sys.modules here would replace it for every test collected after this folder.
if importlib.util.find_spec("fastapi") is None:
    _fastapi = MagicMock()
    _fastapi.HTTPException = type("HTTPException", (Exception,), {
        "__init__": lambda self, status_code=None, headers=None, detail=None: (
            setattr(self, "status_code", status_code),
            setattr(self, "headers", headers),
            setattr(self, "detail", detail),
            None,
        )[-1],
    })
    _fastapi.Request = MagicMock
    sys.modules.setdefault("fastapi", _fastapi)
    sys.modules.setdefault("fastapi.responses", MagicMock())
    sys.modules.setdefault("fastapi.templating", MagicMock())

# mcp SDK is imported by jobs/mcp_tools.py for tool registration. Tests that
# need real registration behavior must run inside the jobs container; the
# stubs here only let pure-helper tests (like the CSV pre-check) import the
# module without dragging in the SDK.
_mcp = MagicMock()
sys.modules.setdefault("mcp", _mcp)
sys.modules.setdefault("mcp.server", MagicMock())
sys.modules.setdefault("mcp.server.mcpserver", MagicMock())
sys.modules.setdefault("mcp.server.transport_security", MagicMock())
