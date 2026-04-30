"""Database connection and assertion helpers for E2E tests.

Provides simple wrappers around psycopg and pymongo for verifying
pipeline results in PostgreSQL and MongoDB.
"""


def pg_connect(port=5432, dbname="datasets", user="postgres", password=None):
    """Create a psycopg connection to PostgreSQL.

    Connects via localhost (the inner docker network publishes ports
    to the sysbox container's loopback).

    Args:
        port: PostgreSQL port.
        dbname: Database name.
        user: Database user.
        password: Optional password.

    Returns:
        psycopg connection object.
    """
    import psycopg

    conninfo = f"host=localhost port={port} dbname={dbname} user={user}"
    if password:
        conninfo += f" password={password}"
    return psycopg.connect(conninfo)


def pg_query_scalar(conn, query, params=None):
    """Execute a query and return the first column of the first row."""
    result = conn.execute(query, params)
    row = result.fetchone()
    return row[0] if row else None


def pg_table_exists(conn, schema, table):
    """Check if a table exists in a given schema."""
    return pg_query_scalar(
        conn,
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s)",
        (schema, table),
    )


def pg_row_count(conn, schema, table):
    """Return row count for a table."""
    return pg_query_scalar(conn, f'SELECT count(*) FROM "{schema}"."{table}"')


def pg_index_count(conn, schema):
    """Return number of indexes in a schema (excluding PK)."""
    return pg_query_scalar(
        conn,
        "SELECT count(*) FROM pg_indexes WHERE schemaname = %s",
        (schema,),
    )


def pg_table_has_pk(conn, schema, table):
    """Return True if a table has a PRIMARY KEY constraint."""
    return pg_query_scalar(
        conn,
        "SELECT EXISTS("
        " SELECT 1 FROM pg_constraint c"
        " JOIN pg_class t ON t.oid = c.conrelid"
        " JOIN pg_namespace n ON n.oid = t.relnamespace"
        " WHERE c.contype = 'p' AND n.nspname = %s AND t.relname = %s)",
        (schema, table),
    )


def pg_drop_pk(conn, schema, table):
    """Drop the PRIMARY KEY constraint on a table.

    Used by E2E tests to simulate a half-finalized fast-load (the table
    exists with data but the PK was never added because the prior run
    crashed between blind COPY and ALTER TABLE ADD PRIMARY KEY).
    """
    pk_name = pg_query_scalar(
        conn,
        "SELECT c.conname FROM pg_constraint c"
        " JOIN pg_class t ON t.oid = c.conrelid"
        " JOIN pg_namespace n ON n.oid = t.relnamespace"
        " WHERE c.contype = 'p' AND n.nspname = %s AND t.relname = %s",
        (schema, table),
    )
    if pk_name is None:
        return
    conn.execute(f'ALTER TABLE "{schema}"."{table}" DROP CONSTRAINT "{pk_name}"')
    conn.commit()


def mongo_connect(port=27017, username=None, password=None):
    """Create a pymongo MongoClient.

    Args:
        port: MongoDB port.
        username: Optional username.
        password: Optional password.

    Returns:
        pymongo.MongoClient instance.
    """
    from pymongo import MongoClient

    if username and password:
        uri = f"mongodb://{username}:{password}@localhost:{port}/?authSource=admin"
    else:
        uri = f"mongodb://localhost:{port}/"
    return MongoClient(uri)


def mongo_doc_count(client, db_name, collection_name):
    """Return document count for a collection."""
    return client[db_name][collection_name].count_documents({})


def mongo_collection_exists(client, db_name, collection_name):
    """Check if a collection exists in a database."""
    return collection_name in client[db_name].list_collection_names()


def sr_connect(port=9030, user="root", password=None, database=None):
    """Create a mysql-connector connection to StarRocks (MySQL wire protocol).

    Args:
        port: StarRocks MySQL protocol port.
        user: StarRocks user.
        password: Optional password (omit for no-auth setups).
        database: Optional database to USE on connect.

    Returns:
        mysql.connector connection object.
    """
    import mysql.connector

    params = dict(host="localhost", port=port, user=user)
    if password:
        params["password"] = password
    if database:
        params["database"] = database
    return mysql.connector.connect(**params)


def sr_query_scalar(conn, query, params=None):
    """Execute a query and return the first column of the first row."""
    cur = conn.cursor()
    try:
        cur.execute(query, params or ())
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()


def sr_table_exists(conn, database, table):
    """Check if a table exists in a StarRocks database."""
    return sr_query_scalar(
        conn,
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (database, table),
    ) > 0


def sr_row_count(conn, database, table):
    """Return row count for a StarRocks table."""
    return sr_query_scalar(conn, f"SELECT COUNT(*) FROM `{database}`.`{table}`")


def wait_mcp_alive(url, timeout=60):
    """Poll an MCP HTTP endpoint until it returns a non-5xx response.

    The bug class we care about for MCP is "container started, port bound,
    .ro_credentials read correctly" — proven by any HTTP response, including
    4xx (e.g. method-not-allowed, missing-Accept-header). A connection error
    means the container isn't bound; a 5xx means the server is broken.

    Args:
        url: Full URL to poll (e.g. http://localhost:8000/sse).
        timeout: Max wait time in seconds.

    Returns:
        The first non-5xx requests.Response.
    """
    import time
    import requests

    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            resp = requests.get(url, timeout=3, stream=True)
            # Drain a tiny bit so the connection can close cleanly for
            # streaming endpoints (SSE / Streamable HTTP).
            resp.close()
            if resp.status_code < 500:
                return resp
            last_err = f"HTTP {resp.status_code}"
        except requests.RequestException as e:
            last_err = repr(e)
        time.sleep(2)
    raise TimeoutError(f"MCP at {url} did not respond within {timeout}s. Last: {last_err}")


def read_ro_credentials(data_path):
    """Read .ro_credentials from a database data directory.

    Format on disk is `username:password` (single line, chmod 600). Written
    by `_write_ro_credentials` in setup/db.py during `sdp db setup` when
    auth is enabled with a read-only user.

    Args:
        data_path: pathlib.Path to the data directory (e.g. workspace /
            "data" / "database" / "postgres").

    Returns:
        (username, password) tuple.
    """
    cred = (data_path / ".ro_credentials").read_text().strip()
    user, _, pwd = cred.partition(":")
    return user, pwd


def sr_index_columns(conn, database, table):
    """Return the set of column names with BITMAP indexes on a table.

    Uses SHOW INDEXES; StarRocks reports one row per index with the column
    in the `Column_name` field (case as returned by the server).
    """
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW INDEXES FROM `{database}`.`{table}`")
        rows = cur.fetchall()
        # Locate Column_name dynamically — column ordering varies by SR version.
        col_names = [d[0] for d in cur.description]
        idx = col_names.index("Column_name")
        return {row[idx] for row in rows}
    finally:
        cur.close()
