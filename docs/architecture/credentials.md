---
title: "Credentials and auth"
parent: "Architecture"
nav_order: 6
---
# Credentials and auth

Database authentication is optional; when `python sdp.py db setup` enables it, each database gets an admin user, whose password is prompted at every start and never stored, and an optional read-only user, whose password lives in `.ro_credentials` in the database's data volume. This page describes where each credential lives and how each database's entrypoint applies it. The users and the recovery commands are on [Databases](../user-guide/databases.md#authentication).

## Where credentials live

| Credential | Where | Who writes it |
|------------|-------|---------------|
| Admin password | Nowhere on disk: prompted with `getpass` at `python sdp.py db start` and passed to the containers as `POSTGRES_PASSWORD`, `MONGO_ADMIN_PASSWORD` or `STARROCKS_ROOT_PASSWORD` | You, at each start |
| Read-only user name | `config/db/<db>.yaml`, and `POSTGRES_RO_USER` / `MONGO_RO_USER` / `STARROCKS_RO_USER` in `.env` | `python sdp.py db setup` |
| Read-only password | `.ro_credentials` (chmod 600) in each database's data volume (`PGDATA_PATH`, `MONGO_DATA_PATH`, `STARROCKS_DATA_PATH`) | `python sdp.py db setup`, `python sdp.py db recover-password --regenerate-ro` |
| Auth flag | `POSTGRES_AUTH_ENABLED`, `MONGO_AUTH_ENABLED`, `STARROCKS_AUTH_ENABLED` in `.env`, and `config/db/<db>.yaml` | `python sdp.py db setup` |

The database entrypoint wrappers create or update the read-only user on every start, reading the password from `.ro_credentials` in the data volume. That is why `python sdp.py db reset` keeps `.ro_credentials`: the next `python sdp.py db start` re-creates the read-only user from it. `recover-password --regenerate-ro` writes a fresh `.ro_credentials` for every database with auth and a read-only user before each branch's final restart, and the entrypoint applies the new password as part of the same restart cycle.

## How each database applies auth

| Database | Mechanism |
|----------|-----------|
| PostgreSQL | `pg_hba.local.conf` uses `scram-sha-256` for remote connections and `trust` for the local socket. Existing databases are migrated by a temporary trust-auth start in the entrypoint. |
| MongoDB | Fresh databases delegate to the official `docker-entrypoint.sh`. Existing databases use the MongoDB localhost exception to create the first admin user. |
| StarRocks | The entrypoint wrapper starts StarRocks, waits for the FE to be ready, then sets the root password and creates or syncs the read-only user through SQL. Read-only access is `GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE 'sdp_readonly'`. |

## Password recovery

`python sdp.py db recover-password` resets the admin password without the old one:

- **PostgreSQL**: backs up `config/postgres/pg_hba.local.conf`, restarts PostgreSQL with trust auth, sets the new password, and restores the original `pg_hba.local.conf`.
- **MongoDB**: stops MongoDB, starts it without auth temporarily, and sets the new admin password.
- **StarRocks**: the analogue of PostgreSQL's trust-auth swap. It stops StarRocks, backs up `config/starrocks/fe.local.conf`, appends `enable_auth_check = false` to it, starts StarRocks with auth checks disabled, sets the new root password, and restores the normal `fe.local.conf`.

With auth enabled, the database health checks also log in as the read-only user, so a database counts as healthy only when that user works.
