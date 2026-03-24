import sqlite3
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse


DEFAULT_DATABASE_PATH = Path("Data/backtest_results.db")


class DatabaseSession:
    def __init__(self, connection, *, dialect):
        self.connection = connection
        self.dialect = dialect

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.connection.close()

    def _translate_query(self, query):
        if self.dialect == "postgres":
            return query.replace("?", "%s")
        return query

    def executescript(self, script):
        if self.dialect == "sqlite":
            self.connection.executescript(script)
            return

        statements = [statement.strip() for statement in script.split(";") if statement.strip()]
        with self.connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)

    def execute(self, query, params=()):
        query = self._translate_query(query)
        if self.dialect == "sqlite":
            return self.connection.execute(query, params)

        cursor = self.connection.cursor()
        cursor.execute(query, params)
        return cursor

    def executemany(self, query, param_rows):
        query = self._translate_query(query)
        if self.dialect == "sqlite":
            self.connection.executemany(query, param_rows)
            return

        with self.connection.cursor() as cursor:
            cursor.executemany(query, param_rows)

    def fetchall_dicts(self, query, params=()):
        cursor = self.execute(query, params)
        rows = cursor.fetchall()
        if self.dialect == "sqlite":
            return [dict(row) for row in rows]

        columns = [description[0] for description in cursor.description]
        cursor.close()
        return [dict(zip(columns, row)) for row in rows]

    def fetchone_dict(self, query, params=()):
        cursor = self.execute(query, params)
        row = cursor.fetchone()
        if row is None:
            if self.dialect == "postgres":
                cursor.close()
            return None

        if self.dialect == "sqlite":
            return dict(row)

        columns = [description[0] for description in cursor.description]
        cursor.close()
        return dict(zip(columns, row))


def _normalize_sqlite_path(database_path=None, database_url=None):
    if database_url:
        parsed = urlparse(database_url)
        if parsed.scheme != "sqlite":
            raise RuntimeError(f"Unsupported sqlite URL scheme: {database_url}")
        if parsed.netloc not in {"", "localhost"}:
            raise RuntimeError("SQLite database URLs should not include a remote host.")
        path = unquote(parsed.path or "")
        if path.startswith("/") and len(path) >= 3 and path[2] == ":":
            path = path[1:]
        if not path:
            raise RuntimeError("SQLite database URL must include a file path.")
        return Path(path)

    return Path(database_path or DEFAULT_DATABASE_PATH)


def _connect_sqlite(database_path=None, database_url=None):
    database_path = _normalize_sqlite_path(database_path=database_path, database_url=database_url)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return DatabaseSession(connection, dialect="sqlite")


def _connect_postgres(database_url):
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL support requires the 'psycopg' package. "
            "Install it before using a postgresql:// database URL."
        ) from exc

    parsed = urlparse(database_url)
    query_params = parse_qs(parsed.query)
    sslmode = query_params.get("sslmode", ["require"])[0]
    connection = psycopg.connect(database_url, sslmode=sslmode)
    return DatabaseSession(connection, dialect="postgres")


def connect_database(*, database_path=None, database_url=None):
    if database_url:
        parsed = urlparse(database_url)
        if parsed.scheme == "sqlite":
            return _connect_sqlite(database_url=database_url)
        if parsed.scheme in {"postgres", "postgresql"}:
            return _connect_postgres(database_url)
        raise RuntimeError(
            "Unsupported database URL scheme. Use sqlite:///... or postgresql://..."
        )

    return _connect_sqlite(database_path=database_path)
