import datetime as dt

from Database.Connection import DEFAULT_DATABASE_PATH, connect_database


DEFAULT_LIVE_DATABASE_PATH = DEFAULT_DATABASE_PATH


def ensure_live_schema(database_path=DEFAULT_LIVE_DATABASE_PATH, *, database_url=None):
    with connect_database(database_path=database_path, database_url=database_url) as session:
        session.executescript(
            """
            CREATE TABLE IF NOT EXISTS live_runs (
                run_id TEXT PRIMARY KEY,
                generated_at TEXT NOT NULL,
                run_date TEXT,
                status TEXT NOT NULL,
                environment TEXT NOT NULL,
                trigger_source TEXT NOT NULL,
                market_health INTEGER,
                approved_count INTEGER,
                raw_rank_consideration_limit INTEGER NOT NULL,
                max_position_fraction REAL NOT NULL,
                defensive_mode TEXT NOT NULL,
                defensive_symbol TEXT NOT NULL,
                opened_count INTEGER NOT NULL,
                closed_count INTEGER NOT NULL,
                overrisked_count INTEGER NOT NULL,
                underrisked_count INTEGER NOT NULL,
                capped_sells_count INTEGER NOT NULL,
                defensive_buy_count INTEGER NOT NULL,
                initial_cash REAL,
                final_cash REAL,
                initial_portfolio_value REAL,
                final_portfolio_value REAL,
                final_positions_count INTEGER NOT NULL,
                error_detail TEXT
            );

            CREATE TABLE IF NOT EXISTS live_run_actions (
                run_id TEXT NOT NULL,
                action_index INTEGER NOT NULL,
                action_type TEXT NOT NULL,
                symbol TEXT NOT NULL,
                PRIMARY KEY (run_id, action_index),
                FOREIGN KEY (run_id) REFERENCES live_runs(run_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS live_run_positions (
                run_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                qty REAL NOT NULL,
                market_value REAL NOT NULL,
                PRIMARY KEY (run_id, symbol),
                FOREIGN KEY (run_id) REFERENCES live_runs(run_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_live_runs_generated_at
            ON live_runs(generated_at DESC);

            CREATE INDEX IF NOT EXISTS idx_live_run_actions_run
            ON live_run_actions(run_id, action_type);
            """
        )


def _make_live_run_id(generated_at, run_date):
    suffix = run_date.isoformat() if run_date else "unknown-date"
    return f"live_{generated_at:%Y%m%dT%H%M%S}_{suffix}"


def _normalize_account_snapshot(snapshot):
    if snapshot is None:
        return {"cash": None, "portfolio_value": None}

    return {
        "cash": float(snapshot["cash"]) if snapshot.get("cash") is not None else None,
        "portfolio_value": float(snapshot["portfolio_value"]) if snapshot.get("portfolio_value") is not None else None,
    }


def save_live_run_record(
    *,
    result=None,
    generated_at=None,
    environment,
    trigger_source="worker",
    database_path=DEFAULT_LIVE_DATABASE_PATH,
    database_url=None,
    initial_account=None,
    final_account=None,
    final_positions=None,
    error_detail=None,
):
    ensure_live_schema(database_path, database_url=database_url)

    generated_at = generated_at or dt.datetime.now(dt.timezone.utc)
    run_date = result.get("run_date") if result else None
    run_id = _make_live_run_id(generated_at, run_date)
    status = "completed" if error_detail is None else "failed"

    initial_account = _normalize_account_snapshot(initial_account)
    final_account = _normalize_account_snapshot(final_account)
    final_positions = final_positions or []

    action_rows = []
    if result is not None:
        ordered_action_groups = [
            ("closed", result.get("closed", [])),
            ("capped_sell", result.get("capped_sells", [])),
            ("risk_sell", result.get("overrisked", [])),
            ("risk_buy", result.get("underrisked", [])),
            ("opened", result.get("opened", [])),
            ("defensive_buy", result.get("defensive_buys", [])),
        ]
        action_index = 0
        for action_type, symbols in ordered_action_groups:
            for symbol in symbols:
                action_rows.append((run_id, action_index, action_type, str(symbol)))
                action_index += 1

    with connect_database(database_path=database_path, database_url=database_url) as session:
        session.execute("DELETE FROM live_runs WHERE run_id = ?", (run_id,))

        session.execute(
            """
            INSERT INTO live_runs (
                run_id,
                generated_at,
                run_date,
                status,
                environment,
                trigger_source,
                market_health,
                approved_count,
                raw_rank_consideration_limit,
                max_position_fraction,
                defensive_mode,
                defensive_symbol,
                opened_count,
                closed_count,
                overrisked_count,
                underrisked_count,
                capped_sells_count,
                defensive_buy_count,
                initial_cash,
                final_cash,
                initial_portfolio_value,
                final_portfolio_value,
                final_positions_count,
                error_detail
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                generated_at.isoformat(),
                run_date.isoformat() if run_date else None,
                status,
                environment,
                trigger_source,
                None if result is None else int(bool(result.get("market_health"))),
                None if result is None else int(result.get("approved_count", 0)),
                0 if result is None else int(result.get("raw_rank_consideration_limit", 0)),
                0.0 if result is None else float(result.get("max_position_fraction", 0.0)),
                "" if result is None else str(result.get("defensive_mode", "")),
                "" if result is None else str(result.get("defensive_symbol", "")),
                0 if result is None else len(result.get("opened", [])),
                0 if result is None else len(result.get("closed", [])),
                0 if result is None else len(result.get("overrisked", [])),
                0 if result is None else len(result.get("underrisked", [])),
                0 if result is None else len(result.get("capped_sells", [])),
                0 if result is None else len(result.get("defensive_buys", [])),
                initial_account["cash"],
                final_account["cash"],
                initial_account["portfolio_value"],
                final_account["portfolio_value"],
                len(final_positions),
                error_detail,
            ),
        )

        if action_rows:
            session.executemany(
                """
                INSERT INTO live_run_actions (
                    run_id,
                    action_index,
                    action_type,
                    symbol
                ) VALUES (?, ?, ?, ?)
                """,
                action_rows,
            )

        position_rows = [
            (
                run_id,
                position["symbol"],
                float(position["qty"]),
                float(position["market_value"]),
            )
            for position in final_positions
        ]
        if position_rows:
            session.executemany(
                """
                INSERT INTO live_run_positions (
                    run_id,
                    symbol,
                    qty,
                    market_value
                ) VALUES (?, ?, ?, ?)
                """,
                position_rows,
            )

    return run_id


def list_live_runs(*, database_path=DEFAULT_LIVE_DATABASE_PATH, database_url=None, limit=20):
    ensure_live_schema(database_path, database_url=database_url)

    with connect_database(database_path=database_path, database_url=database_url) as session:
        rows = session.fetchall_dicts(
            """
            SELECT *
            FROM live_runs
            ORDER BY generated_at DESC
            LIMIT ?
            """,
            (limit,),
        )

    return rows


def load_live_run(run_id, *, database_path=DEFAULT_LIVE_DATABASE_PATH, database_url=None):
    ensure_live_schema(database_path, database_url=database_url)

    with connect_database(database_path=database_path, database_url=database_url) as session:
        run_row = session.fetchone_dict(
            "SELECT * FROM live_runs WHERE run_id = ?",
            (run_id,),
        )
        if run_row is None:
            return None

        action_rows = session.fetchall_dicts(
            """
            SELECT *
            FROM live_run_actions
            WHERE run_id = ?
            ORDER BY action_index ASC
            """,
            (run_id,),
        )
        position_rows = session.fetchall_dicts(
            """
            SELECT *
            FROM live_run_positions
            WHERE run_id = ?
            ORDER BY symbol ASC
            """,
            (run_id,),
        )

    return {
        "run": run_row,
        "actions": action_rows,
        "positions": position_rows,
    }
