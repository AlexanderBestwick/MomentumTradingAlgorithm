from pathlib import Path

from Database.Connection import DEFAULT_DATABASE_PATH, connect_database


DEFAULT_BACKTEST_DATABASE_PATH = DEFAULT_DATABASE_PATH


def ensure_schema(database_path=DEFAULT_BACKTEST_DATABASE_PATH, *, database_url=None):
    with connect_database(database_path=database_path, database_url=database_url) as session:
        session.executescript(
            """
            CREATE TABLE IF NOT EXISTS backtest_runs (
                run_id TEXT PRIMARY KEY,
                generated_at TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                period_label TEXT NOT NULL,
                initial_cash REAL NOT NULL,
                benchmark_symbol TEXT NOT NULL,
                final_portfolio_value REAL NOT NULL,
                final_benchmark_value REAL NOT NULL,
                portfolio_return_percent REAL NOT NULL,
                benchmark_return_percent REAL NOT NULL,
                alpha_percent REAL NOT NULL,
                alpha_dollars REAL NOT NULL,
                final_reserve_percentage REAL NOT NULL,
                reserve_label TEXT NOT NULL,
                positions_final INTEGER NOT NULL,
                trade_count INTEGER NOT NULL,
                strategy_run_count INTEGER NOT NULL,
                fees_paid_cumulative REAL NOT NULL,
                max_drawdown_percent REAL NOT NULL,
                defensive_mode TEXT NOT NULL,
                defensive_symbol TEXT NOT NULL,
                raw_rank_consideration_limit INTEGER NOT NULL,
                max_position_fraction REAL NOT NULL,
                trade_fee_flat REAL NOT NULL,
                trade_fee_rate REAL NOT NULL,
                elapsed_seconds REAL NOT NULL,
                elapsed_label TEXT NOT NULL,
                results_path TEXT,
                chart_path TEXT
            );

            CREATE TABLE IF NOT EXISTS backtest_timeseries (
                run_id TEXT NOT NULL,
                point_index INTEGER NOT NULL,
                date TEXT NOT NULL,
                portfolio_value REAL NOT NULL,
                benchmark_value REAL NOT NULL,
                benchmark_200dma_value REAL NOT NULL,
                reserve_percentage REAL NOT NULL,
                cash REAL NOT NULL,
                invested_value REAL NOT NULL,
                positions INTEGER NOT NULL,
                strategy_ran INTEGER NOT NULL,
                market_health INTEGER,
                fees_paid_cumulative REAL NOT NULL,
                trade_count INTEGER NOT NULL,
                PRIMARY KEY (run_id, point_index),
                FOREIGN KEY (run_id) REFERENCES backtest_runs(run_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_backtest_runs_generated_at
            ON backtest_runs(generated_at DESC);

            CREATE INDEX IF NOT EXISTS idx_backtest_timeseries_run_date
            ON backtest_timeseries(run_id, date);
            """
        )


def save_backtest_record(
    backtest_record,
    results_df,
    *,
    database_path=DEFAULT_BACKTEST_DATABASE_PATH,
    database_url=None,
    results_path=None,
    chart_path=None,
):
    ensure_schema(database_path, database_url=database_url)

    summary = backtest_record["summary"]
    period = backtest_record["period"]
    run_id = backtest_record["id"]

    with connect_database(database_path=database_path, database_url=database_url) as session:
        session.execute("DELETE FROM backtest_runs WHERE run_id = ?", (run_id,))

        session.execute(
            """
            INSERT INTO backtest_runs (
                run_id,
                generated_at,
                start_date,
                end_date,
                period_label,
                initial_cash,
                benchmark_symbol,
                final_portfolio_value,
                final_benchmark_value,
                portfolio_return_percent,
                benchmark_return_percent,
                alpha_percent,
                alpha_dollars,
                final_reserve_percentage,
                reserve_label,
                positions_final,
                trade_count,
                strategy_run_count,
                fees_paid_cumulative,
                max_drawdown_percent,
                defensive_mode,
                defensive_symbol,
                raw_rank_consideration_limit,
                max_position_fraction,
                trade_fee_flat,
                trade_fee_rate,
                elapsed_seconds,
                elapsed_label,
                results_path,
                chart_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                backtest_record["generated_at"],
                period["start"],
                period["end"],
                period["label"],
                summary["initial_cash"],
                summary["benchmark_symbol"],
                summary["final_portfolio_value"],
                summary["final_benchmark_value"],
                summary["portfolio_return_percent"],
                summary["benchmark_return_percent"],
                summary["alpha_percent"],
                summary["alpha_dollars"],
                summary["final_reserve_percentage"],
                summary["reserve_label"],
                summary["positions_final"],
                summary["trade_count"],
                summary["strategy_run_count"],
                summary["fees_paid_cumulative"],
                summary["max_drawdown_percent"],
                summary["defensive_mode"],
                summary["defensive_symbol"],
                summary["raw_rank_consideration_limit"],
                summary["max_position_fraction"],
                summary["trade_fee_flat"],
                summary["trade_fee_rate"],
                summary["elapsed_seconds"],
                summary["elapsed_label"],
                str(results_path) if results_path else None,
                str(chart_path) if chart_path else None,
            ),
        )

        timeseries_rows = []
        for point_index, row in enumerate(results_df.itertuples(index=False)):
            market_health = None if row.market_health is None else int(bool(row.market_health))
            timeseries_rows.append(
                (
                    run_id,
                    point_index,
                    row.date.isoformat(),
                    float(row.portfolio_value),
                    float(row.sptm_value),
                    float(row.sptm_200dma_value),
                    float(row.reserve_percentage),
                    float(row.cash),
                    float(row.invested_value),
                    int(row.positions),
                    int(bool(row.strategy_ran)),
                    market_health,
                    float(row.fees_paid_cumulative),
                    int(row.trade_count),
                )
            )

        session.executemany(
            """
            INSERT INTO backtest_timeseries (
                run_id,
                point_index,
                date,
                portfolio_value,
                benchmark_value,
                benchmark_200dma_value,
                reserve_percentage,
                cash,
                invested_value,
                positions,
                strategy_ran,
                market_health,
                fees_paid_cumulative,
                trade_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            timeseries_rows,
        )


def list_backtest_runs(*, database_path=DEFAULT_BACKTEST_DATABASE_PATH, database_url=None, limit=20):
    ensure_schema(database_path, database_url=database_url)

    with connect_database(database_path=database_path, database_url=database_url) as session:
        rows = session.fetchall_dicts(
            """
            SELECT *
            FROM backtest_runs
            ORDER BY generated_at DESC
            LIMIT ?
            """,
            (limit,),
        )

    return rows


def load_backtest_run(run_id, *, database_path=DEFAULT_BACKTEST_DATABASE_PATH, database_url=None):
    ensure_schema(database_path, database_url=database_url)

    with connect_database(database_path=database_path, database_url=database_url) as session:
        run_row = session.fetchone_dict(
            "SELECT * FROM backtest_runs WHERE run_id = ?",
            (run_id,),
        )
        if run_row is None:
            return None

        series_rows = session.fetchall_dicts(
            """
            SELECT *
            FROM backtest_timeseries
            WHERE run_id = ?
            ORDER BY point_index ASC
            """,
            (run_id,),
        )

    return {
        "run": run_row,
        "series": series_rows,
    }
