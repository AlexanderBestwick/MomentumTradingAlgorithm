import datetime as dt

from Database.Connection import DEFAULT_DATABASE_PATH, connect_database
from Funds.Accounting import (
    DEFAULT_INITIAL_UNIT_PRICE,
    apply_cash_flows_at_nav,
    build_unit_ledger_entries,
    calculate_member_value,
    calculate_ownership_percent,
)
from Funds.Models import (
    CashFlowRequest,
    FundDefinition,
    FundMembership,
    InvestorDefinition,
    NavSnapshot,
    UnitLedgerEntry,
)


DEFAULT_FUND_DATABASE_PATH = DEFAULT_DATABASE_PATH


def ensure_fund_schema(database_path=DEFAULT_FUND_DATABASE_PATH, *, database_url=None):
    with connect_database(database_path=database_path, database_url=database_url) as session:
        session.executescript(
            """
            CREATE TABLE IF NOT EXISTS funds (
                fund_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                base_currency TEXT NOT NULL,
                initial_unit_price REAL NOT NULL,
                active INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                notes TEXT
            );

            CREATE TABLE IF NOT EXISTS fund_investors (
                investor_id TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                email TEXT,
                active INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS fund_memberships (
                fund_id TEXT NOT NULL,
                investor_id TEXT NOT NULL,
                joined_at TEXT NOT NULL,
                active INTEGER NOT NULL,
                PRIMARY KEY (fund_id, investor_id),
                FOREIGN KEY (fund_id) REFERENCES funds(fund_id) ON DELETE CASCADE,
                FOREIGN KEY (investor_id) REFERENCES fund_investors(investor_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS fund_cash_flows (
                request_id TEXT PRIMARY KEY,
                fund_id TEXT NOT NULL,
                investor_id TEXT NOT NULL,
                flow_type TEXT NOT NULL,
                status TEXT NOT NULL,
                cash_amount REAL NOT NULL,
                requested_at TEXT NOT NULL,
                effective_at TEXT,
                processed_at TEXT,
                unit_price REAL,
                units_delta REAL,
                note TEXT,
                FOREIGN KEY (fund_id) REFERENCES funds(fund_id) ON DELETE CASCADE,
                FOREIGN KEY (investor_id) REFERENCES fund_investors(investor_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS fund_nav_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                fund_id TEXT NOT NULL,
                valued_at TEXT NOT NULL,
                net_asset_value REAL NOT NULL,
                gross_asset_value REAL NOT NULL,
                cash_value REAL NOT NULL,
                liabilities_value REAL NOT NULL,
                total_units REAL NOT NULL,
                unit_price REAL NOT NULL,
                source_run_id TEXT,
                note TEXT,
                FOREIGN KEY (fund_id) REFERENCES funds(fund_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS fund_unit_ledger (
                entry_id TEXT PRIMARY KEY,
                fund_id TEXT NOT NULL,
                investor_id TEXT NOT NULL,
                snapshot_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                entry_type TEXT NOT NULL,
                cash_amount REAL NOT NULL,
                unit_price REAL NOT NULL,
                units_delta REAL NOT NULL,
                request_id TEXT,
                note TEXT,
                FOREIGN KEY (fund_id) REFERENCES funds(fund_id) ON DELETE CASCADE,
                FOREIGN KEY (investor_id) REFERENCES fund_investors(investor_id) ON DELETE CASCADE,
                FOREIGN KEY (snapshot_id) REFERENCES fund_nav_snapshots(snapshot_id) ON DELETE CASCADE,
                FOREIGN KEY (request_id) REFERENCES fund_cash_flows(request_id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS fund_strategy_allocations (
                fund_id TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                sleeve_weight REAL NOT NULL,
                active INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                notes TEXT,
                PRIMARY KEY (fund_id, strategy_id),
                FOREIGN KEY (fund_id) REFERENCES funds(fund_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_fund_cash_flows_status_time
            ON fund_cash_flows(fund_id, status, requested_at);

            CREATE INDEX IF NOT EXISTS idx_fund_nav_snapshots_fund_time
            ON fund_nav_snapshots(fund_id, valued_at DESC);

            CREATE INDEX IF NOT EXISTS idx_fund_unit_ledger_fund_investor
            ON fund_unit_ledger(fund_id, investor_id, created_at DESC);
            """
        )


def _as_iso_datetime(value):
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def create_fund(fund, *, created_at=None, database_path=DEFAULT_FUND_DATABASE_PATH, database_url=None):
    ensure_fund_schema(database_path, database_url=database_url)
    if not isinstance(fund, FundDefinition):
        raise TypeError("fund must be a FundDefinition instance.")

    created_at = created_at or dt.datetime.now(dt.timezone.utc)
    with connect_database(database_path=database_path, database_url=database_url) as session:
        session.execute(
            """
            INSERT INTO funds (
                fund_id,
                name,
                base_currency,
                initial_unit_price,
                active,
                created_at,
                notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fund_id) DO UPDATE SET
                name = excluded.name,
                base_currency = excluded.base_currency,
                initial_unit_price = excluded.initial_unit_price,
                active = excluded.active,
                notes = excluded.notes
            """,
            (
                fund.fund_id,
                fund.name,
                fund.base_currency,
                float(fund.initial_unit_price),
                int(bool(fund.active)),
                created_at.isoformat(),
                fund.notes,
            ),
        )


def upsert_investor(investor, *, created_at=None, database_path=DEFAULT_FUND_DATABASE_PATH, database_url=None):
    ensure_fund_schema(database_path, database_url=database_url)
    if not isinstance(investor, InvestorDefinition):
        raise TypeError("investor must be an InvestorDefinition instance.")

    created_at = created_at or dt.datetime.now(dt.timezone.utc)
    with connect_database(database_path=database_path, database_url=database_url) as session:
        session.execute(
            """
            INSERT INTO fund_investors (
                investor_id,
                display_name,
                email,
                active,
                created_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(investor_id) DO UPDATE SET
                display_name = excluded.display_name,
                email = excluded.email,
                active = excluded.active
            """,
            (
                investor.investor_id,
                investor.display_name,
                investor.email,
                int(bool(investor.active)),
                created_at.isoformat(),
            ),
        )


def set_fund_membership(membership, *, database_path=DEFAULT_FUND_DATABASE_PATH, database_url=None):
    ensure_fund_schema(database_path, database_url=database_url)
    if not isinstance(membership, FundMembership):
        raise TypeError("membership must be a FundMembership instance.")

    with connect_database(database_path=database_path, database_url=database_url) as session:
        session.execute(
            """
            INSERT INTO fund_memberships (
                fund_id,
                investor_id,
                joined_at,
                active
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(fund_id, investor_id) DO UPDATE SET
                joined_at = excluded.joined_at,
                active = excluded.active
            """,
            (
                membership.fund_id,
                membership.investor_id,
                membership.joined_at.isoformat(),
                int(bool(membership.active)),
            ),
        )


def set_strategy_allocation(
    fund_id,
    strategy_id,
    sleeve_weight,
    *,
    active=True,
    updated_at=None,
    notes=None,
    database_path=DEFAULT_FUND_DATABASE_PATH,
    database_url=None,
):
    ensure_fund_schema(database_path, database_url=database_url)
    updated_at = updated_at or dt.datetime.now(dt.timezone.utc)

    with connect_database(database_path=database_path, database_url=database_url) as session:
        session.execute(
            """
            INSERT INTO fund_strategy_allocations (
                fund_id,
                strategy_id,
                sleeve_weight,
                active,
                updated_at,
                notes
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(fund_id, strategy_id) DO UPDATE SET
                sleeve_weight = excluded.sleeve_weight,
                active = excluded.active,
                updated_at = excluded.updated_at,
                notes = excluded.notes
            """,
            (
                str(fund_id).strip(),
                str(strategy_id).strip(),
                float(sleeve_weight),
                int(bool(active)),
                updated_at.isoformat(),
                notes,
            ),
        )


def record_cash_flow_request(
    cash_flow,
    *,
    status="pending",
    database_path=DEFAULT_FUND_DATABASE_PATH,
    database_url=None,
):
    ensure_fund_schema(database_path, database_url=database_url)
    if not isinstance(cash_flow, CashFlowRequest):
        raise TypeError("cash_flow must be a CashFlowRequest instance.")

    with connect_database(database_path=database_path, database_url=database_url) as session:
        session.execute(
            """
            INSERT INTO fund_cash_flows (
                request_id,
                fund_id,
                investor_id,
                flow_type,
                status,
                cash_amount,
                requested_at,
                effective_at,
                note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(request_id) DO UPDATE SET
                flow_type = excluded.flow_type,
                status = excluded.status,
                cash_amount = excluded.cash_amount,
                requested_at = excluded.requested_at,
                effective_at = excluded.effective_at,
                note = excluded.note
            """,
            (
                cash_flow.request_id,
                cash_flow.fund_id,
                cash_flow.investor_id,
                cash_flow.flow_type,
                str(status).strip().lower(),
                float(cash_flow.amount),
                cash_flow.requested_at.isoformat(),
                _as_iso_datetime(cash_flow.effective_at),
                cash_flow.note,
            ),
        )


def save_nav_snapshot(snapshot, *, database_path=DEFAULT_FUND_DATABASE_PATH, database_url=None):
    ensure_fund_schema(database_path, database_url=database_url)
    if not isinstance(snapshot, NavSnapshot):
        raise TypeError("snapshot must be a NavSnapshot instance.")

    with connect_database(database_path=database_path, database_url=database_url) as session:
        session.execute(
            """
            INSERT INTO fund_nav_snapshots (
                snapshot_id,
                fund_id,
                valued_at,
                net_asset_value,
                gross_asset_value,
                cash_value,
                liabilities_value,
                total_units,
                unit_price,
                source_run_id,
                note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_id) DO UPDATE SET
                fund_id = excluded.fund_id,
                valued_at = excluded.valued_at,
                net_asset_value = excluded.net_asset_value,
                gross_asset_value = excluded.gross_asset_value,
                cash_value = excluded.cash_value,
                liabilities_value = excluded.liabilities_value,
                total_units = excluded.total_units,
                unit_price = excluded.unit_price,
                source_run_id = excluded.source_run_id,
                note = excluded.note
            """,
            (
                snapshot.snapshot_id,
                snapshot.fund_id,
                snapshot.valued_at.isoformat(),
                float(snapshot.net_asset_value),
                float(snapshot.gross_asset_value),
                float(snapshot.cash_value),
                float(snapshot.liabilities_value),
                float(snapshot.total_units),
                float(snapshot.unit_price),
                snapshot.source_run_id,
                snapshot.note,
            ),
        )


def record_unit_ledger_entries(entries, *, database_path=DEFAULT_FUND_DATABASE_PATH, database_url=None):
    ensure_fund_schema(database_path, database_url=database_url)
    if any(not isinstance(entry, UnitLedgerEntry) for entry in entries):
        raise TypeError("entries must contain UnitLedgerEntry instances.")

    if not entries:
        return

    with connect_database(database_path=database_path, database_url=database_url) as session:
        session.executemany(
            """
            INSERT INTO fund_unit_ledger (
                entry_id,
                fund_id,
                investor_id,
                snapshot_id,
                created_at,
                entry_type,
                cash_amount,
                unit_price,
                units_delta,
                request_id,
                note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(entry_id) DO UPDATE SET
                entry_type = excluded.entry_type,
                cash_amount = excluded.cash_amount,
                unit_price = excluded.unit_price,
                units_delta = excluded.units_delta,
                request_id = excluded.request_id,
                note = excluded.note
            """,
            [
                (
                    entry.entry_id,
                    entry.fund_id,
                    entry.investor_id,
                    entry.snapshot_id,
                    entry.created_at.isoformat(),
                    entry.entry_type,
                    float(entry.cash_amount),
                    float(entry.unit_price),
                    float(entry.units_delta),
                    entry.request_id,
                    entry.note,
                )
                for entry in entries
            ],
        )


def load_latest_nav_snapshot(fund_id, *, database_path=DEFAULT_FUND_DATABASE_PATH, database_url=None):
    ensure_fund_schema(database_path, database_url=database_url)
    with connect_database(database_path=database_path, database_url=database_url) as session:
        return session.fetchone_dict(
            """
            SELECT *
            FROM fund_nav_snapshots
            WHERE fund_id = ?
            ORDER BY valued_at DESC
            LIMIT 1
            """,
            (str(fund_id).strip(),),
        )


def list_pending_cash_flows(
    fund_id,
    *,
    effective_on_or_before=None,
    database_path=DEFAULT_FUND_DATABASE_PATH,
    database_url=None,
):
    ensure_fund_schema(database_path, database_url=database_url)
    comparison_time = _as_iso_datetime(effective_on_or_before) if effective_on_or_before else None

    query = """
        SELECT *
        FROM fund_cash_flows
        WHERE fund_id = ?
          AND status = 'pending'
    """
    params = [str(fund_id).strip()]

    if comparison_time is not None:
        query += """
          AND COALESCE(effective_at, requested_at) <= ?
        """
        params.append(comparison_time)

    query += """
        ORDER BY COALESCE(effective_at, requested_at) ASC, requested_at ASC, request_id ASC
    """

    with connect_database(database_path=database_path, database_url=database_url) as session:
        return session.fetchall_dicts(query, tuple(params))


def list_investor_unit_balances(fund_id, *, database_path=DEFAULT_FUND_DATABASE_PATH, database_url=None):
    ensure_fund_schema(database_path, database_url=database_url)
    latest_snapshot = load_latest_nav_snapshot(fund_id, database_path=database_path, database_url=database_url)

    with connect_database(database_path=database_path, database_url=database_url) as session:
        rows = session.fetchall_dicts(
            """
            SELECT
                membership.investor_id,
                investor.display_name,
                investor.email,
                COALESCE(SUM(ledger.units_delta), 0.0) AS units_balance
            FROM fund_memberships AS membership
            JOIN fund_investors AS investor
              ON investor.investor_id = membership.investor_id
            LEFT JOIN fund_unit_ledger AS ledger
              ON ledger.fund_id = membership.fund_id
             AND ledger.investor_id = membership.investor_id
            WHERE membership.fund_id = ?
              AND membership.active = 1
            GROUP BY membership.investor_id, investor.display_name, investor.email
            ORDER BY investor.display_name ASC
            """,
            (str(fund_id).strip(),),
        )

    total_units = 0.0 if latest_snapshot is None else float(latest_snapshot["total_units"])
    unit_price = 0.0 if latest_snapshot is None else float(latest_snapshot["unit_price"])

    for row in rows:
        units_balance = float(row["units_balance"])
        row["current_value"] = calculate_member_value(units_balance, unit_price)
        row["ownership_percent"] = calculate_ownership_percent(units_balance, total_units)
        row["unit_price"] = unit_price

    return rows


def execute_pending_cash_flows(
    fund_id,
    *,
    valued_at,
    net_asset_value_before_flows,
    gross_asset_value_before_flows,
    cash_value_before_flows,
    liabilities_value=0.0,
    source_run_id=None,
    note=None,
    database_path=DEFAULT_FUND_DATABASE_PATH,
    database_url=None,
):
    ensure_fund_schema(database_path, database_url=database_url)
    fund_id = str(fund_id).strip()

    latest_snapshot = load_latest_nav_snapshot(fund_id, database_path=database_path, database_url=database_url)
    pending_rows = list_pending_cash_flows(
        fund_id,
        effective_on_or_before=valued_at,
        database_path=database_path,
        database_url=database_url,
    )

    with connect_database(database_path=database_path, database_url=database_url) as session:
        fund_row = session.fetchone_dict(
            "SELECT * FROM funds WHERE fund_id = ?",
            (fund_id,),
        )
        if fund_row is None:
            raise RuntimeError(f"Unknown fund_id: {fund_id}")

    initial_unit_price = float(fund_row["initial_unit_price"] or DEFAULT_INITIAL_UNIT_PRICE)
    total_units_before_flows = 0.0 if latest_snapshot is None else float(latest_snapshot["total_units"])

    cash_flows = [
        CashFlowRequest(
            request_id=row["request_id"],
            fund_id=row["fund_id"],
            investor_id=row["investor_id"],
            flow_type=row["flow_type"],
            amount=float(row["cash_amount"]),
            requested_at=dt.datetime.fromisoformat(row["requested_at"]),
            effective_at=None if row["effective_at"] is None else dt.datetime.fromisoformat(row["effective_at"]),
            note=row.get("note"),
        )
        for row in pending_rows
    ]

    batch_result = apply_cash_flows_at_nav(
        net_asset_value_before_flows=net_asset_value_before_flows,
        total_units_before_flows=total_units_before_flows,
        cash_flows=cash_flows,
        initial_unit_price=initial_unit_price,
    )

    if not batch_result.executions:
        return batch_result

    cash_delta = batch_result.ending_nav - batch_result.starting_nav
    snapshot = NavSnapshot(
        snapshot_id=f"{fund_id}:{valued_at:%Y%m%dT%H%M%S}",
        fund_id=fund_id,
        valued_at=valued_at,
        net_asset_value=batch_result.ending_nav,
        gross_asset_value=float(gross_asset_value_before_flows) + cash_delta,
        cash_value=float(cash_value_before_flows) + cash_delta,
        liabilities_value=float(liabilities_value),
        total_units=batch_result.ending_units,
        unit_price=batch_result.unit_price,
        source_run_id=source_run_id,
        note=note,
    )
    ledger_entries = build_unit_ledger_entries(
        batch_result,
        snapshot_id=snapshot.snapshot_id,
        created_at=valued_at,
    )

    save_nav_snapshot(snapshot, database_path=database_path, database_url=database_url)
    record_unit_ledger_entries(ledger_entries, database_path=database_path, database_url=database_url)

    with connect_database(database_path=database_path, database_url=database_url) as session:
        session.executemany(
            """
            UPDATE fund_cash_flows
            SET status = 'executed',
                effective_at = ?,
                processed_at = ?,
                unit_price = ?,
                units_delta = ?
            WHERE request_id = ?
            """,
            [
                (
                    valued_at.isoformat(),
                    valued_at.isoformat(),
                    float(execution.unit_price),
                    float(execution.units_delta),
                    execution.request_id,
                )
                for execution in batch_result.executions
            ],
        )

    return batch_result
