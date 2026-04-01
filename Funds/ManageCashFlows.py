import argparse
from pathlib import Path

from Config import load_local_env
from Funds.FundCycle import (
    build_cash_flow_request_record,
    build_investor_record,
    mark_request_settled,
)
from Funds.LedgerStore import DEFAULT_LEDGER_PREFIX, FundLedgerStore


def _build_store(args):
    if args.bucket or args.ledger_root:
        return FundLedgerStore(
            fund_id=args.fund_id,
            bucket_name=args.bucket,
            prefix=args.prefix,
            root_path=args.ledger_root,
            aws_region=args.aws_region,
        )
    return FundLedgerStore.from_env(fund_id=args.fund_id, require=True)


def _print_request_summary(record):
    print(f"request_id={record['request_id']}")
    print(f"fund_id={record['fund_id']}")
    print(f"investor_id={record['investor_id']}")
    print(f"flow_type={record['flow_type']}")
    print(f"status={record['status']}")
    print(f"net_amount={record['net_amount']:.2f}")
    print(f"effective_at={record['effective_at']}")


def _record_command(args):
    store = _build_store(args)

    if args.display_name:
        investor_record = build_investor_record(
            investor_id=args.investor_id,
            display_name=args.display_name,
        )
        store.save_investor(investor_record)

    request_record = build_cash_flow_request_record(
        fund_id=args.fund_id,
        investor_id=args.investor_id,
        flow_type=args.flow_type,
        gross_amount=args.gross_amount,
        fee_amount=args.fee_amount,
        net_amount=args.net_amount,
        request_id=args.request_id,
        requested_at=args.requested_at,
        effective_at=args.effective_at,
        display_name=args.display_name,
        external_reference=args.external_reference,
        note=args.note,
    )
    store.save_request(request_record)

    print("Saved confirmed cash-flow request.")
    _print_request_summary(request_record)


def _settle_command(args):
    store = _build_store(args)
    existing_record = store.load_request(args.request_id)
    if existing_record is None:
        raise RuntimeError(f"Unknown request_id: {args.request_id}")

    updated_record = mark_request_settled(
        existing_record,
        settled_at=args.settled_at,
        note=args.note,
    )
    store.save_request(updated_record)

    print("Marked cash-flow request as settled.")
    _print_request_summary(updated_record)


def parse_args():
    load_local_env()

    parser = argparse.ArgumentParser(
        description="Record or settle manual fund cash flows in the private ledger."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    record_parser = subparsers.add_parser("record", help="Record a confirmed contribution or withdrawal.")
    record_parser.add_argument("--fund-id", required=True, help="Fund identifier.")
    record_parser.add_argument("--investor-id", required=True, help="Investor identifier.")
    record_parser.add_argument("--display-name", help="Investor display name to store alongside the request.")
    record_parser.add_argument(
        "--flow-type",
        required=True,
        choices=["contribution", "withdrawal"],
        help="Cash-flow type.",
    )
    record_parser.add_argument("--gross-amount", type=float, required=True, help="Gross amount before fees.")
    record_parser.add_argument("--fee-amount", type=float, default=0.0, help="Fee amount deducted before Alpaca.")
    record_parser.add_argument(
        "--net-amount",
        type=float,
        help="Net amount that actually reaches or leaves Alpaca. Defaults to gross minus fees.",
    )
    record_parser.add_argument("--request-id", help="Optional explicit request identifier.")
    record_parser.add_argument("--requested-at", help="ISO timestamp for when the cash was confirmed.")
    record_parser.add_argument(
        "--effective-at",
        help="ISO timestamp for the dealing point. Defaults to the next Wednesday at 09:30 UTC.",
    )
    record_parser.add_argument("--external-reference", help="Optional bank or manual transfer reference.")
    record_parser.add_argument("--note", help="Free-form note saved with the request.")
    record_parser.add_argument("--bucket", help="S3 bucket for the fund ledger.")
    record_parser.add_argument("--prefix", default=DEFAULT_LEDGER_PREFIX, help="S3 key prefix or local subfolder.")
    record_parser.add_argument("--ledger-root", type=Path, help="Local ledger root for dry-runs instead of S3.")
    record_parser.add_argument("--aws-region", help="AWS region for the S3 client.")
    record_parser.set_defaults(func=_record_command)

    settle_parser = subparsers.add_parser(
        "settle",
        help="Mark a previously executed withdrawal as settled after the cash leaves Alpaca.",
    )
    settle_parser.add_argument("--fund-id", required=True, help="Fund identifier.")
    settle_parser.add_argument("--request-id", required=True, help="Request identifier to settle.")
    settle_parser.add_argument("--settled-at", help="ISO timestamp for when the withdrawal was settled.")
    settle_parser.add_argument("--note", help="Optional settlement note.")
    settle_parser.add_argument("--bucket", help="S3 bucket for the fund ledger.")
    settle_parser.add_argument("--prefix", default=DEFAULT_LEDGER_PREFIX, help="S3 key prefix or local subfolder.")
    settle_parser.add_argument("--ledger-root", type=Path, help="Local ledger root for dry-runs instead of S3.")
    settle_parser.add_argument("--aws-region", help="AWS region for the S3 client.")
    settle_parser.set_defaults(func=_settle_command)

    return parser.parse_args()


def main():
    args = parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
