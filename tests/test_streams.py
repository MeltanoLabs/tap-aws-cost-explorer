"""Tests stream behavior against stubbed AWS responses."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import boto3
import pytest
from botocore.stub import Stubber
from singer_sdk.exceptions import ConfigValidationError

from tap_aws_cost_explorer.tap import TapAWSCostExplorer

if TYPE_CHECKING:
    from collections.abc import Iterator

BASE_CONFIG = {
    "access_key": "test",
    "secret_key": "test",
    "start_date": "2026-10-01",
    "end_date": "2026-10-03",
    "metrics": ["UnblendedCost"],
}


def _client(service_name: str) -> Any:  # noqa: ANN401
    return boto3.client(
        service_name,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture
def ce() -> Iterator[Stubber]:
    """Stub the Cost Explorer client used by every stream."""
    client = _client("ce")
    with Stubber(client) as stubber, pytest.MonkeyPatch.context() as mp:
        mp.setattr(TapAWSCostExplorer, "get_client", lambda _self, _name: client)
        yield stubber
        stubber.assert_no_pending_responses()


def _records(tap: TapAWSCostExplorer, stream_name: str) -> list[dict[str, Any]]:
    stream = tap.streams[stream_name]
    # Load the bookmark from state, as Stream.sync() does before reading records.
    stream._write_starting_replication_value(None)  # noqa: SLF001
    return [record for record in stream.get_records(None) if isinstance(record, dict)]


def _metric(amount: str) -> dict[str, dict[str, str]]:
    return {"UnblendedCost": {"Amount": amount, "Unit": "USD"}}


def test_grouped_report_follows_pages(ce: Stubber) -> None:
    request = {
        "TimePeriod": {"Start": "2026-10-01", "End": "2026-10-03"},
        "Granularity": "DAILY",
        "Metrics": ["UnblendedCost"],
        "GroupBy": [
            {"Type": "DIMENSION", "Key": "LINKED_ACCOUNT"},
            {"Type": "DIMENSION", "Key": "SERVICE"},
        ],
    }
    period = {"TimePeriod": {"Start": "2026-10-01", "End": "2026-10-02"}, "Estimated": True}
    ce.add_response(
        "get_cost_and_usage",
        {
            "ResultsByTime": [
                {
                    **period,
                    "Groups": [{"Keys": ["111111111111", "Amazon S3"], "Metrics": _metric("1.5")}],
                }
            ],
            "NextPageToken": "page-2",
        },
        request,
    )
    ce.add_response(
        "get_cost_and_usage",
        {
            "ResultsByTime": [
                {
                    **period,
                    "Groups": [
                        {"Keys": ["222222222222", "AWS Lambda"], "Metrics": _metric("0.25")}
                    ],
                }
            ]
        },
        {**request, "NextPageToken": "page-2"},
    )

    records = _records(TapAWSCostExplorer(config=BASE_CONFIG), "cost_by_account_service")

    assert records == [
        {
            "time_period_start": "2026-10-01",
            "time_period_end": "2026-10-02",
            "estimated": True,
            "linked_account": "111111111111",
            "service": "Amazon S3",
            "metric_name": "UnblendedCost",
            "amount": Decimal("1.5"),
            "amount_unit": "USD",
        },
        {
            "time_period_start": "2026-10-01",
            "time_period_end": "2026-10-02",
            "estimated": True,
            "linked_account": "222222222222",
            "service": "AWS Lambda",
            "metric_name": "UnblendedCost",
            "amount": Decimal("0.25"),
            "amount_unit": "USD",
        },
    ]


def test_ungrouped_report_reads_totals(ce: Stubber) -> None:
    ce.add_response(
        "get_cost_and_usage",
        {
            "ResultsByTime": [
                {
                    "TimePeriod": {"Start": "2026-10-01", "End": "2026-10-02"},
                    "Total": _metric("10"),
                    "Groups": [],
                }
            ]
        },
    )

    records = _records(TapAWSCostExplorer(config=BASE_CONFIG), "cost")

    assert [(r["metric_name"], r["amount"]) for r in records] == [("UnblendedCost", Decimal(10))]
    assert tuple(TapAWSCostExplorer(config=BASE_CONFIG).streams["cost"].primary_keys) == (
        "time_period_start",
        "metric_name",
    )


def test_custom_report_tags_and_cost_categories(ce: Stubber) -> None:
    config = {
        **BASE_CONFIG,
        "reports": [
            {
                "name": "cost_by_team",
                "group_by": [
                    {"type": "TAG", "key": "Team"},
                    {"type": "COST_CATEGORY", "key": "Business Unit"},
                ],
                "filter": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Usage"]}},
            }
        ],
    }
    ce.add_response(
        "get_cost_and_usage",
        {
            "ResultsByTime": [
                {
                    "TimePeriod": {"Start": "2026-10-01", "End": "2026-10-02"},
                    "Groups": [
                        {
                            "Keys": ["Team$platform", "Business Unit$Engineering"],
                            "Metrics": _metric("3"),
                        },
                        {"Keys": ["Team$", "Business Unit$"], "Metrics": _metric("4")},
                    ],
                }
            ]
        },
        {
            "TimePeriod": {"Start": "2026-10-01", "End": "2026-10-03"},
            "Granularity": "DAILY",
            "Metrics": ["UnblendedCost"],
            "GroupBy": [
                {"Type": "TAG", "Key": "Team"},
                {"Type": "COST_CATEGORY", "Key": "Business Unit"},
            ],
            "Filter": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Usage"]}},
        },
    )

    records = _records(TapAWSCostExplorer(config=config), "cost_by_team")

    assert [(r["tag_team"], r["cost_category_business_unit"]) for r in records] == [
        ("platform", "Engineering"),
        (None, None),
    ]


@pytest.mark.parametrize(
    ("granularity", "expected_start"),
    [("DAILY", "2026-10-17"), ("MONTHLY", "2026-10-01")],
)
def test_bookmark_lookback(ce: Stubber, granularity: str, expected_start: str) -> None:
    config = {**BASE_CONFIG, "end_date": "2026-10-30", "granularity": granularity}
    state = {
        "bookmarks": {
            "cost": {
                "replication_key": "time_period_start",
                "replication_key_value": "2026-10-20",
            }
        }
    }
    ce.add_response(
        "get_cost_and_usage",
        {"ResultsByTime": []},
        {
            "TimePeriod": {"Start": expected_start, "End": "2026-10-30"},
            "Granularity": granularity,
            "Metrics": ["UnblendedCost"],
        },
    )

    assert _records(TapAWSCostExplorer(config=config, state=state), "cost") == []


@pytest.mark.usefixtures("ce")
def test_empty_time_period_makes_no_request() -> None:
    config = {**BASE_CONFIG, "end_date": "2026-10-01"}

    assert _records(TapAWSCostExplorer(config=config), "cost") == []


def test_accounts() -> None:
    client = _client("organizations")
    joined = datetime(2024, 1, 2, tzinfo=timezone.utc)
    with Stubber(client) as stubber, pytest.MonkeyPatch.context() as mp:
        mp.setattr(TapAWSCostExplorer, "get_client", lambda _self, _name: client)
        stubber.add_response(
            "list_accounts",
            {
                "Accounts": [
                    {
                        "Id": "111111111111",
                        "Name": "production",
                        "State": "ACTIVE",
                        "JoinedMethod": "CREATED",
                        "JoinedTimestamp": joined,
                    }
                ],
                "NextToken": "page-2",
            },
        )
        stubber.add_response(
            "list_accounts",
            {"Accounts": [{"Id": "222222222222", "Name": "staging"}]},
            {"NextToken": "page-2"},
        )

        records = _records(TapAWSCostExplorer(config=BASE_CONFIG), "accounts")

    assert [(r["id"], r["name"]) for r in records] == [
        ("111111111111", "production"),
        ("222222222222", "staging"),
    ]
    assert records[0]["joined_timestamp"] == joined


@pytest.mark.parametrize(
    "reports",
    [
        pytest.param(
            [
                {
                    "name": "too_many",
                    "group_by": [
                        {"type": "DIMENSION", "key": "SERVICE"},
                        {"type": "DIMENSION", "key": "REGION"},
                        {"type": "DIMENSION", "key": "USAGE_TYPE"},
                    ],
                }
            ],
            id="more than two group-bys",
        ),
        pytest.param(
            [{"name": "cost", "group_by": []}],
            id="clashes with a built-in stream",
        ),
        pytest.param(
            [
                {
                    "name": "same_column",
                    "group_by": [
                        {"type": "DIMENSION", "key": "SERVICE"},
                        {"type": "DIMENSION", "key": "service"},
                    ],
                }
            ],
            id="same column twice",
        ),
    ],
)
def test_invalid_reports(reports: list[dict[str, Any]]) -> None:
    with pytest.raises(ConfigValidationError):
        TapAWSCostExplorer(config={**BASE_CONFIG, "reports": reports}).discover_streams()
