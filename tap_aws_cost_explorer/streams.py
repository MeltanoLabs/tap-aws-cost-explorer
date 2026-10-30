"""Stream type classes for tap-aws-cost-explorer."""

from __future__ import annotations

import re
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

from singer_sdk import Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.exceptions import ConfigValidationError

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context

    from tap_aws_cost_explorer.tap import TapAWSCostExplorer

GROUP_BY_TYPES = ["DIMENSION", "TAG", "COST_CATEGORY"]

# Cost Explorer accepts at most two group-bys per query.
MAX_GROUP_BY = 2

# Column name prefixes, so a tag and a dimension with the same key don't collide.
GROUP_COLUMN_PREFIXES = {"DIMENSION": "", "TAG": "tag_", "COST_CATEGORY": "cost_category_"}


def _dimension(key: str) -> dict[str, str]:
    return {"type": "DIMENSION", "key": key}


# Every member account's costs are visible from the organization's management account,
# split out by the LINKED_ACCOUNT dimension.
BUILTIN_REPORTS: list[dict[str, Any]] = [
    {"name": "cost", "group_by": []},
    {
        "name": "cost_by_account_service",
        "group_by": [_dimension("LINKED_ACCOUNT"), _dimension("SERVICE")],
    },
    {
        "name": "cost_by_account_usage_type",
        "group_by": [_dimension("LINKED_ACCOUNT"), _dimension("USAGE_TYPE")],
    },
    {
        "name": "cost_by_account_region",
        "group_by": [_dimension("LINKED_ACCOUNT"), _dimension("REGION")],
    },
    {
        "name": "cost_by_service_usage_type",
        "group_by": [_dimension("SERVICE"), _dimension("USAGE_TYPE")],
    },
]


def group_column(group: dict[str, str]) -> str:
    """Return the column name for a group-by, e.g. `linked_account` or `tag_team`.

    Args:
        group: A Cost Explorer group definition.

    Returns:
        The column name.
    """
    key = re.sub(r"[^0-9a-z]+", "_", group["key"].lower()).strip("_")
    return GROUP_COLUMN_PREFIXES[group["type"]] + key


def group_value(group: dict[str, str], key: str) -> str | None:
    """Return a group's value from a Cost Explorer group key.

    Tag and cost category keys come back as `name$value`, with an empty value for
    costs that aren't tagged or categorized.

    Args:
        group: A Cost Explorer group definition.
        key: The key Cost Explorer returned for the group.

    Returns:
        The group value, or None for untagged or uncategorized costs.
    """
    if group["type"] == "DIMENSION":
        return key
    return key.partition("$")[2] or None


class AWSStream(Stream):
    """Base class for streams that call AWS."""

    def get_client(self, service_name: str) -> Any:  # noqa: ANN401
        """Return a client for an AWS service, from the tap's session.

        Args:
            service_name: The AWS service, e.g. `ce`.

        Returns:
            A boto3 client.
        """
        return cast("TapAWSCostExplorer", self._tap).get_client(service_name)


class CostReportStream(AWSStream):
    """Costs from the GetCostAndUsage API, grouped by up to two dimensions."""

    replication_key = "time_period_start"
    is_sorted = False

    def __init__(
        self,
        tap: TapAWSCostExplorer,
        *,
        name: str,
        group_by: list[dict[str, str]],
        filter_expression: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the stream.

        Args:
            tap: The parent tap.
            name: The stream name.
            group_by: Up to two dimensions, tags or cost categories to group costs by.
            filter_expression: An optional Cost Explorer filter expression.

        Raises:
            ConfigValidationError: If the group-bys are invalid.
        """
        if len(group_by) > MAX_GROUP_BY:
            msg = f"Report '{name}' has more than {MAX_GROUP_BY} group_by entries"
            raise ConfigValidationError(msg)
        self.group_by = group_by
        self.group_columns = [group_column(group) for group in group_by]
        if len(set(self.group_columns)) != len(self.group_columns):
            msg = f"Report '{name}' groups by the same column more than once"
            raise ConfigValidationError(msg)
        self.filter_expression = filter_expression
        schema = th.PropertiesList(
            th.Property("time_period_start", th.DateType, required=True),
            th.Property("time_period_end", th.DateType),
            th.Property("estimated", th.BooleanType),
            *(th.Property(column, th.StringType) for column in self.group_columns),
            th.Property("metric_name", th.StringType, required=True),
            th.Property("amount", th.NumberType),
            th.Property("amount_unit", th.StringType),
        ).to_dict()
        super().__init__(tap, schema=schema, name=name)
        # Set after __init__, which resets the primary keys.
        self.primary_keys = ["time_period_start", *self.group_columns, "metric_name"]

    def _get_time_period(self, context: Context | None) -> tuple[date, date]:
        start = date.fromisoformat(self.config["start_date"][:10])
        if bookmark := self.get_starting_replication_key_value(context):
            lookback = timedelta(days=self.config.get("lookback_days", 3))
            start = max(start, date.fromisoformat(str(bookmark)[:10]) - lookback)
        if self.config.get("granularity") == "MONTHLY":
            # A mid-month start would return a partial month under a different key.
            start = start.replace(day=1)
        if end_date := self.config.get("end_date"):
            end = date.fromisoformat(end_date[:10])
        else:
            end = datetime.now(tz=timezone.utc).date()
        return start, end

    @override
    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One record per time period, group and metric.
        """
        start, end = self._get_time_period(context)
        if start >= end:
            return

        client = self.get_client("ce")
        request: dict[str, Any] = {
            "TimePeriod": {"Start": start.isoformat(), "End": end.isoformat()},
            "Granularity": self.config.get("granularity", "DAILY"),
            "Metrics": self.config["metrics"],
        }
        if self.group_by:
            request["GroupBy"] = [
                {"Type": group["type"], "Key": group["key"]} for group in self.group_by
            ]
        if self.filter_expression:
            request["Filter"] = self.filter_expression

        while True:
            response = client.get_cost_and_usage(**request)
            for result in response["ResultsByTime"]:
                period = {
                    "time_period_start": result["TimePeriod"]["Start"],
                    "time_period_end": result["TimePeriod"]["End"],
                    "estimated": result.get("Estimated"),
                }
                if self.group_by:
                    for group in result.get("Groups", []):
                        groups = {
                            column: group_value(group_def, key)
                            for column, group_def, key in zip(
                                self.group_columns, self.group_by, group["Keys"], strict=True
                            )
                        }
                        yield from self._metric_records(period | groups, group["Metrics"])
                else:
                    yield from self._metric_records(period, result.get("Total", {}))

            if not (token := response.get("NextPageToken")):
                break
            request["NextPageToken"] = token

    @staticmethod
    def _metric_records(
        base: dict[str, Any], metrics: dict[str, dict[str, str]]
    ) -> Iterable[dict[str, Any]]:
        for metric_name, metric in metrics.items():
            yield {
                **base,
                "metric_name": metric_name,
                "amount": Decimal(metric["Amount"]),
                "amount_unit": metric.get("Unit"),
            }


class AccountsStream(AWSStream):
    """Accounts in the AWS organization, to name the accounts in cost reports."""

    name = "accounts"
    primary_keys = ("id",)
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType, required=True),
        th.Property("arn", th.StringType),
        th.Property("email", th.StringType),
        th.Property("name", th.StringType),
        th.Property("state", th.StringType),
        th.Property("status", th.StringType),
        th.Property("joined_method", th.StringType),
        th.Property("joined_timestamp", th.DateTimeType),
    ).to_dict()

    @override
    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One record per account.
        """
        paginator = self.get_client("organizations").get_paginator("list_accounts")
        for page in paginator.paginate():
            for account in page["Accounts"]:
                yield {
                    "id": account["Id"],
                    "arn": account.get("Arn"),
                    "email": account.get("Email"),
                    "name": account.get("Name"),
                    "state": account.get("State"),
                    "status": account.get("Status"),
                    "joined_method": account.get("JoinedMethod"),
                    "joined_timestamp": account.get("JoinedTimestamp"),
                }
