"""AWS Cost Explorer tap class."""

from __future__ import annotations

import sys
from functools import cached_property
from typing import TYPE_CHECKING, Any

import boto3
from botocore.config import Config
from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.exceptions import ConfigValidationError

from tap_aws_cost_explorer import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.streams import Stream

# Cost Explorer and Organizations are global services served from us-east-1.
AWS_REGION = "us-east-1"

# Adaptive retries back off when Cost Explorer throttles requests.
BOTO_CONFIG = Config(retries={"mode": "adaptive", "max_attempts": 10})

DEFAULT_METRICS = ["AmortizedCost", "UnblendedCost", "NetUnblendedCost", "UsageQuantity"]


class TapAWSCostExplorer(Tap):
    """Singer tap for AWS Cost Explorer."""

    name = "tap-aws-cost-explorer"
    package_name = "meltanolabs-tap-aws-cost-explorer"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_key",
            th.StringType(nullable=True),
            secret=True,  # Flag config as protected.
            title="Access Key ID",
            description=(
                "AWS access key ID. Leave unset to use the default AWS credential chain "
                "(environment variables, shared config, or an instance or task role)."
            ),
        ),
        th.Property(
            "secret_key",
            th.StringType(nullable=True),
            secret=True,
            title="Secret Access Key",
            description="AWS secret access key for the access key ID.",
        ),
        th.Property(
            "session_token",
            th.StringType(nullable=True),
            secret=True,
            title="Session Token",
            description="AWS session token, if the credentials are temporary.",
        ),
        th.Property(
            "role_arn",
            th.StringType(nullable=True),
            title="Role ARN",
            description=(
                "IAM role to assume before calling AWS, for example a read-only billing "
                "role in the organization's management account."
            ),
        ),
        th.Property(
            "start_date",
            th.DateType(nullable=False),
            required=True,
            title="Start Date",
            description=(
                "First day of cost data to extract. Cost Explorer serves the last 14 months, "
                "or 38 months of monthly data if multi-year data is enabled."
            ),
        ),
        th.Property(
            "end_date",
            th.DateType(nullable=True),
            title="End Date",
            description="Day to stop at, exclusive. Defaults to today, so data runs to yesterday.",
        ),
        th.Property(
            "lookback_days",
            th.IntegerType(nullable=False),
            default=3,
            title="Lookback Days",
            description=(
                "Days before the bookmark to extract again on each run. AWS revises recent "
                "figures as usage is processed, so recent days are read again to pick up "
                "the final amounts."
            ),
        ),
        th.Property(
            "granularity",
            th.StringType(allowed_values=["DAILY", "MONTHLY"]),
            default="DAILY",
            title="Granularity",
            description="Time period of each row: DAILY or MONTHLY.",
        ),
        th.Property(
            "metrics",
            th.ArrayType(th.StringType),
            default=DEFAULT_METRICS,
            title="Metrics",
            description=(
                "Cost metrics to extract: AmortizedCost, BlendedCost, NetAmortizedCost, "
                "NetUnblendedCost, NormalizedUsageAmount, UnblendedCost and UsageQuantity."
            ),
        ),
        th.Property(
            "reports",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "name",
                        th.StringType(nullable=False),
                        required=True,
                        description="Stream name for the report.",
                    ),
                    th.Property(
                        "group_by",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property(
                                    "type",
                                    th.StringType(allowed_values=streams.GROUP_BY_TYPES),
                                    required=True,
                                ),
                                th.Property("key", th.StringType, required=True),
                            )
                        ),
                        required=True,
                        description=(
                            "Up to two dimensions, tags or cost categories to group costs by, "
                            "for example {type: DIMENSION, key: SERVICE} or "
                            "{type: TAG, key: team}."
                        ),
                    ),
                    th.Property(
                        "filter",
                        th.ObjectType(additional_properties=True),
                        description=(
                            "Cost Explorer filter expression, in the same format as the "
                            "Filter parameter of the GetCostAndUsage API."
                        ),
                    ),
                )
            ),
            title="Reports",
            description=(
                "Extra cost reports, each extracted as its own stream. Use these to group "
                "costs by dimensions, tags or cost categories the built-in streams don't cover."
            ),
        ),
    ).to_dict()

    @cached_property
    def session(self) -> boto3.Session:
        """Return an AWS session, assuming `role_arn` if it is set.

        Returns:
            An AWS session.
        """
        session = boto3.Session(
            aws_access_key_id=self.config.get("access_key"),
            aws_secret_access_key=self.config.get("secret_key"),
            aws_session_token=self.config.get("session_token"),
            region_name=AWS_REGION,
        )
        if role_arn := self.config.get("role_arn"):
            sts = session.client("sts", config=BOTO_CONFIG)
            credentials = sts.assume_role(
                RoleArn=role_arn,
                RoleSessionName=self.name,
            )["Credentials"]
            session = boto3.Session(
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
                region_name=AWS_REGION,
            )
        return session

    def get_client(self, service_name: str) -> Any:  # noqa: ANN401
        """Return a client for an AWS service.

        Args:
            service_name: The AWS service, e.g. `ce`.

        Returns:
            A boto3 client.
        """
        return self.session.client(service_name, config=BOTO_CONFIG)

    @override
    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.

        Raises:
            ConfigValidationError: If two reports have the same name.
        """
        reports = [*streams.BUILTIN_REPORTS, *self.config.get("reports", [])]
        names = [streams.AccountsStream.name, *(report["name"] for report in reports)]
        if duplicates := sorted({name for name in names if names.count(name) > 1}):
            msg = (
                f"Report names must be unique, and not the name of a built-in stream: {duplicates}"
            )
            raise ConfigValidationError(msg)
        return [
            streams.AccountsStream(self),
            *(
                streams.CostReportStream(
                    self,
                    name=report["name"],
                    group_by=report["group_by"],
                    filter_expression=report.get("filter"),
                )
                for report in reports
            ),
        ]


if __name__ == "__main__":
    TapAWSCostExplorer.cli()
