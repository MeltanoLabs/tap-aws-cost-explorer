"""Tests standard tap features using the built-in SDK tests library."""

import os
from datetime import datetime, timedelta, timezone

import pytest
from singer_sdk.testing import get_tap_test_class

from tap_aws_cost_explorer.tap import TapAWSCostExplorer

SAMPLE_CONFIG = {
    "access_key": os.environ.get("TAP_AWS_COST_EXPLORER_ACCESS_KEY", ""),
    "secret_key": os.environ.get("TAP_AWS_COST_EXPLORER_SECRET_KEY", ""),
    "start_date": (datetime.now(tz=timezone.utc) - timedelta(days=7)).date().isoformat(),
}


# Run standard built-in tap tests from the SDK against the live API. Each Cost Explorer
# request is billed, so keep the date range short.
TestTapAWSCostExplorer = pytest.mark.skipif(
    not SAMPLE_CONFIG["access_key"],
    reason="TAP_AWS_COST_EXPLORER_ACCESS_KEY is not set",
)(
    get_tap_test_class(
        tap_class=TapAWSCostExplorer,
        config=SAMPLE_CONFIG,
    )
)
