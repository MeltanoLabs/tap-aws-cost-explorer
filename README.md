# tap-aws-cost-explorer

`tap-aws-cost-explorer` is a Singer tap for [AWS Cost Explorer](https://aws.amazon.com/aws-cost-management/aws-cost-explorer/), built with the [Meltano Singer SDK](https://sdk.meltano.com).

It extracts daily or monthly AWS costs, grouped by account, service, usage type, region, cost allocation tag or cost category. Run against an AWS Organizations management account, it covers every member account in the organization.

This is the MeltanoLabs variant, forked from [albert-marrero/tap-aws-cost-explorer](https://github.com/albert-marrero/tap-aws-cost-explorer) by Albert Marrero.

## Installation

```bash
uv tool install meltanolabs-tap-aws-cost-explorer
```

## Prerequisites

The tap needs AWS credentials that can call:

| Permission | Used by |
|---|---|
| `ce:GetCostAndUsage` | Every `cost*` stream |
| `organizations:ListAccounts` | The `accounts` stream |

To cover every account in an organization, use credentials (or `role_arn`) for the organization's **management account**. Cost Explorer there returns costs for every member account, split out by the `LINKED_ACCOUNT` dimension. Credentials for a member account only return that account's costs, and can't call `organizations:ListAccounts`, so deselect the `accounts` stream.

**Cost Explorer API requests are billed** by AWS, at $0.01 per request at the time of writing. The tap makes one request per page of results per stream, so each selected stream adds to the cost of a run.

## Settings

| Setting | Required | Default | Description |
|---|---|---|---|
| `access_key` | | | AWS access key ID. Leave unset to use the default AWS credential chain (environment variables, shared config, or an instance or task role). |
| `secret_key` | | | AWS secret access key. |
| `session_token` | | | AWS session token, if the credentials are temporary. |
| `role_arn` | | | IAM role to assume before calling AWS. |
| `start_date` | Yes | | First day of cost data to extract. Cost Explorer serves the last 14 months, or 38 months of monthly data if multi-year data is enabled. |
| `end_date` | | Today | Day to stop at, exclusive, so by default data runs to yesterday. |
| `lookback_days` | | `3` | Days before the bookmark to extract again on each run. |
| `granularity` | | `DAILY` | `DAILY` or `MONTHLY`. |
| `metrics` | | `AmortizedCost`, `UnblendedCost`, `NetUnblendedCost`, `UsageQuantity` | Metrics to extract. Also available: `BlendedCost`, `NetAmortizedCost` and `NormalizedUsageAmount`. |
| `reports` | | | Extra cost reports; see [Custom reports](#custom-reports). |

A full list of settings and capabilities is available by running:

```bash
tap-aws-cost-explorer --about
```

## Streams

| Stream | Grouped by | Primary key |
|---|---|---|
| `cost` | Nothing: total cost | `time_period_start`, `metric_name` |
| `cost_by_account_service` | `linked_account`, `service` | `time_period_start`, `linked_account`, `service`, `metric_name` |
| `cost_by_account_usage_type` | `linked_account`, `usage_type` | `time_period_start`, `linked_account`, `usage_type`, `metric_name` |
| `cost_by_account_region` | `linked_account`, `region` | `time_period_start`, `linked_account`, `region`, `metric_name` |
| `cost_by_service_usage_type` | `service`, `usage_type` | `time_period_start`, `service`, `usage_type`, `metric_name` |
| `accounts` | | `id` |

Each `cost*` stream has one row per time period, group and metric:

| Field | Type | Description |
|---|---|---|
| `time_period_start` | date | First day of the period, and the replication key |
| `time_period_end` | date | Day after the last day of the period |
| `estimated` | boolean | Whether AWS still considers the amount an estimate |
| _group columns_ | string | One column per group-by, e.g. `linked_account` and `service` |
| `metric_name` | string | The metric, e.g. `UnblendedCost` |
| `amount` | number | The amount |
| `amount_unit` | string | The unit, e.g. `USD`, or `N/A` for `UsageQuantity` |

The `accounts` stream lists the accounts in the organization, with their `id`, `arn`, `email`, `name`, `state`, `status`, `joined_method` and `joined_timestamp`. Join it to `linked_account` to name the accounts in cost reports.

### Custom reports

Cost Explorer can group a query by at most two things, so each report is its own stream. Add a report for any other combination of [dimensions](https://docs.aws.amazon.com/aws-cost-management/latest/APIReference/API_GetDimensionValues.html), cost allocation tags (`TAG`) or cost categories (`COST_CATEGORY`):

```yaml
config:
  reports:
  - name: cost_by_team
    group_by:
    - type: TAG
      key: team
    - type: DIMENSION
      key: SERVICE
    # Optional: a Cost Explorer filter expression, as in the GetCostAndUsage API.
    filter:
      Dimensions:
        Key: RECORD_TYPE
        Values: [Usage]
```

Group columns are named after the key in snake case, with tags prefixed `tag_` and cost categories `cost_category_`, so this report has `tag_team` and `service` columns. Costs that aren't tagged or categorized have a null tag or cost category value. A tag must be [activated as a cost allocation tag](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/activating-tags.html) before Cost Explorer can group by it.

## Incremental replication

Every `cost*` stream is incremental on `time_period_start`. Each run starts `lookback_days` before the bookmark, because AWS revises recent amounts as usage is processed and while a month's bill is finalized. Rows read again replace the earlier ones by primary key. With `MONTHLY` granularity, each run starts on the 1st of the month.

To backfill, reset the stream's state and set `start_date` to the first day you need, within the last 14 months.

## Usage

You can run `tap-aws-cost-explorer` by itself or in a pipeline using [Meltano](https://meltano.com/).

```bash
tap-aws-cost-explorer --version
tap-aws-cost-explorer --help
tap-aws-cost-explorer --config CONFIG --discover > ./catalog.json
```

## Developer resources

### Initialize your development environment

```bash
uv sync
```

### Create and run tests

The tests in `tests/test_streams.py` run against stubbed AWS responses. The standard SDK tests in `tests/test_core.py` call the live API, and run only when `TAP_AWS_COST_EXPLORER_ACCESS_KEY` and `TAP_AWS_COST_EXPLORER_SECRET_KEY` are set.

```bash
uv run pytest
```

### Testing with [Meltano](https://www.meltano.com)

```bash
uv tool install meltano
meltano install
meltano invoke tap-aws-cost-explorer --version
meltano run tap-aws-cost-explorer target-jsonl
```

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more on how to use the SDK to develop taps and targets.

## License

[MIT](LICENSE)
