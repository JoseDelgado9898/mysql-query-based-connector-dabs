# MySQL Query-Based Connector Pipeline

This Databricks Asset Bundle deploys a Lakeflow Connect **query-based**
ingestion pipeline for MySQL. The connector issues periodic queries against
source tables (using a cursor column) and writes SCD Type 1 Delta tables to
Unity Catalog. It does not require MySQL binary logging.

Unlike the [integrated CDC bundle](https://github.com/JoseDelgado9898/mysql-integrated-cdc-dabs),
query-based ingestion is **table-by-table**. Each source table needs its own
primary key and a monotonically increasing cursor column.

## Prerequisites

- A Unity Catalog-enabled Databricks workspace.
- The **Lakeflow Connect query-based connector** preview enabled in the
  workspace.
- A Unity Catalog connection to MySQL. Create and test it before deploying.
  The deploying principal needs `USE CONNECTION` on it.
- Destination privileges: `USE CATALOG`, `USE SCHEMA`, and `CREATE TABLE`.
- Databricks CLI authentication for the destination workspace.

Do not store MySQL credentials in this repository. Credentials belong in the
Unity Catalog connection object referenced by `connection_name`.

## Project structure

- `databricks.yml` defines bundle targets and shared environment values.
- `variables.yml` declares those variables.
- `resources/mysql_qbc.pipeline.yml` defines the ingestion pipeline.

Shared settings (destination catalog/schema, connection, source schema) are
variables. Per-table settings (`source_table`, `primary_keys`,
`cursor_columns`, hard-deletion interval) are hardcoded on each `objects`
entry so the bundle does not grow a variable per table.

## Configuration

Set these under the target in `databricks.yml`, or pass them with `--var`:

- `pipeline_name`: deployed pipeline name (default `mysql_qbc`).
- `connection_name`: Unity Catalog MySQL connection object name.
- `source_schema`: source MySQL database/schema.
- `dest_catalog`: destination Unity Catalog catalog.
- `dest_schema`: destination Unity Catalog schema.

Add another table by copying the `- table:` block in
`resources/mysql_qbc.pipeline.yml` and changing `source_table`,
`primary_keys`, and `cursor_columns`.

`hard_deletion_sync_min_interval_in_seconds` enables Beta hard-deletion
tracking: the connector snapshots source primary keys and removes destination
rows that no longer exist. The value is a lower bound in seconds between those
scans. It requires `primary_keys` and is not supported with SCD Type 2.

## Validate and deploy

Choose an authenticated Databricks CLI profile and use it consistently:

```bash
databricks auth profiles
databricks bundle validate --strict --target dev --profile <profile>
databricks bundle deploy --target dev --profile <profile>
```

Start the deployed pipeline:

```bash
databricks bundle run mysql_qbc --target dev --profile <profile>
```

The first update snapshots each configured table. Later updates query rows
with a cursor value above the stored high-water mark. Query-based MySQL
ingestion runs on serverless compute.
