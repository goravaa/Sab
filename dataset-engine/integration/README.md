# Connected supplier demo dataset

Generate a deterministic, synthetic B2B equipment supplier dataset with 1,000
customers, randomized orders and line items, invoices, shipments, and support
tickets. Postgres records and S3 documents share stable IDs and store direct
object keys, so a customer, order, invoice, shipment, product, or ticket can be
followed across both systems.

## Run against the local services

From `dataset-engine/`:

```sh
python -m pip install -r integration/requirements.txt
docker compose --env-file integration/docker/.env -f integration/docker/docker-compose.yaml up -d
python integration/data/generate_supplier_dataset.py
```

To run the integration checks from `dataset-engine/`:

```sh
python -m pytest
```

Pytest starts the Postgres and MinIO services from the same Compose file, waits
until both accept connections, runs the seed-42 generator once per test
session, and exercises the real connectors. It checks row counts and foreign
key/object-key relationships, `open_object` across PDF/DOCX/CSV/XLSX, CSV and
Excel row-to-Document conversion, and stable Postgres document IDs/signatures.
The fixture leaves the development services running after the test run. The
generator is deterministic and safe to rerun for this seed.

For each new connector, document reader, or processing step, add an integration
test that exercises its public path against these fixtures, then run
`python -m pytest` before moving on. Prefer assertions about observable output
and cross-source links so the tests catch wiring and data-shape regressions as
well as connection failures.

The script reads credentials from `integration/docker/.env` (or the process
environment), generates local files under `integration/generated/seed-0042/`,
upserts rows into the dedicated `supplier_demo` Postgres schema, and uploads
objects beneath `s3://sab-integration/supplier-demo/seed-0042/`. Re-running the
same seed refreshes its database IDs and object keys. Other schemas, seeds, and
S3 prefixes are left intact. Change the deterministic data with `--seed 7`; use
`--skip-postgres` or `--skip-s3` to generate only one side.

Default scale is 1,000 customers, 80 products, 1–5 orders per customer, 1–4
items per order, and randomized shipments and support cases. S3 contains DOCX
customer agreements and support case notes, a PDF invoice per order, one CSV
carrier tracking export, and an Excel workbook with product/inventory and order
summary sheets. The S3 keys are also saved on their matching Postgres records.

The generator verifies the completed data through `PostgresConnector` and
`S3Connector`, then prints row counts, a sample customer row, object counts,
sizes, and sample keys. All generated identities, companies, and documents are
fictional and use the reserved `example.test` email domain.
