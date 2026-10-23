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

From `dataset-engine/`, run the full integration suite with visible output using:

```sh
python -m pytest integration/tests -v -s
```

Live embedding checks use the OpenAI SDK against an OpenAI-compatible endpoint
such as Ollama. Configure `SAB_EMBED_BASE_URL`, `SAB_EMBED_MODEL`, and
`SAB_EMBED_API_KEY` in `integration/docker/.env` (see `.env.example`), and pull
the model first, for example `ollama pull bge-m3`. These checks embed text
extracted from the generated DOCX and PDF objects in MinIO; they skip when
`SAB_EMBED_BASE_URL` is empty.

The suite keeps focused contract checks for utilities in `main/` beside the
connector integration checks under `integration/tests/`. The utility checks
assert observable outputs for canonicalization, stable IDs and signatures, CSV
edge cases and stream cleanup, Excel sheet reading, database-row document
conversion, Markdown/text normalization, DOCX heading/list/table extraction,
PDF page extraction, and OpenAI-compatible embedding batching/validation. They
need no services. Connector checks use pytest
fixtures to start Postgres and MinIO from the same Compose file, wait until
both accept connections, run the seed-42 generator once per session, and
exercise the real connectors. They check row counts and foreign
key/object-key relationships, `open_object` across PDF/DOCX/CSV/XLSX, CSV,
Excel, PDF, and DOCX to Document conversion, and stable Postgres document
IDs/signatures. The service fixture
leaves development services running after the test run. The generator is
deterministic and safe to rerun for this seed.

Running plain `python -m pytest` executes the suite. For each new main utility
or processing step, add a focused contract test under `integration/tests/`;
extend it with fixture-backed connector coverage when the public path crosses
a real service. Prefer assertions about observable output and cross-source
links so tests catch data-shape regressions as well as connection failures.

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
