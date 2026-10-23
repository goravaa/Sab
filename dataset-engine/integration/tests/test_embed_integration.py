from __future__ import annotations

import pytest

from main.connect.object_storage.s3 import S3Config, S3Connector
from main.document.unstructured_data.documents import iter_unstructured_documents
from main.embed.embeddings import TextEmbedder
from main.schema.service import EmbedderConfig

PREFIX = "supplier-demo/seed-0042/"


pytestmark = pytest.mark.integration


def test_openai_sdk_embeds_real_files_through_live_endpoint(
    integration_data,
    object_by_key,
):
    embed_config = integration_data.embed_config

    if not embed_config["base_url"]:
        pytest.skip(
            "Set SAB_EMBED_BASE_URL, SAB_EMBED_MODEL, and SAB_EMBED_API_KEY "
            "in integration/docker/.env to run the live embedding test."
        )

    s3_config = integration_data.s3_config
    s3 = S3Connector(S3Config(
        type="s3",
        bucket=s3_config["bucket"],
        access_key_id=s3_config["access_key"],
        secret_access_key=s3_config["secret_key"],
        endpoint_url=s3_config["endpoint"],
        region="us-east-1",
        prefix=PREFIX,
    ))

    # Extract the actual DOCX and PDF stored in the generated S3 dataset.
    sources = [
        object_by_key[f"{PREFIX}customers/CUST-0042-000001/agreement.docx"],
        object_by_key[f"{PREFIX}invoices/INV-0042-0000006.pdf"],
    ]
    documents = []
    for source in sources:
        documents.extend(
            iter_unstructured_documents(
                s3,
                source,
                namespace="supplier-demo",
            )
        )

    assert len(documents) == 2

    texts = [document.raw_content for document in documents]
    texts.append("Which invoices are paid?")

    embedder = TextEmbedder(
        EmbedderConfig(
            model=embed_config["model"],
            batch_size=2,
        ),
        base_url=embed_config["base_url"],
        api_key=embed_config["api_key"],
    )

    vectors = embedder.embed_texts(texts)

    assert len(vectors) == len(texts)

    dimensions = {len(vector) for vector in vectors}
    assert len(dimensions) == 1
    assert dimensions.pop() > 0

    assert vectors[0] != vectors[1]
    assert vectors[2] != vectors[0]

    query_vector = embedder.embed_text("Which invoices are paid?")
    assert len(query_vector) == len(vectors[0])
