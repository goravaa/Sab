from pathlib import Path

import pytest

from main.process.workspace import (
    WorkspaceConfigError,
    load_workspace,
    workspace_view,
)


def write_workspace(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def test_load_workspace_reads_sources_and_optional_target_credentials(tmp_path):
    path = write_workspace(
        tmp_path / "workspace.yaml",
        """
dataset: personal
target:
  bucket: sab-data
  prefix: sab-datasets
  region: us-west-2
sources:
  - name: notes
    label: Notes bucket
    description: Markdown and PDF notes.
    connection:
      type: s3
      bucket: notes
      prefix: personal/
      profile: sab
""",
    )

    config = load_workspace(path)

    assert config.dataset == "personal"
    assert config.sources[0].label == "Notes bucket"
    assert config.sources[0].description == "Markdown and PDF notes."
    assert config.sources[0].connection.profile == "sab"
    assert config.target is not None
    assert config.target.bucket == "sab-data"

    view = workspace_view(config)
    assert view["target"]["uri"] == "s3://sab-data/sab-datasets/personal"
    assert view["target"]["auth"] == "default"
    assert view["sources"][0]["auth"] == "default"
    assert view["sources"][0]["location"] == "s3://notes/personal"


def test_load_workspace_expands_environment_variables(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_DB_PASSWORD", "secret-from-env")
    path = write_workspace(
        tmp_path / "workspace.yaml",
        """
dataset: personal
target:
  bucket: sab-data
sources:
  - name: db
    connection:
      type: postgres
      host: 127.0.0.1
      port: 5432
      database: app
      username: reader
      password: ${TEST_DB_PASSWORD}
""",
    )

    config = load_workspace(path)

    assert config.sources[0].connection.password.get_secret_value() == (
        "secret-from-env"
    )
    assert "secret-from-env" not in str(workspace_view(config))


def test_load_workspace_rejects_duplicate_source_names(tmp_path):
    path = write_workspace(
        tmp_path / "workspace.yaml",
        """
dataset: personal
target:
  bucket: sab-data
sources:
  - name: notes
    connection:
      type: s3
      bucket: notes
  - name: notes
    connection:
      type: s3
      bucket: other
""",
    )

    with pytest.raises(WorkspaceConfigError, match="unique"):
        load_workspace(path)


def test_load_workspace_reports_missing_file(tmp_path):
    with pytest.raises(WorkspaceConfigError, match="workspace config not found"):
        load_workspace(tmp_path / "missing.yaml")


def test_load_workspace_reports_missing_environment_variable(tmp_path):
    path = write_workspace(
        tmp_path / "workspace.yaml",
        """
dataset: personal
target:
  bucket: sab-data
sources:
  - name: db
    connection:
      type: postgres
      host: 127.0.0.1
      port: 5432
      database: app
      username: reader
      password: ${MISSING_WORKSPACE_SECRET}
""",
    )

    with pytest.raises(WorkspaceConfigError, match="MISSING_WORKSPACE_SECRET"):
        load_workspace(path)
