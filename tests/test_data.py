import json
import os

import pytest

from etl.constants import (
    BUCKET_NAME,
    SOURCE_DIR,
    DATA_DIR,
    GITHUB_REPO,
    RELEASE,
)
from etl.lookup_sync import LookupTableSync


@pytest.fixture
def lookup(setup_env):
    lookup_table_sync = LookupTableSync(
        BUCKET_NAME,
        SOURCE_DIR,
        DATA_DIR,
        GITHUB_REPO,
        RELEASE,
    )
    return lookup_table_sync


@pytest.fixture
def database_overwrite(setup_env):
    if os.path.isfile(f"{DATA_DIR}/database_overwrite.json"):
        with open(f"{DATA_DIR}/database_overwrite.json", "r") as f:
            return json.loads(f.read())
    return {}


def test_database_schema(database_overwrite, lookup):
    if database_overwrite.get("description"):
        assert lookup.db_schema["description"] == \
               database_overwrite["description"]
    if database_overwrite.get("bucket"):
        assert lookup.db_schema["bucket"] == database_overwrite["bucket"]
    if not database_overwrite:
        assert lookup.db_schema["description"] == \
               f"A lookup table deployed from {GITHUB_REPO}"
        assert lookup.db_schema["bucket"] == BUCKET_NAME
    assert lookup.db_schema["base_folder"] == f"{GITHUB_REPO}/database"
    assert lookup.db_schema["name"] == GITHUB_REPO
    assert lookup.db_schema["base_folder"].endswith("/database")


def test_file_names_match_schema(lookup):
    assert len(lookup.meta_and_files.keys()) > 0
    for name, info in lookup.meta_and_files.items():
        assert info["meta_path"] is not None
        assert os.path.isfile(info["meta_path"])
        assert info["data_path"] is not None
        assert os.path.isfile(info["data_path"])
        assert name is not None
