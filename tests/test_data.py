import json
import os

import pytest
import pandas as pd

from dataengineeringutils.pd_metadata_conformance import (
    check_pd_df_exactly_conforms_to_metadata,
)

from constants import (
    BUCKET_NAME,
    DATA_DIR,
    META_DIR,
    RAW_DIR,
)
from lookup_sync import LookupTableSync


@pytest.fixture
def lookup():
    lookup_table_sync = LookupTableSync(
        BUCKET_NAME,
        META_DIR,
        DATA_DIR,
        RAW_DIR
    )
    return lookup_table_sync


@pytest.fixture
def database():
    with open(f"{META_DIR}/database.json", "r") as f:
        return json.loads(f.read())


def test_database_schema(database):
    assert os.path.isfile(f"{META_DIR}/database.json")
    assert "description" in database
    assert "name" in database
    assert "bucket" in database
    assert "base_folder" in database
    assert database["base_folder"].startswith("database/")


def test_file_names_match_schema(lookup):
    for name, info in lookup.meta_and_files.items():
        assert info["meta_path"] is not None
        assert os.path.isfile(info["meta_path"])
        assert info["data_path"] is not None
        assert os.path.isfile(info["data_path"])
        assert name is not None


def test_data_matches_schema(lookup):
    for name, info in lookup.meta_and_files.items():
        with open(info["meta_path"], "r") as f:
            table_metadata = json.load(f)
        df = pd.read_csv(info["data_path"])
        assert check_pd_df_exactly_conforms_to_metadata(df, table_metadata) \
            is None
