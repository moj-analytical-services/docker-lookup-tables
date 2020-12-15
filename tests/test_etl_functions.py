import os
import pytest
import json


@pytest.mark.parametrize("overwrite_json", [True, False])
def test_end_to_end(setup_env_and_s3, overwrite_json):
    os.environ["GITHUB_REPO"] = "dummy_repo"
    os.environ["BUCKET_NAME"] = "moj-analytics-lookup-tables"

    print(os.listdir("data/lookup1/"))
    if overwrite_json:
        with open("data/database_overwrite.json", 'w') as f:
            overwrite_param = {
                "description": "test new desc",
                "bucket": "alpha-lookup-overwrite-bucket"
            }
            json.dump(overwrite_param, f)

    from etl.constants import (
        BUCKET_NAME,
        DATA_DIR,
        GITHUB_REPO,
        RELEASE,
    )
    from dataengineeringutils3.s3 import get_filepaths_from_s3_folder
    from etl.lookup_sync import LookupTableSync

    lookup_table_sync = LookupTableSync(
        BUCKET_NAME,
        DATA_DIR,
        GITHUB_REPO,
        RELEASE
    )
    lookup_table_sync.send_raw()

    # Check files uploaded to the correct place
    b = "alpha-lookup-overwrite-bucket" if overwrite_json else BUCKET_NAME
    expected_s3_basepath = f"s3://{b}/{GITHUB_REPO}/{RELEASE}/"
    fps = get_filepaths_from_s3_folder(expected_s3_basepath)
    fps = [fp.replace(expected_s3_basepath, "") for fp in fps]

    expected_fps = [
        "data/lookup1/data.csv",
        "data/lookup1/meta.json",
        "data/lookup2/lookup2.csv",
        "data/lookup2/lookup2.json"
    ]

    assert sorted(fps) == sorted(expected_fps)
