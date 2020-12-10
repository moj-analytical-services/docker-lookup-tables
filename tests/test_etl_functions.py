def test_end_to_end(s3, mocker):
    from etl.constants import (
        BUCKET_NAME,
        SOURCE_DIR,
        DATA_DIR,
        GITHUB_REPO,
        RELEASE,
    )
    from dataengineeringutils3.s3 import get_filepaths_from_s3_folder
    from etl.lookup_sync import LookupTableSync

    lookup_table_sync = LookupTableSync(
        BUCKET_NAME,
        SOURCE_DIR,
        DATA_DIR,
        GITHUB_REPO,
        RELEASE
    )
    lookup_table_sync.send_raw()

    # Check files uploaded to the correct place
    expected_s3_basepath = f"s3://{BUCKET_NAME}/{GITHUB_REPO}/{RELEASE}/"
    fps = get_filepaths_from_s3_folder(expected_s3_basepath)
    fps = [fp.replace(expected_s3_basepath, "") for fp in fps]

    expected_fps = [
        "data/lookup1/data.csv",
        "data/lookup1/meta.json",
        "data/lookup2/lookup2.csv",
        "data/lookup2/lookup2.json"
    ]

    assert sorted(fps) == sorted(expected_fps)
