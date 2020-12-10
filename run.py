from etl.constants import (
    BUCKET_NAME,
    SOURCE_DIR,
    DATA_DIR,
    GITHUB_REPO,
    RELEASE,
)
from etl.lookup_sync import LookupTableSync

if __name__ == "__main__":
    lookup_table_sync = LookupTableSync(
        BUCKET_NAME,
        SOURCE_DIR,
        DATA_DIR,
        GITHUB_REPO,
        RELEASE
    )

    lookup_table_sync.sync()
