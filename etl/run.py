from constants import BUCKET_NAME, DATA_DIR, GITHUB_REPO, RELEASE
from lookup_sync import LookupTableSync

if __name__ == "__main__":
    lookup_table_sync = LookupTableSync(BUCKET_NAME, DATA_DIR, GITHUB_REPO, RELEASE)

    lookup_table_sync.sync()
