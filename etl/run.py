from constants import (
    BUCKET_NAME,
    DATA_DIR,
    META_DIR,
    RAW_DIR,
)



if __name__ == "__main__":
    lookup_table_sync = LookupTableSync(
        BUCKET_NAME,
        META_DIR,
        DATA_DIR,
        RAW_DIR
    )
    lookup_table_sync.sync()
