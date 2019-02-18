import os


DATA_DIR = 'data'
META_DIR = 'meta'


def find_meta_and_data_files():
    """get meta and data files"""
    metas = os.listdir(META_DIR)
    data = os.listdir(DATA_DIR)

    meta_and_data = {}
    for data_path in data:
        name = data_path[:data_path.rfind('.')]
        meta_path = next((m for m in metas if m.startswith(name)), None)
        meta_and_data[name] = {
            "meta_path": meta_path,
            "": data_path
        }
    return meta_and_data


def validate_row():
    """Validate data in row to make sure it conforms to meta data"""


def parse_file():
    """parse file in rows or chunks"""


def create_dataset():
    """create_dataset"""


def create_timestamped_dataset():
    """
    create timestamped version of datasset

    If we want to have access to versioned datasets.
    """


def replace_latest_dataset():
    """create or replace latest version"""


def send_to_s3():
    """send data to s3/athena - maybe requires processing in glue"""


if __name__ == '__main__':
    print('Run etl')
