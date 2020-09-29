import argparse
import os

from purge.purge_data import purge_data


def trigger_purge(event, context):
    config_file_path = os.environ["CONFIG_FILE_PATH_IN_GCS"]
    purge_data(config_file_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_file_path", required=True, help="Config file path in GCS")

    args = parser.parse_args()
    purge_data(args.config_file_path)
