import argparse
import os

from generate_report.report import generate_report


def trigger_generate_report(event, context):
    config_file_path = os.environ["CONFIG_FILE_PATH_IN_GCS"]
    generate_report(config_file_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_file_path", required=True, help="Config file path in GCS")

    args = parser.parse_args()
    generate_report(args.config_file_path)
