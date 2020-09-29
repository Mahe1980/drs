import argparse
import json
import os
from datetime import datetime, timedelta

from common.utils import GcsUri, ReportEvent
from send_report.report import send_report


def trigger_send_report(event, context):
    data = json.loads(event["attributes"]['attrs'])
    config_file_path = os.environ["CONFIG_FILE_PATH_IN_GCS"]
    send_report(data, config_file_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_file_path", required=True, help="Config file path in GCS")
    parser.add_argument("--report_file_path", required=True, help="Report file path in GCS")

    args = parser.parse_args()
    datetime_utcnow = datetime.utcnow()
    gcs_uri = GcsUri(args.report_file_path)

    report_event = ReportEvent(datetime_utcnow.strftime("%Y-%m-%dT%H:%M:%S"), gcs_uri.bucket_name, gcs_uri.file_key,
                               (datetime_utcnow + timedelta(days=7)).strftime('%Y-%m-%d'))
    event_dict = report_event.to_json()

    send_report(event_dict, args.config_file_path)
