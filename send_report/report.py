import json
import os
from datetime import datetime, timedelta

from slack import WebClient
from slack.errors import SlackApiError

from common.utils import get_logger, GcsUri, load_config, get_blob

logger = get_logger()


def get_slack_credentials(slack_credentials_path: str):
    gcs_uri = GcsUri(slack_credentials_path)
    blob = get_blob(gcs_uri.bucket_name, gcs_uri.file_key)
    credentials = json.loads(blob.download_as_string())
    return credentials['channel'], credentials['token']


def send_report_slack(data: dict, bucket: str, channel: str, token: str):
    logger.info("Sending report on Slack")

    file_key = data['fileKey']
    if data['eventType'] == 'OBJECT_FINALIZE' and file_key.endswith('.csv'):
        event_time = data['eventTime']
        file_name = file_key.split("/")[-1]
        file_path = f"/tmp/{file_name}"

        blob = get_blob(bucket, file_key)
        with open(file_path, "wb") as file_obj:
            blob.download_to_file(file_obj)

        try:
            slack_client = WebClient(token=token)
            trash_date = data['trashDate']
            purge_date = (datetime.strptime(trash_date, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")

            text = f'Here is a DRS report generated on `{event_time}`. The data in this report will be trashed ' \
                f'on `{trash_date}` and purged on `{purge_date}`'
            slack_client.chat_postMessage(channel=channel, text=text)
            slack_client.files_upload(channels=channel, file=file_path, title="DRS Report")
        except SlackApiError as e:
            logger.error(f"Unable to send message due to {e.response['error']}")
        finally:
            os.remove(file_path)
    else:
        logger.warning("Unexpected data received in pubsub")


def send_report(data: dict, config_file_path: str):
    try:
        logger.info("Sending report")
        config = load_config(config_file_path)
        channel, token = get_slack_credentials(config['slack_credentials_path'])
        send_report_slack(data, config['report_bucket'], channel, token)
        logger.info("Report sent successfully!")
    except Exception as ex:
        logger.error(f"Unable to send report due to `{ex}`")
