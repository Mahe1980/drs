from datetime import datetime, timedelta
from unittest import mock

import pytest
from slack.errors import SlackApiError

from send_report.report import get_slack_credentials, send_report_slack, send_report

test_send_slack_message_data = [({"eventType": "OBJECT_FINALIZE",
                                  "eventTime": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                                  "fileBucket": "test",
                                  "fileKey": "test/test.csv",
                                  "trashDate": (datetime.utcnow() + timedelta(days=7)).strftime("%Y-%m-%d"),
                                  }, "vf-dev-ca-lab-dev", "test_channel", "test_token")]

test_send_slack_message_invalid_data_data = [({"eventType": "INVALID",
                                               "eventTime": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                                               "fileBucket": "test",
                                               "fileKey": "test/test.txt",
                                               "trashDate": (datetime.utcnow() + timedelta(days=7)).strftime(
                                                   "%Y-%m-%d"),
                                               }, "vf-dev-ca-lab-dev", "test_channel", "test_token")]

test_send_report_data = [({"eventType": "OBJECT_FINALIZE",
                           "eventTime": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                           "fileBucket": "test",
                           "fileKey": "test/test.csv",
                           "trashDate": (datetime.utcnow() + timedelta(days=7)).strftime("%Y-%m-%d"),
                           }, "gs://test/config/config.yaml")]

test_send_report_exception_data = [({"test": "test"}, "test.yaml")]

test_get_slack_credentials_data = [
    ('gs://test/secure/slack.json', '{"channel": "test_channel", "token": "test_token"}')]


@pytest.mark.parametrize("slack_credentials_path, json_string", test_get_slack_credentials_data)
@mock.patch("google.cloud.storage.Client")
def test_get_slack_credentials(mock_storage_client, slack_credentials_path, json_string):
    mock_storage_client().get_bucket().blob().download_as_string.return_value = json_string
    channel, token = get_slack_credentials(slack_credentials_path)
    assert channel == "test_channel"
    assert token == "test_token"


@pytest.mark.parametrize("data, bucket, channel, token", test_send_slack_message_data)
@mock.patch('send_report.report.os.remove')
@mock.patch('send_report.report.WebClient')
@mock.patch("google.cloud.storage.Client")
def test_send_report_slack(mock_storage_client, mock_slack_client, mock_remove, data, bucket, channel, token):
    trash_date = data['trashDate']
    purge_date = (datetime.strptime(trash_date, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")

    text = f'Here is a DRS report generated on `{data["eventTime"]}`. The data in this report will be trashed ' \
        f'on `{trash_date}` and purged on `{purge_date}`'

    with mock.patch("send_report.report.open", create=True) as mock_file:
        send_report_slack(data, bucket, channel, token)

        tmp_file = "/tmp/test.csv"
        mock_storage_client().get_bucket.assert_called_with(bucket)
        mock_storage_client().get_bucket().blob.assert_called_with(data['fileKey'])
        mock_storage_client().get_bucket().blob().download_to_file.assert_called()
        mock_slack_client().chat_postMessage.assert_called_with(channel=channel, text=text)
        mock_slack_client().files_upload.assert_called_with(channels=channel, file=tmp_file, title="DRS Report")
        mock_file.assert_called_with(tmp_file, "wb")
        mock_remove.assert_called_with(tmp_file)


@pytest.mark.parametrize("data, bucket, channel, token", test_send_slack_message_data)
@mock.patch('send_report.report.os.remove')
@mock.patch('send_report.report.WebClient')
@mock.patch("google.cloud.storage.Client")
def test_send_report_slack_slack_api_error(mock_storage_client, mock_slack_client, mock_remove, data, bucket,
                                           channel, token):
    trash_date = data['trashDate']
    purge_date = (datetime.strptime(trash_date, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")

    text = f'Here is a DRS report generated on `{data["eventTime"]}`. The data in this report will be trashed ' \
        f'on `{trash_date}` and purged on `{purge_date}`'
    with mock.patch("send_report.report.open", create=True) as mock_file:
        mock_slack_client().chat_postMessage.side_effect = SlackApiError('Error', {'error': "Channel not found"})
        send_report_slack(data, bucket, channel, token)

        mock_storage_client().get_bucket.assert_called_with(bucket)
        mock_storage_client().get_bucket().blob.assert_called_with(data['fileKey'])
        mock_storage_client().get_bucket().blob().download_to_file.assert_called()
        mock_slack_client().chat_postMessage.assert_called_with(channel=channel, text=text)
        mock_slack_client().files_upload.assert_not_called()
        mock_file.assert_called_with("/tmp/test.csv", "wb")
        mock_remove.assert_called_with("/tmp/test.csv")


@pytest.mark.parametrize("data, bucket, channel, token", test_send_slack_message_invalid_data_data)
@mock.patch('send_report.report.WebClient')
@mock.patch("google.cloud.storage.Client")
def test_send_report_slack_invalid_data(mock_storage_client, mock_slack_client, data, bucket, channel, token):
    send_report_slack(data, bucket, channel, token)

    mock_storage_client().get_bucket.assert_not_called()
    mock_slack_client().chat_postMessage.assert_not_called()
    mock_slack_client().files_upload.assert_not_called()


@pytest.mark.parametrize("data, config_file_path", test_send_report_data)
@mock.patch('send_report.report.send_report_slack')
def test_send_report(send_report_slack_function, data, config_file_path, monkeypatch):
    def mock_load_config(config_file_path):
        config = dict()
        config['project'] = "test"
        config['topic'] = "test"
        config['report_bucket'] = "test_bucket"
        config['report_dataset'] = "test_dataset"
        config['report_table'] = "test_table"
        config['datastore_config'] = "test"
        config['datastore_config'] = "test"
        config['slack_credentials_path'] = "gs://test/test.json"
        return config

    def mock_get_slack_credentials(bucket):
        return 'test_channel', 'test_token'

    monkeypatch.setattr('send_report.report.load_config', mock_load_config)
    monkeypatch.setattr('send_report.report.get_slack_credentials', mock_get_slack_credentials)
    config_dict = mock_load_config(config_file_path)
    channel, token = mock_get_slack_credentials(config_dict['slack_credentials_path'])
    send_report(data, config_file_path)
    send_report_slack_function.assert_called_with(data, config_dict['report_bucket'], channel, token)


@pytest.mark.parametrize("data, config_file", test_send_report_exception_data)
@mock.patch('send_report.report.send_report_slack')
def test_send_report_exception(send_report_slack_function, data, config_file):
    send_report(data, config_file)
    send_report_slack_function.assert_not_called()
