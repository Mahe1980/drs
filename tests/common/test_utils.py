from datetime import datetime, timedelta
from unittest import mock

import pytest

from common.utils import GcsUri, ReportEvent, load_config, get_blob, collect_partitions_metadata, \
    execute_query, update_audit_table, get_update_query_successful_gcs, get_update_query_successful_bq, \
    get_update_query_error_gcs, get_update_query_error_bq, GCSPartitionMetadata, BQPartitionMetadata

test_gcs_uri_data = [("gs://test/test_path/test_path/parquet/year=2020/month=07/day=25", 'test',
                      'test_path/test_path/parquet/year=2020/month=07/day=25')]

test_get_blob_data = [('test_bucket', 'test/test.json')]

test_load_config_data = [
    ("gs://test/config/config.yaml", "test_project", "drs_report", "test_bucket", "test/report", "test_dataset",
     "test_table", "test_status_table", {"namespace": "test_namespace", "kind": "test_kind"},
     "gs://test/secure/test.json", """
                                        drs:
                                          project:  test_project
                                        gcs:
                                          report_bucket:  test_bucket
                                          report_location: test/report
                                        big_query:
                                          report_dataset:  test_dataset
                                          report_table:  test_table
                                          status_table:  test_status_table
                                        pubsub:
                                          report_topic: drs_report
                                        datastore:
                                          namespace:  test_namespace
                                          kind:  test_kind
                                        slack_credentials:
                                          path: gs://test/secure/test.json""")]

test_report_event_data = [("test_bucket", "test/test/text.csv")]

test_update_columns = [(["trashed_date"]), (["purged_date"])]

test_bq_partition_metadata_data = [("111-222-abc", datetime.strptime("2020-08-14", "%Y-%m-%d").date(),
                                    "test_project", "test_project.test_dataset.test_table", "partition_year_month_day",
                                    "test_project.test_dataset_trash.test_table_trash")]

test_gcs_partition_metadata_data = [("111-222-abc", datetime.strptime("2020-08-14", "%Y-%m-%d").date(),
                                     "test_project", "test-bucket", "test-bucket-trash",
                                     "/test/parquet/year=2020/month=8/day=13/")]

test_collect_partitions_metadata_data = [
    ("111-222-abc", datetime.strptime("2020-08-14", "%Y-%m-%d").date(), "test_project",
     "test_dataset", "test_table", "test_partition_column", "gs://test/test/parquet/year=2020/month=8/day=13/"),
    ("111-222-abc", datetime.strptime("2020-08-14", "%Y-%m-%d").date(), "test_project",
     "test_dataset", "test_table", "test_partition_column", None),
    ("111-222-abc", datetime.strptime("2020-08-14", "%Y-%m-%d").date(), "test_project",
     None, None, None, "gs://test/test/parquet/year=2020/month=8/day=13/")]

test_execute_query_data = [('2020-08-18', 200)]

test_update_audit_table_data = [('test_project', 'test_dataset', 'test_table',
                                 {'gcs_errors': [{'uid': '000', 'error_gcs': 'Unable to load'}],
                                  'gcs_uids': ['000', '111', '222'],
                                  'error_gcs_column': 'error_gcs', 'date_gcs_column': 'trashed_date_gcs'},
                                 {'bq_errors': [{'uid': '444', 'error_bq': 'Unable to load'}],
                                  'bq_uids': ['222', '333', '444'],
                                  'error_bq_column': 'error_bq', 'date_bq_column': 'trashed_date_bq'}),
                                ('test_project', 'test_dataset', 'test_table',
                                 {'gcs_errors': [{'uid': '000', 'error_gcs': 'Unable to load'}],
                                  'gcs_uids': ['000', '111', '222'],
                                  'error_gcs_column': 'error_gcs', 'date_gcs_column': 'purged_date_bq'},
                                 {'bq_errors': [{'uid': '444', 'error_bq': 'Unable to load'}],
                                  'bq_uids': ['222', '333', '444'],
                                  'error_bq_column': 'error_bq', 'date_bq_column': 'purged_date_bq'})]

test_get_update_query_successful_gcs_data = [
    ('test_project', 'test_dataset', 'test_table', 'trashed_date_gcs', ['111', '222'],
     "update `test_project.test_dataset.test_table` set trashed_date_gcs=current_timestamp, error_gcs='' where "
     "uid in ('111','222')"),
    ('test_project', 'test_dataset', 'test_table', 'purged_date_gcs', ['111', '222'],
     "update `test_project.test_dataset.test_table` set purged_date_gcs=current_timestamp, error_gcs='' where "
     "uid in ('111','222') and trashed_date_gcs is not null")]

test_get_update_query_successful_bq_data = [
    ('test_project', 'test_dataset', 'test_table', 'trashed_date_bq', ['111', '222'],
     "update `test_project.test_dataset.test_table` set trashed_date_bq=current_timestamp, error_bq='' where "
     "uid in ('111','222')"),
    ('test_project', 'test_dataset', 'test_table', 'purged_date_bq', ['111', '222'],
     "update `test_project.test_dataset.test_table` set purged_date_bq=current_timestamp, error_bq='' where "
     "uid in ('111','222') and trashed_date_bq is not null")]

test_get_update_query_error_gcs = [("test_project", "test_dataset", "test_table",
                                    "update `test_project.test_dataset.test_table` a set error_gcs=e.error_gcs "
                                    "from `test_project.test_dataset.error_tbl` e where a.uid=e.uid")]

test_get_update_query_error_bq = [("test_project", "test_dataset", "test_table",
                                   "update `test_project.test_dataset.test_table` a set error_bq=e.error_bq "
                                   "from `test_project.test_dataset.error_tbl` e where a.uid=e.uid")]


@pytest.mark.parametrize("uri, expected_bucket_name, expected_file_key", test_gcs_uri_data)
def test_gcs_uri(uri, expected_bucket_name, expected_file_key):
    gcs_uri_obj = GcsUri(uri)
    assert gcs_uri_obj.file_key == expected_file_key
    assert gcs_uri_obj.bucket_name == expected_bucket_name


@pytest.mark.parametrize("bucket_name, file_key", test_report_event_data)
def test_report_event(bucket_name, file_key):
    datetime_utcnow = datetime.utcnow()
    event_time = datetime_utcnow.strftime("%Y-%m-%dT%H:%M:%S")
    trash_date = (datetime_utcnow + timedelta(days=7)).strftime('%Y-%m-%d')
    report_event = ReportEvent(event_time, bucket_name, file_key, trash_date)
    event_dict = report_event.to_json()
    assert event_dict['eventType'] == "OBJECT_FINALIZE"
    assert event_dict['eventTime'] == event_time
    assert event_dict['fileBucket'] == bucket_name
    assert event_dict['fileKey'] == file_key
    assert event_dict['trashDate'] == trash_date


@pytest.mark.parametrize("bucket, file_key", test_get_blob_data)
@mock.patch("google.cloud.storage.Client")
def test_get_blob(mock_storage_client, bucket, file_key):
    get_blob(bucket, file_key)
    mock_storage_client().get_bucket.assert_called_with(bucket)
    mock_storage_client().get_bucket().blob.assert_called_with(file_key)


@pytest.mark.parametrize("partition_date, row_count", test_execute_query_data)
@mock.patch("google.cloud.bigquery.Client")
def test_execute_query(mock_client, partition_date, row_count):
    mock_client().query().result.return_value = [(partition_date, row_count)]
    query = 'select partition_date, row_count from test.test.test'
    results = execute_query(mock_client(), query)
    assert results == [(partition_date, row_count)]


@pytest.mark.parametrize(
    "config_file_path, project, topic, report_bucket, report_location, report_dataset, report_table, status_table, "
    "datastore_config, slack_credentials_path, yaml_str", test_load_config_data)
@mock.patch("google.cloud.storage.Client")
def test_load_config(mock_storage_client, config_file_path, project, topic, report_bucket, report_location,
                     report_dataset, report_table, status_table, datastore_config, slack_credentials_path, yaml_str):
    mock_storage_client().get_bucket().blob().download_as_string.return_value = yaml_str
    actual = load_config(config_file_path)
    assert actual['project'] == project
    assert actual['topic'] == topic
    assert actual['report_bucket'] == report_bucket
    assert actual['report_location'] == report_location
    assert actual['report_dataset'] == report_dataset
    assert actual['report_table'] == report_table
    assert actual['status_table'] == status_table
    assert actual['datastore_config'] == datastore_config
    assert actual['slack_credentials_path'] == slack_credentials_path


@pytest.mark.parametrize(
    "uid, partition_date, project_id, source_bucket_name, trash_bucket_name, partition_path",
    test_gcs_partition_metadata_data)
def test_gcs_partition_metadata(uid, partition_date, project_id, source_bucket_name, trash_bucket_name, partition_path):
    partition_metadata = GCSPartitionMetadata(uid, partition_date, project_id, source_bucket_name, trash_bucket_name,
                                              partition_path)
    assert partition_metadata.uid == uid
    assert partition_metadata.partition_date == partition_date
    assert partition_metadata.project_id == project_id
    assert partition_metadata.source_bucket_name == source_bucket_name
    assert partition_metadata.trash_bucket_name == trash_bucket_name
    assert partition_metadata.partition_path == partition_path


@pytest.mark.parametrize(
    "uid, partition_date, project_id, source_table_id, partition_column, trash_table_id",
    test_bq_partition_metadata_data)
def test_bq_partition_metadata(uid, partition_date, project_id, source_table_id, partition_column, trash_table_id):
    partition_metadata = BQPartitionMetadata(uid, partition_date, project_id, source_table_id, partition_column,
                                             trash_table_id)
    assert partition_metadata.uid == uid
    assert partition_metadata.partition_date == partition_date
    assert partition_metadata.project_id == project_id
    assert partition_metadata.source_table_id == source_table_id
    assert partition_metadata.partition_column == partition_column
    assert partition_metadata.trash_table_id == trash_table_id


@pytest.mark.parametrize("uid, partition_date, project_id, dataset, table_name, partition_column, partition_path",
                         test_collect_partitions_metadata_data)
@mock.patch("google.cloud.bigquery.Client")
@mock.patch("google.cloud.bigquery.table.Row")
def test_collect_partitions_metadata(mock_row, mock_client, uid, partition_date, project_id, dataset, table_name,
                                     partition_column, partition_path):
    mock_row.uid = uid
    mock_row.partition_date = partition_date
    mock_row.project_id = project_id
    mock_row.dataset = dataset
    mock_row.table_name = table_name
    mock_row.partition_column = partition_column
    mock_row.partition_path = partition_path
    mock_client().query().result.return_value = [mock_row]
    select_query = "SELECT id, partition_date, project_id, dataset, table_name, partition_column," \
                   "partition_path from `test.test.test` " \
                   "WHERE format_datetime(\'%Y-%m-%d\',datetime(report_date)) = '2020-07-01' " \
                   "and anomaly_detected = false "

    partitions_metadata = collect_partitions_metadata(project_id, select_query)
    assert len(partitions_metadata) == 2
    if partitions_metadata['bq']:
        assert partitions_metadata['bq'][0].uid == uid
        assert partitions_metadata['bq'][0].partition_date == partition_date
        assert partitions_metadata['bq'][0].project_id == project_id
        assert partitions_metadata['bq'][0].source_table_id == f"{project_id}.{dataset}.{table_name}"
        assert partitions_metadata['bq'][0].partition_column == partition_column
        assert partitions_metadata['bq'][0].trash_table_id == f"{project_id}.{dataset}_trash.{table_name}_trash"
    if partitions_metadata['gcs']:
        assert partitions_metadata['gcs'][0].uid == uid
        assert partitions_metadata['gcs'][0].partition_date == partition_date
        assert partitions_metadata['gcs'][0].project_id == project_id
        assert partitions_metadata['gcs'][0].source_bucket_name == GcsUri(partition_path).bucket_name
        assert partitions_metadata['gcs'][0].trash_bucket_name == f"{GcsUri(partition_path).bucket_name}-trash"
        assert partitions_metadata['gcs'][0].partition_path == GcsUri(partition_path).file_key


@pytest.mark.parametrize(
    "project, report_dataset, report_table, date_gcs_column, gcs_successful_uids, expected_update_query",
    test_get_update_query_successful_gcs_data)
def test_get_update_query_successful_gcs(project, report_dataset, report_table, date_gcs_column, gcs_successful_uids,
                                         expected_update_query):
    actual_update_query = get_update_query_successful_gcs(project, report_dataset, report_table, date_gcs_column,
                                                          gcs_successful_uids)
    assert actual_update_query == expected_update_query


@pytest.mark.parametrize(
    "project, report_dataset, report_table, date_bq_column, bq_successful_uids, expected_update_query",
    test_get_update_query_successful_bq_data)
def test_get_update_query_successful_bq(project, report_dataset, report_table, date_bq_column, bq_successful_uids,
                                        expected_update_query):
    actual_update_query = get_update_query_successful_bq(project, report_dataset, report_table, date_bq_column,
                                                         bq_successful_uids)
    assert actual_update_query == expected_update_query


@pytest.mark.parametrize("project, report_dataset, report_table, expected_update_query",
                         test_get_update_query_error_gcs)
def test_get_update_query_error_gcs(project, report_dataset, report_table, expected_update_query):
    actual_update_query = get_update_query_error_gcs(project, report_dataset, report_table)
    assert actual_update_query == expected_update_query


@pytest.mark.parametrize("project, report_dataset, report_table, expected_update_query",
                         test_get_update_query_error_bq)
def test_get_update_query_error_bq(project, report_dataset, report_table, expected_update_query):
    actual_update_query = get_update_query_error_bq(project, report_dataset, report_table)
    assert actual_update_query == expected_update_query


@pytest.mark.parametrize("project, report_dataset, report_table, gcs_update_data, bq_update_data",
                         test_update_audit_table_data)
@mock.patch("pandas.DataFrame.to_gbq")
@mock.patch("google.cloud.bigquery.Client")
def test_update_audit_table(mock_client, to_gbq, project, report_dataset, report_table, gcs_update_data,
                            bq_update_data):
    update_audit_table(project, report_dataset, report_table, gcs_update_data, bq_update_data)
    assert mock_client().query().result.call_count == 6
