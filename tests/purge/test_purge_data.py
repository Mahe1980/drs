from datetime import datetime
from unittest import mock

import pytest
from google.cloud.exceptions import NotFound

from common.utils import GcsUri
from purge.purge_data import purge_bigquery_data, BQPartitionMetadata, GCSPartitionMetadata, get_delete_query, \
    collect_partitions_metadata_query_for_purge, purge_gcs_data, purge_data

test_purge_bigquery_data_data = [
    ("111", datetime.strptime('2020-06-10', '%Y-%m-%d').date(), 'test', 'test.test.test', 'partition_year_month_day',
     'test.test_purge.test_purge')]

test_get_bucket_data = [('test_bucket', 'eu-west-1')]

test_purge_gcs_data_data = [("111", datetime.strptime('2020-06-10', '%Y-%m-%d').date(), 'test',
                             'test-bucket', 'test-bucket-trash', 'parquet/year=2020/month=6/day=6/')]

test_collect_partitions_to_be_purged_query_data = [('test_project', 'test_dataset', 'test_table')]

test_purge_data_data = [('test.yaml', {"uid": '111', "partition_date": "2020-08-18", "project_id": '111',
                                       "dataset": "test_dataset", "table_name": "test_table_name",
                                       "partition_column": "test_partition_column",
                                       "partition_path": "gs://test/test/parquet/year=2020/month=7/day=10/"
                                       })]

test_get_delete_query_data = [
    ('test_project.test_dataset.test_table', 'partition_year_month_day', datetime.now().date(),
     f"delete from `test_project.test_dataset.test_table` where "
     f"partition_year_month_day=TIMESTAMP '{datetime.now().date()}'")]


@pytest.mark.parametrize("trash_table_id, partition_column, partition_date, expected_delete_query",
                         test_get_delete_query_data)
def test_get_delete_query(trash_table_id, partition_column, partition_date, expected_delete_query):
    actual_delete_query = get_delete_query(trash_table_id, partition_column, partition_date)
    assert actual_delete_query == expected_delete_query


@pytest.mark.parametrize(
    "uid, partition_date, project_id, source_table_id, partition_column, purge_table_id", test_purge_bigquery_data_data)
@mock.patch("google.cloud.bigquery.table.Row")
@mock.patch("google.cloud.bigquery.Client")
def test_purge_bigquery_data(mock_client, mock_row, uid, partition_date, project_id, source_table_id,
                             partition_column, purge_table_id):
    bq_partitions_to_be_purged = BQPartitionMetadata(uid, partition_date, project_id, source_table_id,
                                                     partition_column, purge_table_id)
    mock_client().query().result.side_effect = [[mock_row], None, None]
    purge_bigquery_data(bq_partitions_to_be_purged)
    assert mock_client().query().result.call_count == 1


@pytest.mark.parametrize(
    "uid, partition_date, project_id, source_table_id, partition_column, purge_table_id", test_purge_bigquery_data_data)
@mock.patch("google.cloud.bigquery.Client")
def test_purge_bigquery_data_exception(mock_client, uid, partition_date, project_id,
                                       source_table_id, partition_column, purge_table_id):
    bq_partitions_to_be_purged = BQPartitionMetadata(uid, partition_date, project_id, source_table_id,
                                                     partition_column, purge_table_id)
    mock_client().query().result.side_effect = [Exception('Unable to delete')]
    result = purge_bigquery_data(bq_partitions_to_be_purged)
    assert mock_client().query().result.call_count == 1
    assert result['uid'] == uid
    assert result['error_bq'] == 'Unable to delete'


@pytest.mark.parametrize(
    "uid, partition_date, project_id, source_bucket_name, target_bucket_name, partition_path", test_purge_gcs_data_data)
@mock.patch("google.cloud.storage.blob.Blob")
@mock.patch("google.cloud.storage.blob.Blob")
@mock.patch("google.cloud.storage.Client")
def test_purge_gcs_data(mock_client, mock_blob_file1, mock_blob_file2, uid, partition_date, project_id,
                        source_bucket_name, target_bucket_name, partition_path):
    gcs_partitions_to_be_purged = GCSPartitionMetadata(uid, partition_date, project_id, source_bucket_name,
                                                       target_bucket_name, partition_path)
    mock_blob_file1.name = f"{partition_path}/test.parquet"
    trash_bucket = mock_client().get_bucket('trash_bucket')
    mock_client().get_bucket().list_blobs.return_value = [mock_blob_file1, mock_blob_file2]
    trash_bucket.delete_blob.return_value = [None, NotFound("File Not Found")]
    purge_gcs_data(gcs_partitions_to_be_purged)
    trash_bucket.delete_blob.assert_called_with(mock_blob_file2.name)
    assert trash_bucket.delete_blob.call_count == 2


@pytest.mark.parametrize(
    "uid, partition_date, project_id, source_bucket_name, target_bucket_name, partition_path", test_purge_gcs_data_data)
@mock.patch("google.cloud.storage.Client")
def test_purge_gcs_data_exception(mock_client, uid, partition_date, project_id, source_bucket_name, target_bucket_name,
                                  partition_path):
    gcs_partitions_to_be_purged = GCSPartitionMetadata(uid, partition_date, project_id, source_bucket_name,
                                                       target_bucket_name, partition_path)
    trash_bucket = mock_client().get_bucket('trash_bucket')
    mock_client().get_bucket.side_effect = [Exception('Unable to get trash bucket')]
    result = purge_gcs_data(gcs_partitions_to_be_purged)
    trash_bucket.delete_blob.assert_not_called()
    assert result['uid'] == uid
    assert result['error_gcs'] == 'Unable to get trash bucket'


@pytest.mark.parametrize("project, report_dataset, report_table", test_collect_partitions_to_be_purged_query_data)
def test_collect_partitions_to_be_purged_query(project, report_dataset, report_table):
    expected_query = f"SELECT uid, format_datetime('%Y-%m-%d',datetime(partition_date)) as partition_date," \
        f"project_id, dataset, table_name, partition_column, partition_path " \
        f"FROM `{project}.{report_dataset}.{report_table}` " \
        f"WHERE expected_trash_date is not null and " \
        f"((trashed_date_gcs is not null and purged_date_gcs is null) or " \
        f"(trashed_date_bq is not null and purged_date_bq is null)) and " \
        f"format_datetime(\'%Y-%m-%d\',datetime(expected_purge_date)) <= '{datetime.utcnow().date()}' "
    actual_query = collect_partitions_metadata_query_for_purge(project, report_dataset, report_table)
    assert actual_query == expected_query


@pytest.mark.parametrize("config_file, partition_metadata", test_purge_data_data)
@mock.patch("purge.purge_data.multiprocess_function")
@mock.patch("google.cloud.storage.Client")
@mock.patch("google.cloud.bigquery.Client")
def test_purge_data(mock_bq_client, mock_gcs_client, multiprocess_function, config_file, partition_metadata,
                    monkeypatch):
    def mock_load_config(config_file_path):
        config = dict()
        config['project'] = "test"
        config['topic'] = "test"
        config['report_bucket'] = "test_bucket"
        config['report_location'] = "test/report"
        config['report_dataset'] = "test_dataset"
        config['report_table'] = "test_table"
        config['status_table'] = "status_table"
        config['datastore_config'] = "test"
        config['datastore_config'] = "test"
        config['slack_credentials_path'] = "gs://test/test.json"
        return config

    def mock_collect_partitions_metadata(bq_client, select_query):
        uid = partition_metadata['uid']
        partition_date = partition_metadata['partition_date']
        project_id = partition_metadata['project_id']
        dataset = partition_metadata['dataset']
        table_name = partition_metadata['table_name']
        partition_column = partition_metadata['partition_column']
        gcs_uri = GcsUri(partition_metadata['partition_path'])
        source_bucket_name = gcs_uri.bucket_name
        trash_bucket_name = f"{source_bucket_name}-trash"
        partition_path = gcs_uri.file_key
        gcs_partitions_metadata = [
            GCSPartitionMetadata(uid, partition_date, project_id, source_bucket_name, trash_bucket_name,
                                 partition_path)]
        bq_partitions_metadata = [
            BQPartitionMetadata(uid, partition_date, project_id, f"{project_id}.{dataset}.{table_name}",
                                partition_column, f"{project_id}.{dataset}_trash.{table_name}_trash")]
        partitions_metadata = {'gcs': gcs_partitions_metadata, 'bq': bq_partitions_metadata}

        return partitions_metadata

    def mock_purge_bigquery_data(bq_partition_to_be_purged):
        return

    def mock_update_audit_table(project, report_dataset, report_table, gcs_update_data, bq_update_data):
        return

    monkeypatch.setattr('purge.purge_data.load_config', mock_load_config)
    monkeypatch.setattr('purge.purge_data.collect_partitions_metadata', mock_collect_partitions_metadata)
    monkeypatch.setattr('purge.purge_data.purge_bigquery_data', mock_purge_bigquery_data)
    monkeypatch.setattr('purge.purge_data.update_audit_table', mock_update_audit_table)

    purge_data(config_file)


@pytest.mark.parametrize("config_file, partition_metadata", test_purge_data_data)
@mock.patch("purge.purge_data.multiprocess_function")
@mock.patch("google.cloud.storage.Client")
@mock.patch("google.cloud.bigquery.Client")
def test_purge_data_with_errors(mock_bq_client, mock_gcs_client, multiprocess_function, config_file, partition_metadata,
                                monkeypatch):
    def mock_load_config(config_file_path):
        config = dict()
        config['project'] = "test"
        config['topic'] = "test"
        config['report_bucket'] = "test_bucket"
        config['report_location'] = "test/report"
        config['report_dataset'] = "test_dataset"
        config['report_table'] = "test_table"
        config['status_table'] = "status_table"
        config['datastore_config'] = "test"
        config['datastore_config'] = "test"
        config['slack_credentials_path'] = "gs://test/test.json"
        return config

    def mock_collect_partitions_metadata(bq_client, select_query):
        uid = partition_metadata['uid']
        partition_date = partition_metadata['partition_date']
        project_id = partition_metadata['project_id']
        dataset = partition_metadata['dataset']
        table_name = partition_metadata['table_name']
        partition_column = partition_metadata['partition_column']
        gcs_uri = GcsUri(partition_metadata['partition_path'])
        source_bucket_name = gcs_uri.bucket_name
        trash_bucket_name = f"{source_bucket_name}-trash"
        partition_path = gcs_uri.file_key
        gcs_partitions_metadata = [
            GCSPartitionMetadata(uid, partition_date, project_id, source_bucket_name, trash_bucket_name,
                                 partition_path)]
        bq_partitions_metadata = [
            BQPartitionMetadata(uid, partition_date, project_id, f"{project_id}.{dataset}.{table_name}",
                                partition_column, f"{project_id}.{dataset}_trash.{table_name}_trash")]
        partitions_metadata = {'gcs': gcs_partitions_metadata, 'bq': bq_partitions_metadata}

        return partitions_metadata

    def mock_purge_bigquery_data(bq_partition_to_be_purged):
        uid = partition_metadata['uid']
        return {'uid': uid, 'error_bq': 'Unable to purge table'}

    def mock_update_audit_table(project, report_dataset, report_table, gcs_update_data, bq_update_data):
        return

    multiprocess_function.return_value = [
        {'uid': partition_metadata['uid'], 'error_bq': 'Unable to purge partition path'}]
    monkeypatch.setattr('purge.purge_data.load_config', mock_load_config)
    monkeypatch.setattr('purge.purge_data.collect_partitions_metadata', mock_collect_partitions_metadata)
    monkeypatch.setattr('purge.purge_data.purge_bigquery_data', mock_purge_bigquery_data)
    monkeypatch.setattr('purge.purge_data.update_audit_table', mock_update_audit_table)

    purge_data(config_file)


@pytest.mark.parametrize("config_file, partition_metadata", test_purge_data_data)
@mock.patch("google.cloud.bigquery.Client")
def test_purge_data_no_data(mock_bq_client, config_file, partition_metadata, monkeypatch):
    def mock_load_config(config_file_path):
        config = dict()
        config['project'] = "test"
        config['topic'] = "test"
        config['report_bucket'] = "test_bucket"
        config['report_location'] = "test/report"
        config['report_dataset'] = "test_dataset"
        config['report_table'] = "test_table"
        config['status_table'] = "status_table"
        config['datastore_config'] = "test"
        config['datastore_config'] = "test"
        config['slack_credentials_path'] = "gs://test/test.json"
        return config

    def mock_collect_partitions_metadata(bq_client, select_query):
        return {'gcs': [], 'bq': []}

    monkeypatch.setattr('purge.purge_data.load_config', mock_load_config)
    monkeypatch.setattr('purge.purge_data.collect_partitions_metadata', mock_collect_partitions_metadata)

    purge_data(config_file)
