from datetime import datetime
from unittest import mock

import pytest
from google.cloud.exceptions import NotFound

from common.utils import GcsUri
from trash.trash_data import GCSPartitionMetadata, BQPartitionMetadata, collect_partitions_metadata_query_for_trash, \
    create_missing_trash_buckets, trash_bigquery_data, trash_gcs_data, trash_data, \
    collect_existing_trash_table_ids, create_missing_trash_tables, collect_existing_trash_tables_query, \
    collect_source_metadata_query, get_partition_by, collect_create_trash_table_query, \
    collect_source_table_rowcount_query, collect_insert_query, collect_delete_query, get_missing_trash_table_ids

test_trash_bigquery_data_data = [(
    "111", datetime.strptime('2020-06-10', '%Y-%m-%d').date(), 'test', 'test.test.test', 'partition_year_month_day',
    'test.test_trash.test_trash')]

test_create_missing_trash_buckets_data = [([('test-bucket', 'test-bucket-trash', 'test-project')])]

test_trash_gcs_data_data = [(
    "111", datetime.strptime('2020-06-10', '%Y-%m-%d').date(), 'test', 'test-bucket', 'test-bucket-trash',
    'parquet/year=2020/month=6/day=6/')]

test_collect_partitions_to_be_trashed_query_data = [('test_project', 'test_dataset', 'test_table')]

test_trash_data_data = [('test.yaml', {"uid": '111', "partition_date": "2020-08-18", "project_id": '111',
                                       "dataset": "test_dataset", "table_name": "test_table_name",
                                       "partition_column": "test_partition_column",
                                       "partition_path": "gs://test/test/parquet/year=2020/month=7/day=10/"
                                       })]
test_get_partition_by_data = [
    ([]),
    ([["partition_year_month_day", "DATE"]]),
    ([["partition_year_month_day", "TIMESTAMP"]])
]

test_collect_existing_trash_tables_query_data = [('test_project', 'test_dataset_trash')]

test_collect_source_metadata_query_data = [('test_project', 'test_dataset', 'test_table')]

test_collect_delete_query_data = [('test_table', 'partition_year_month_day', '2020-08-20')]

test_collect_insert_query_data = [
    ('test_project.test_dataset_trash.test_table_trash', 'test_project.test_dataset.test_table',
     'partition_year_month_day', '2020-20-03')]

test_collect_source_table_rowcount_query_data = [
    ('partition_year_month_day', 'test_project.test_dataset.test_table', '2020-08-03')]

test_collect_create_trash_table_query_data = [
    (['test_project.test_dataset.test_table', 'test_project.test_dataset.test_table', '']),
    (['test_project.test_dataset.test_table', 'test_project.test_dataset.test_table', "partition by col_name"]),
    (['test_project.test_dataset.test_table', 'test_project.test_dataset.test_table', "partition by DATE (col_name)"])]

test_missing_trash_tables_data = [(
    ["111", datetime.strptime('2020-06-10', '%Y-%m-%d').date(), 'test', 'test.test.t1', 'partition_year_month_day',
     'test.test_trash.t1_trash'], ["222", datetime.strptime('2020-06-10', '%Y-%m-%d').date(), 'test',
                                   'test.test.t2', 'partition_year_month_day',
                                   'test.test_trash.t2_trash'])]

missing_trash_table_with_source_ids_data = [
    ([('test_project.test_dataset.test_table', 'test_project.test_dataset_trash.test_table_trash')], []),
    ([('test_project.test_dataset.test_table', 'test_project.test_dataset_trash.test_table_trash')],
     [["partition_year_month_day", "DATE"]]),
    ([('test_project.test_dataset.test_table', 'test_project.test_dataset_trash.test_table_trash')],
     [["partition_year_month_day", "TIMESTAMP"]]),
]

test_collect_existing_trash_table_ids_data = [(['test_project.test_datset_trash.test_table_trash'])]


@pytest.mark.parametrize("project_id, trash_table_schema", test_collect_existing_trash_tables_query_data)
def test_collect_existing_trash_tables_query(project_id, trash_table_schema):
    expected_query = f"SELECT concat(table_catalog,'.',table_schema,'.',table_name) AS trash_table_id " \
        f"FROM {project_id}.{trash_table_schema}.INFORMATION_SCHEMA.TABLES " \
        f"where table_type='BASE TABLE'"
    actual_query = collect_existing_trash_tables_query(project_id, trash_table_schema)
    assert actual_query == expected_query


@pytest.mark.parametrize("query_result", test_get_partition_by_data)
def test_get_partition_by(query_result):
    partition_by = get_partition_by(query_result)
    if query_result:
        if query_result[0][1] == 'DATE':
            assert partition_by == 'partition by partition_year_month_day'
        elif query_result[0][1] == 'TIMESTAMP':
            assert partition_by == 'partition by DATE (partition_year_month_day)'
    else:
        assert partition_by == ""


@pytest.mark.parametrize("partition_metadata", test_missing_trash_tables_data)
def test_missing_trash_tables(partition_metadata, monkeypatch):
    bq_partitions_to_be_trashed = [BQPartitionMetadata(*partition_metadata[0]),
                                   BQPartitionMetadata(*partition_metadata[1])]

    def mock_collect_existing_trash_table_ids(trash_table_ids):
        return ['test.test_trash.t1_trash']

    monkeypatch.setattr('trash.trash_data.collect_existing_trash_table_ids', mock_collect_existing_trash_table_ids)

    missing_trash_table_ids = get_missing_trash_table_ids(bq_partitions_to_be_trashed)
    assert missing_trash_table_ids == ['test.test_trash.t2_trash']


@pytest.mark.parametrize("project, source_table_schema, source_table_name", test_collect_source_metadata_query_data)
def test_collect_source_metadata_query(project, source_table_schema, source_table_name):
    expected_query = f"SELECT column_name,data_type FROM {source_table_schema}.INFORMATION_SCHEMA.COLUMNS " \
        f"where table_catalog='{project}' and table_schema='{source_table_schema}' " \
        f"and table_name='{source_table_name}' and is_partitioning_column='YES'"

    source_metadata_query = collect_source_metadata_query(project, source_table_schema, source_table_name)
    assert source_metadata_query == expected_query


@pytest.mark.parametrize("trash_table_ids", test_collect_existing_trash_table_ids_data)
@mock.patch("google.cloud.bigquery.table.Row")
@mock.patch("google.cloud.bigquery.Client")
def test_collect_existing_trash_table_ids(mock_client, mock_row, trash_table_ids, monkeypatch):
    mock_row().trash_table_id = trash_table_ids[0]

    def mock_execute_query(mock_client, schema_query):
        return [mock_row()]

    monkeypatch.setattr('trash.trash_data.execute_query', mock_execute_query)
    existing_trash_table_ids = collect_existing_trash_table_ids(trash_table_ids)
    assert existing_trash_table_ids == trash_table_ids


@pytest.mark.parametrize("source_table_id, trash_table_id, partition_by", test_collect_create_trash_table_query_data)
def test_collect_create_trash_table_query(source_table_id, trash_table_id, partition_by):
    query = f"create table {trash_table_id} {partition_by} as " \
        f"select * from {source_table_id} where 1=0"
    create_trash_table_query = collect_create_trash_table_query(source_table_id, trash_table_id, partition_by)
    assert create_trash_table_query == query


@pytest.mark.parametrize("missing_trash_table_with_source_ids, query_result", missing_trash_table_with_source_ids_data)
@mock.patch("google.cloud.bigquery.Client")
def test_create_missing_trash_tables(mock_client, missing_trash_table_with_source_ids, query_result):
    mock_client().query().result.side_effect = [query_result, mock_client().query()]

    create_missing_trash_tables(missing_trash_table_with_source_ids)
    assert mock_client().query().result.call_count == 2


@pytest.mark.parametrize("partition_column, source_table_id, partition_date",
                         test_collect_source_table_rowcount_query_data)
def test_collect_source_table_rowcount_query(partition_column, source_table_id, partition_date):
    expected_query = f"select count({partition_column}) as count from `{source_table_id}` where " \
        f"{partition_column} = TIMESTAMP '{partition_date}'"
    source_tbl_qry = collect_source_table_rowcount_query(partition_column, source_table_id, partition_date)
    assert source_tbl_qry == expected_query


@pytest.mark.parametrize("trash_table_id, source_table_id, partition_column, partition_date",
                         test_collect_insert_query_data)
def test_collect_insert_query(trash_table_id, source_table_id, partition_column, partition_date):
    expected_query = f"insert into `{trash_table_id}` " \
        f"select * from `{source_table_id}` where " \
        f"{partition_column}=TIMESTAMP '{partition_date}'"

    insert_query = collect_insert_query(trash_table_id, source_table_id, partition_column, partition_date)
    assert insert_query == expected_query


@pytest.mark.parametrize("source_table_id, partition_column, partition_date", test_collect_delete_query_data)
def test_collect_delete_query(source_table_id, partition_column, partition_date):
    expected_query = f"delete from `{source_table_id}` where " \
        f"{partition_column}= TIMESTAMP '{partition_date}'"
    delete_qry = collect_delete_query(source_table_id, partition_column, partition_date)
    assert delete_qry == expected_query


@pytest.mark.parametrize(
    "uid, partition_date, project_id, source_table_id, partition_column, trash_table_id", test_trash_bigquery_data_data)
@mock.patch("google.cloud.bigquery.table.Row")
@mock.patch("google.cloud.bigquery.Client")
def test_trash_bigquery_data(mock_client, mock_row, uid, partition_date, project_id, source_table_id,
                             partition_column, trash_table_id):
    bq_partitions_to_be_trashed = BQPartitionMetadata(uid, partition_date, project_id, source_table_id,
                                                      partition_column, trash_table_id)
    mock_client().query().result.side_effect = [[mock_row], None, None]
    trash_bigquery_data(bq_partitions_to_be_trashed)
    assert mock_client().query().result.call_count == 3


@pytest.mark.parametrize(
    "uid, partition_date, project_id, source_table_id, partition_column, trash_table_id", test_trash_bigquery_data_data)
@mock.patch("google.cloud.bigquery.Client")
def test_trash_bigquery_data_no_records(mock_client, uid, partition_date, project_id,
                                        source_table_id, partition_column, trash_table_id):
    bq_partitions_to_be_trashed = BQPartitionMetadata(uid, partition_date, project_id, source_table_id,
                                                      partition_column, trash_table_id)
    mock_client().query().result.return_value = [[0]]
    trash_bigquery_data(bq_partitions_to_be_trashed)
    assert mock_client().query().result.call_count == 1


@pytest.mark.parametrize(
    "uid, partition_date, project_id, source_table_id, partition_column, trash_table_id", test_trash_bigquery_data_data)
@mock.patch("google.cloud.bigquery.Client")
def test_trash_bigquery_data_exception(mock_client, uid, partition_date, project_id,
                                       source_table_id, partition_column, trash_table_id):
    bq_partitions_to_be_trashed = BQPartitionMetadata(uid, partition_date, project_id, source_table_id,
                                                      partition_column, trash_table_id)
    mock_client().query().result.side_effect = [Exception('Unable to query')]
    result = trash_bigquery_data(bq_partitions_to_be_trashed)
    assert mock_client().query().result.call_count == 1
    assert result['uid'] == uid
    assert result['error_bq'] == 'Unable to query'


@pytest.mark.parametrize("source_trash_buckets_with_project", test_create_missing_trash_buckets_data)
@mock.patch("google.cloud.storage.Client")
def test_create_missing_trash_buckets_exists(mock_client, source_trash_buckets_with_project):
    create_missing_trash_buckets(source_trash_buckets_with_project)
    mock_client().bucket().create.assert_not_called()


@pytest.mark.parametrize("source_trash_buckets_with_project", test_create_missing_trash_buckets_data)
@mock.patch("google.cloud.storage.Client")
def test_create_missing_trash_buckets(mock_client, source_trash_buckets_with_project):
    mock_client().lookup_bucket.return_value = None
    create_missing_trash_buckets(source_trash_buckets_with_project)
    mock_client().bucket().create.acsert_called()


@pytest.mark.parametrize(
    "uid, partition_date, project_id, source_bucket_name, trash_bucket_name, partition_path", test_trash_gcs_data_data)
@mock.patch("google.cloud.storage.blob.Blob")
@mock.patch("google.cloud.storage.blob.Blob")
@mock.patch("google.cloud.storage.Client")
def test_trash_gcs_data(mock_client, mock_blob_file1, mock_blob_file2, uid, partition_date, project_id,
                        source_bucket_name, trash_bucket_name, partition_path):
    gcs_partitions_to_be_trashed = GCSPartitionMetadata(uid, partition_date, project_id, source_bucket_name,
                                                        trash_bucket_name, partition_path)
    mock_blob_file1.name = f"{partition_path}/test.parquet"
    src_bucket = mock_client().get_bucket(source_bucket_name)
    trash_bucket = mock_client().bucket(trash_bucket_name)
    src_bucket.delete_blob.return_value = [None, NotFound("File Not Found")]
    mock_client().get_bucket().list_blobs.return_value = [mock_blob_file1, mock_blob_file2]
    trash_gcs_data(gcs_partitions_to_be_trashed)
    src_bucket.copy_blob.assert_called_with(mock_blob_file2, trash_bucket)
    src_bucket.delete_blob.assert_called_with(mock_blob_file2.name)
    assert src_bucket.delete_blob.call_count == 2
    assert src_bucket.copy_blob.call_count == 2


@pytest.mark.parametrize(
    "uid, partition_date, project_id, source_bucket_name, trash_bucket_name, partition_path", test_trash_gcs_data_data)
@mock.patch("google.cloud.storage.Client")
def test_trash_gcs_data_no_files(mock_client, uid, partition_date, project_id, source_bucket_name, trash_bucket_name,
                                 partition_path):
    gcs_partitions_to_be_trashed = GCSPartitionMetadata(uid, partition_date, project_id, source_bucket_name,
                                                        trash_bucket_name, partition_path)
    src_bucket = mock_client().get_bucket('src_bucket')
    mock_client().get_bucket().list_blobs.return_value = []
    trash_gcs_data(gcs_partitions_to_be_trashed)
    src_bucket.copy_blob.assert_not_called()
    src_bucket.delete_blob.assert_not_called()


@pytest.mark.parametrize(
    "uid, partition_date, project_id, source_bucket_name, trash_bucket_name, partition_path", test_trash_gcs_data_data)
@mock.patch("google.cloud.storage.Client")
def test_trash_gcs_data_exception(mock_client, uid, partition_date, project_id, source_bucket_name, trash_bucket_name,
                                  partition_path):
    gcs_partitions_to_be_trashed = GCSPartitionMetadata(uid, partition_date, project_id, source_bucket_name,
                                                        trash_bucket_name, partition_path)
    src_bucket = mock_client().get_bucket('src_bucket')
    mock_client().get_bucket.side_effect = [Exception('Unable to get src bucket')]
    result = trash_gcs_data(gcs_partitions_to_be_trashed)
    src_bucket.copy_blob.assert_not_called()
    src_bucket.delete_blob.assert_not_called()
    assert result['uid'] == uid
    assert result['error_gcs'] == 'Unable to get src bucket'


@pytest.mark.parametrize("project, report_dataset, report_table", test_collect_partitions_to_be_trashed_query_data)
def test_collect_partitions_to_be_trashed_query(project, report_dataset, report_table):
    expected_query = f"SELECT uid, format_datetime('%Y-%m-%d',datetime(partition_date)) as partition_date," \
        f"project_id, dataset, table_name, partition_column, partition_path " \
        f"FROM `{project}.{report_dataset}.{report_table}` " \
        f"WHERE expected_trash_date is not null and (trashed_date_gcs is null and trashed_date_bq is null) " \
        f"and format_datetime(\'%Y-%m-%d\',datetime(expected_trash_date)) <= '{datetime.utcnow().date()}' "
    actual_query = collect_partitions_metadata_query_for_trash(project, report_dataset, report_table)
    assert actual_query == expected_query


@pytest.mark.parametrize("config_file, partition_metadata", test_trash_data_data)
@mock.patch("trash.trash_data.multiprocess_function")
@mock.patch("google.cloud.storage.Client")
@mock.patch("google.cloud.bigquery.Client")
def test_trash_data(mock_bq_client, mock_gcs_client, multiprocess_function, config_file, partition_metadata,
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

    def mock_trash_bigquery_data(bq_partition_to_be_trashed):
        return

    def mock_update_audit_table(project, report_dataset, report_table, gcs_update_data, bq_update_data):
        return

    def mock_get_missing_trash_table_ids(bq_partitions_to_be_trashed):
        trash_table_id = f"{partition_metadata['project_id']}.{partition_metadata['dataset']}_" \
            f"trash.{partition_metadata['table_name']}_trash"
        return [trash_table_id]

    def mock_create_missing_trash_tables(missing_trash_table_with_source_ids):
        return

    monkeypatch.setattr('trash.trash_data.create_missing_trash_tables', mock_create_missing_trash_tables)
    monkeypatch.setattr('trash.trash_data.get_missing_trash_table_ids', mock_get_missing_trash_table_ids)
    monkeypatch.setattr('trash.trash_data.load_config', mock_load_config)
    monkeypatch.setattr('trash.trash_data.collect_partitions_metadata', mock_collect_partitions_metadata)
    monkeypatch.setattr('trash.trash_data.trash_bigquery_data', mock_trash_bigquery_data)
    monkeypatch.setattr('trash.trash_data.update_audit_table', mock_update_audit_table)

    trash_data(config_file)


@pytest.mark.parametrize("config_file, partition_metadata", test_trash_data_data)
@mock.patch("trash.trash_data.multiprocess_function")
@mock.patch("google.cloud.storage.Client")
@mock.patch("google.cloud.bigquery.Client")
def test_trash_data_with_errors(mock_bq_client, mock_gcs_client, multiprocess_function, config_file, partition_metadata,
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

    def mock_trash_bigquery_data(bq_partition_to_be_trashed):
        uid = partition_metadata['uid']
        return {'uid': uid, 'error_bq': 'Unable to trash table'}

    def mock_update_audit_table(project, report_dataset, report_table, gcs_update_data, bq_update_data):
        return

    def mock_get_missing_trash_table_ids(bq_partitions_to_be_trashed):
        return []

    multiprocess_function.return_value = [
        {'uid': partition_metadata['uid'], 'error_bq': 'Unable to trash partition path'}]
    monkeypatch.setattr('trash.trash_data.get_missing_trash_table_ids', mock_get_missing_trash_table_ids)
    monkeypatch.setattr('trash.trash_data.load_config', mock_load_config)
    monkeypatch.setattr('trash.trash_data.collect_partitions_metadata', mock_collect_partitions_metadata)
    monkeypatch.setattr('trash.trash_data.trash_bigquery_data', mock_trash_bigquery_data)
    monkeypatch.setattr('trash.trash_data.update_audit_table', mock_update_audit_table)

    trash_data(config_file)


@pytest.mark.parametrize("config_file, partition_metadata", test_trash_data_data)
@mock.patch("google.cloud.bigquery.Client")
def test_trash_data_no_data(mock_bq_client, config_file, partition_metadata, monkeypatch):
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

    monkeypatch.setattr('trash.trash_data.load_config', mock_load_config)
    monkeypatch.setattr('trash.trash_data.collect_partitions_metadata', mock_collect_partitions_metadata)

    trash_data(config_file)
