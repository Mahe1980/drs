import os
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from generate_report.report import get_drs_policies, trash_and_anomaly_records, generate_report, write_to_bq, \
    send_pubsub_message, write_to_gcs, get_job_config, get_bigquery_audit_records, ReportRecord, \
    generate_report_payload, get_gcs_audit_records, merge_gcs_bigquery_audit_records, get_last_successful_run_date, \
    update_status_table_with_timestamp, get_job_config_for_status_table, get_payload_for_status_table, \
    get_select_partition_date_count_query, get_current_last_retention_date, get_last_successful_run_date_select_query, \
    BQ_COLUMNS, GCS_COLUMNS

ROOT_DIR = Path(__file__).resolve().parent.parent.parent

test_trash_and_anomaly_records_data = [([('2020-07-13', 200)], 10, [('2020-07-13', 200, False)], False),
                                       ([('2020-07-13', 200), ('2020-07-14', 200)], 10,
                                        [('2020-07-14', 200, False)], True),
                                       ([('2020-07-13', 200), ('2020-07-14', 2000), ('2020-07-15', 200)], 10,
                                        [('2020-07-14', 2000, True, '2020-07-13', 200, 900.0),
                                         ('2020-07-15', 200, True, '2020-07-14', 2000, 90.0)], True)
                                       ]

test_get_gcs_audit_records_data = [
    ({"project_id": 'test', "config_id": "123-456-789", "days_retain": 10, "dataset": 'test', "table_name": 'test',
      "gcs_path": 'gs://test/test/', "partition_column": 'test', 'anomaly_detection': False},
     [(datetime.strptime('2020-06-06', '%Y-%m-%d').date(), 100000000)], None),
    ({"project_id": 'test', "config_id": "234-567-891", "days_retain": 10, "dataset": 'test', "table_name": 'test',
      "gcs_path": 'gs://test/test/', "partition_column": 'test', 'anomaly_detection': True,
      'anomaly_detection_variation': 10},
     [(datetime.strptime('2020-06-06', '%Y-%m-%d').date(), 100000000, False)],
     datetime.strptime('2020-06-05', '%Y-%m-%d').date())]

test_get_audit_record_exception_data = [
    ({"project_id": 'test', "config_id": "123-456-789", "days_retain": 10, "dataset": 'test', "table_name": 'test',
      "gcs_path": 'gs://test/test/', "partition_column": 'test'}, ('2020-07-03', 200))]

test_get_bigquery_audit_records_data = [
    ({'table_name': 'test', 'dataset': 'test', 'config_id': '111-222-333', 'gcs_path': 'gs://test/test',
      'project_id': 'test', 'days_retain': 10, 'anomaly_detection': False, 'anomaly_detection_variation': 10,
      'partition_column': 'test'},
     [('2020-07-13', 200), ('2020-07-12', 200)], [('2020-07-12', 200), ('2020-07-13', 200)], None),
    ({'table_name': 'test', 'dataset': 'test', 'config_id': '111-222-333', 'gcs_path': 'gs://test/test',
      'project_id': 'test', 'days_retain': 10, 'anomaly_detection': True, 'anomaly_detection_variation': 10,
      'partition_column': 'test'},
     [('2020-07-12', 200), ('2020-07-13', 200)], [('2020-07-13', 200, False)],
     datetime.strptime('2020-07-13', '%Y-%m-%d').date()),
    ({'table_name': 'test', 'dataset': 'test', 'config_id': '111-222-333', 'gcs_path': 'gs://test/test',
      'project_id': 'test', 'days_retain': 10, 'anomaly_detection': True, 'anomaly_detection_variation': 10,
      'partition_column': 'test'},
     [('2020-07-12', 2000), ('2020-07-13', 200)],
     [('2020-07-12', 2000, False), ('2020-07-13', 200, True, '2020-07-12', 2000, 90.0)], None)
]

test_get_gcs_bigquery_audit_records_no_partition_records_data = [
    ({'table_name': 'test', 'dataset': 'test', 'config_id': '111-222-333', 'gcs_path': 'gs://test/test',
      'project_id': 'test', 'days_retain': 10, 'anomaly_detection': False, 'anomaly_detection_variation': 10,
      'partition_column': 'test'}, [], datetime.strptime('2020-06-05', '%Y-%m-%d').date())]

test_get_gcs_bigquery_audit_records_no_report_records_data = [
    ({'table_name': 'test', 'dataset': 'test', 'config_id': '111-222-333', 'gcs_path': 'gs://test/test',
      'project_id': 'test', 'days_retain': 10, 'anomaly_detection': True, 'anomaly_detection_variation': 10,
      'partition_column': 'test'},
     [('2020-07-12', 2000)], [], None)]

test_get_bigquery_gcs_audit_records_exception_data = [
    ({'table_name': 'test', 'dataset': 'test', 'config_id': '111-222-333', 'gcs_path': 'gs://test/test',
      'project_id': 'test', 'days_retain': 10, 'anomaly_detection': 'false', 'anomaly_detection_variation': 10,
      'partition_column': 'test'}, None)]

test_generate_report_data = [
    ("gs://test/test/config.yaml",
     [{'table_name': 'test', 'dataset': 'test', 'config_id': '111-222-333', 'partition_column': 'test',
       'gcs_path': 'gs://test/test', 'project_id': 'test', 'days_retain': 10,
       'anomaly_detection': 'false'}])]

test_get_drs_policies_data = [
    ({'namespace': 'test', 'kind': 'test'})]

test_generate_report_payload_data = [(datetime.utcnow())]

test_write_to_gcs_data = [("test_bucket", "test/report", datetime.utcnow())]

test_write_to_bq_data = [("test_project", "test_dataset", "test_table", "gs://test/test/test.csv")]

send_pubsub_message_data = [("test", "test", {"eventType": "OBJECT_FINALIZE",
                                              "eventTime": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                                              "fileBucket": "test",
                                              "fileKey": "test/test.csv"
                                              })]

test_get_job_config_date = [(("uid", "STRING"), ("report_date", "TIMESTAMP"), ("config_id", "STRING"),
                             ("project_id", "STRING"), ("days_to_retain", "INT64"), ("partition_date", "DATE"),
                             ("partition_path", "STRING"), ("total_partition_files_size", "STRING"),
                             ("total_partition_files", "INT64"), ("anomaly_detected_gcs", "BOOLEAN"),
                             ("previous_partition_date_gcs", "DATE"),
                             ("total_previous_partition_files_size", "STRING"),
                             ("variation_with_previous_partition_files_size", "FLOAT64"), ("dataset", "STRING"),
                             ("table_name", "STRING"), ("partition_column", "STRING"), ("partition_row_count", "INT64"),
                             ("anomaly_detected_bq", "BOOLEAN"), ("previous_partition_date_bq", "DATE"),
                             ("previous_partition_row_count", "INT64"),
                             ("variation_with_previous_partition_row_count", "FLOAT64"),
                             ("expected_trash_date", "DATE"), ("expected_purge_date", "DATE"),
                             ("trashed_date_gcs", "TIMESTAMP"), ("purged_date_gcs", "TIMESTAMP"),
                             ("trashed_date_bq", "TIMESTAMP"), ("purged_date_bq", "TIMESTAMP"), ("error_gcs", "STRING"),
                             ("error_bq", "STRING"))]

test_report_record_data = [('2020-07-03', 200), ('2020-07-03', 200, False),
                           ('2020-07-03', 200, True, '2020-07-02', 4200, 90)]

test_get_last_successful_run_date_data = [('test_project', 'test_dataset', 'test_source_table')]

test_get_select_partition_date_count_query_data = [
    (f'test_project.test_dataset.test_table', 'partition_year_month_day', datetime.now().date(), None,
     f"select format_datetime('%Y-%m-%d', datetime(partition_year_month_day)) as partition_date, count(*) as "
     f"row_count from test_project.test_dataset.test_table where "
     f"partition_year_month_day < TIMESTAMP '{datetime.now().date()}' "
     f"group by format_datetime('%Y-%m-%d', datetime(partition_year_month_day))"),
    ('test_project.test_dataset.test_table', 'partition_year_month_day', datetime.now().date(),
     (datetime.now() - timedelta(1)).date(),
     f"select format_datetime('%Y-%m-%d', datetime(partition_year_month_day)) as partition_date, count(*) as "
     f"row_count from test_project.test_dataset.test_table where "
     f"partition_year_month_day >= TIMESTAMP '{(datetime.now() - timedelta(1)).date()}' "
     f"AND partition_year_month_day < TIMESTAMP '{datetime.now().date()}' group by format_datetime('%Y-%m-%d', "
     f"datetime(partition_year_month_day))")]

test_get_current_last_retention_date_data = [(10, False, None, (datetime.now() - timedelta(3)).date(), None),
                                             (10, True, None, (datetime.now() - timedelta(3)).date(), None),
                                             (10, True, (datetime.now() - timedelta(1)).date(),
                                              (datetime.now() - timedelta(3)).date(),
                                              (datetime.now() - timedelta(5)).date()),
                                             (10, False, (datetime.now() - timedelta(1)).date(),
                                              (datetime.now() - timedelta(3)).date(),
                                              (datetime.now() - timedelta(4)).date())]

test_get_last_successful_run_date_select_query_data = [
    (
        'test_project', 'test_dataset', 'test_status_table',
        'select * from `test_project.test_dataset.test_status_table`')]


@pytest.mark.parametrize("report_record_data", test_report_record_data)
def test_report_record(report_record_data):
    report_record = ReportRecord(*report_record_data)
    assert report_record.partition_date == report_record_data[0]
    assert report_record.partition_size == report_record_data[1]

    if len(report_record_data) == 3:
        assert report_record.anomaly == report_record_data[2]
    elif len(report_record_data) > 3:
        assert report_record.anomaly == report_record_data[2]
        assert report_record.previous_partition_date == report_record_data[3]
        assert report_record.previous_partition_size == report_record_data[4]
        assert report_record.variation == report_record_data[5]


@pytest.mark.parametrize("datastore_config", test_get_drs_policies_data)
@mock.patch('google.cloud.datastore.entity.Entity')
@mock.patch("google.cloud.datastore.Client")
def test_get_drs_policies(mock_ds_client, mock_ds_entity, datastore_config):
    mock_ds_client().query().fetch.return_value = [mock_ds_entity, mock_ds_entity]
    result = get_drs_policies(datastore_config)
    assert len(result) == 2
    assert len(result[0]) == 9
    assert result[0]['config_id'] is not None
    assert result[0]['days_retain'] is not None
    assert result[0]['anomaly_detection'] is not None
    assert result[0]['anomaly_detection_variation'] is not None
    assert result[0]['project_id'] is not None
    assert result[0]['table_name'] is not None
    assert result[0]['dataset'] is not None
    assert result[0]['gcs_path'] is not None
    assert result[0]['partition_column'] is not None


@pytest.mark.parametrize("datastore_config", test_get_drs_policies_data)
@mock.patch("google.cloud.datastore.Client")
def test_get_drs_policies_legal_processing(mock_ds_client, datastore_config):
    entity = dict()
    entity['retention_policy'] = {}
    entity['retention_policy']['legalProcessing'] = 'true'
    mock_ds_client().query().fetch.return_value = [entity]
    result = get_drs_policies(datastore_config)
    assert len(result) == 0


@pytest.mark.parametrize("partition_data, variation, expected_report_records, delta_load",
                         test_trash_and_anomaly_records_data)
def test_trash_and_anomaly_records(partition_data, variation, expected_report_records, delta_load):
    actual_records = trash_and_anomaly_records(partition_data, variation, delta_load)
    expected_records = list()
    for expected_report_record in expected_report_records:
        expected_records.append(ReportRecord(*expected_report_record))

    for actual_record, expected_record in zip(actual_records, expected_records):
        assert actual_record.partition_date == expected_record.partition_date
        assert actual_record.partition_size == expected_record.partition_size
        assert actual_record.anomaly == expected_record.anomaly
        assert actual_record.previous_partition_date == expected_record.previous_partition_date
        assert actual_record.previous_partition_size == expected_record.previous_partition_size
        assert actual_record.variation == expected_record.variation


@pytest.mark.parametrize("table_id, partition_column, retention_date, last_retention_date, expected_query",
                         test_get_select_partition_date_count_query_data)
def test_get_select_partition_date_count_query(table_id, partition_column, retention_date, last_retention_date,
                                               expected_query):
    actual_query = get_select_partition_date_count_query(table_id, partition_column, retention_date,
                                                         last_retention_date)
    assert actual_query == expected_query


@pytest.mark.parametrize(
    "days_retain, anomaly_detection, last_successful_run_date, expected_retention_date, expected_last_retention_date",
    test_get_current_last_retention_date_data)
def test_get_current_last_retention_date(days_retain, anomaly_detection, last_successful_run_date,
                                         expected_retention_date, expected_last_retention_date):
    actual_retention_date, actual_last_retention_date = get_current_last_retention_date(days_retain, anomaly_detection,
                                                                                        last_successful_run_date)
    assert actual_retention_date == expected_retention_date
    assert actual_last_retention_date == expected_last_retention_date


@pytest.mark.parametrize("drs_policy, records_to_trash, expected_records_to_trash, last_successful_run_date",
                         test_get_bigquery_audit_records_data)
@mock.patch("google.cloud.bigquery.Client")
def test_get_bigquery_audit_records(mock_bq_client, drs_policy, records_to_trash, expected_records_to_trash,
                                    last_successful_run_date):
    mock_bq_client().query().result.return_value = records_to_trash
    expected_records = [ReportRecord(*expected_report_record) for expected_report_record in expected_records_to_trash]
    expected_bigquery_records = list()
    for report_record in expected_records:
        rec = {'partition_date': datetime.strptime(report_record.partition_date, '%Y-%m-%d').date(),
               'partition_row_count': report_record.partition_size,
               "anomaly_detected_bq": report_record.anomaly,
               "previous_partition_date_bq":
                   datetime.strptime(report_record.previous_partition_date, '%Y-%m-%d').date()
                   if report_record.previous_partition_date else None,
               "previous_partition_row_count": report_record.previous_partition_size,
               "variation_with_previous_partition_row_count": report_record.variation
               }
        expected_bigquery_records.append(rec)
    expected_bigquery_records_df = pd.DataFrame(data=expected_bigquery_records,
                                                columns=expected_bigquery_records[0].keys())
    expected_bigquery_records_df.insert(1, 'partition_column', drs_policy['partition_column'])
    expected_bigquery_records_df.insert(1, 'table_name', drs_policy['table_name'])
    expected_bigquery_records_df.insert(1, 'dataset', drs_policy['dataset'])
    expected_bigquery_records_df.insert(0, 'days_to_retain', drs_policy['days_retain'])
    expected_bigquery_records_df.insert(0, 'project_id', drs_policy['project_id'])
    expected_bigquery_records_df.insert(0, 'config_id', drs_policy['config_id'])

    actual_bigquery_records_df = get_bigquery_audit_records(last_successful_run_date, drs_policy)
    assert_frame_equal(actual_bigquery_records_df, expected_bigquery_records_df)


@pytest.mark.parametrize("drs_policy, records_to_trash, last_successful_run_date",
                         test_get_gcs_bigquery_audit_records_no_partition_records_data)
@mock.patch("google.cloud.bigquery.Client")
def test_get_bigquery_audit_records_no_partition_records(mock_bq_client, drs_policy, records_to_trash,
                                                         last_successful_run_date):
    mock_bq_client().query().result.return_value = records_to_trash
    expected_bigquery_records_df = pd.DataFrame(columns=BQ_COLUMNS)
    actual_bigquery_records_df = get_bigquery_audit_records(last_successful_run_date, drs_policy)
    assert_frame_equal(actual_bigquery_records_df, expected_bigquery_records_df)


@pytest.mark.parametrize("drs_policy, records_to_trash, expected_records_to_trash, last_successful_run_date",
                         test_get_gcs_bigquery_audit_records_no_report_records_data)
@mock.patch("google.cloud.bigquery.Client")
def test_get_bigquery_audit_records_no_report_records(mock_bq_client, drs_policy, records_to_trash,
                                                      expected_records_to_trash, last_successful_run_date, monkeypatch):
    mock_bq_client().query().result.return_value = records_to_trash

    def mock_trash_and_anomaly_records(partition_data, anomaly_variation_threshold, last_retention_date):
        return expected_records_to_trash

    monkeypatch.setattr('generate_report.report.trash_and_anomaly_records', mock_trash_and_anomaly_records)

    expected_gcs_records_df = pd.DataFrame(columns=BQ_COLUMNS)
    actual_gcs_records_df = get_bigquery_audit_records(last_successful_run_date, drs_policy)
    assert_frame_equal(actual_gcs_records_df, expected_gcs_records_df)


@pytest.mark.parametrize("drs_policy, last_successful_run_date", test_get_bigquery_gcs_audit_records_exception_data)
@mock.patch("google.cloud.bigquery.Client")
def test_get_bigquery_audit_records_exception(mock_bq_client, drs_policy, last_successful_run_date):
    mock_bq_client().query.side_effect = Exception(f'Unable to query from table `test`')
    actual_result = get_bigquery_audit_records(last_successful_run_date, drs_policy)
    assert actual_result is None


@pytest.mark.parametrize("drs_policy, expected_records_to_trash, last_successful_run_date",
                         test_get_gcs_audit_records_data)
@mock.patch("google.cloud.storage.blob.Blob")
@mock.patch("google.cloud.storage.blob.Blob")
@mock.patch("google.cloud.storage.blob.Blob")
@mock.patch("google.cloud.storage.Client")
def test_get_gcs_audit_records(mock_storage_client, mock_blob_file1, mock_blob_file2, mock_blob_folder, drs_policy,
                               expected_records_to_trash, last_successful_run_date):
    bucket = mock_storage_client().get_bucket()
    mock_blob_file1.name = f'test/year=2020/month=6/day=6/test.parquet'
    mock_blob_file1.size = 100000000
    mock_blob_file2.name = f'test/year=2020/month=06/day=06/test.parquet'
    mock_blob_file2.size = 100000000
    mock_blob_folder.name = f'test/year=2019/month=6/day=6/'

    bucket.list_blobs.return_value = [mock_blob_file1, mock_blob_file2, mock_blob_folder]
    expected_report_records = [ReportRecord(*expected_report_record) for expected_report_record in
                               expected_records_to_trash]
    expected_gcs_audit_records_df = pd.DataFrame(
        [(report_record.partition_date, report_record.anomaly, report_record.partition_size,
          report_record.previous_partition_date, report_record.previous_partition_size,
          report_record.variation) for report_record in expected_report_records],
        columns=['partition_date', 'anomaly_detected_gcs', 'total_partition_files_size',
                 'previous_partition_date_gcs', 'total_previous_partition_files_size',
                 'variation_with_previous_partition_files_size'])
    expected_gcs_audit_records_df.insert(0, 'total_partition_files', 1)
    expected_gcs_audit_records_df.insert(0, 'partition_path', "gs://test/test/year=2020/month=6/day=6/")
    expected_gcs_audit_records_df.insert(0, 'days_to_retain', drs_policy['days_retain'])
    expected_gcs_audit_records_df.insert(0, 'project_id', drs_policy['project_id'])
    expected_gcs_audit_records_df.insert(0, 'config_id', drs_policy['config_id'])

    expected_gcs_audit_records_df = expected_gcs_audit_records_df.reindex(
        columns=["config_id", "project_id", "days_to_retain", "partition_date", "partition_path",
                 "total_partition_files_size", "total_partition_files", "anomaly_detected_gcs",
                 "previous_partition_date_gcs", "total_previous_partition_files_size",
                 "variation_with_previous_partition_files_size"
                 ])
    actual_gcs_audit_records_df = get_gcs_audit_records(last_successful_run_date, drs_policy)
    assert_frame_equal(actual_gcs_audit_records_df, expected_gcs_audit_records_df)


@pytest.mark.parametrize("drs_policy, records_to_trash, last_successful_run_date",
                         test_get_gcs_bigquery_audit_records_no_partition_records_data)
@mock.patch("google.cloud.storage.Client")
def test_get_gcs_audit_records_no_partition_records(mock_storage_client, drs_policy, records_to_trash,
                                                    last_successful_run_date):
    bucket = mock_storage_client().get_bucket()
    bucket.list_blobs.return_value = records_to_trash
    expected_gcs_records_df = pd.DataFrame(columns=GCS_COLUMNS)
    actual_gcs_records_df = get_gcs_audit_records(last_successful_run_date, drs_policy)
    assert_frame_equal(actual_gcs_records_df, expected_gcs_records_df)


@pytest.mark.parametrize("drs_policy, records_to_trash, expected_records_to_trash, last_successful_run_date",
                         test_get_gcs_bigquery_audit_records_no_report_records_data)
@mock.patch("google.cloud.storage.blob.Blob")
@mock.patch("google.cloud.storage.Client")
def test_get_gcs_audit_records_no_report_records(mock_storage_client, mock_blob_file, drs_policy, records_to_trash,
                                                 expected_records_to_trash, last_successful_run_date, monkeypatch):
    bucket = mock_storage_client().get_bucket()
    mock_blob_file.name = f'test/year=2020/month=6/day=6/test.parquet'
    mock_blob_file.size = 100000000

    bucket.list_blobs.return_value = [mock_blob_file]

    def mock_trash_and_anomaly_records(partition_data, anomaly_variation_threshold, last_retention_date):
        return expected_records_to_trash

    monkeypatch.setattr('generate_report.report.trash_and_anomaly_records', mock_trash_and_anomaly_records)

    expected_gcs_records_df = pd.DataFrame(columns=GCS_COLUMNS)
    actual_gcs_records_df = get_gcs_audit_records(last_successful_run_date, drs_policy)
    assert_frame_equal(actual_gcs_records_df, expected_gcs_records_df)


@pytest.mark.parametrize("drs_policy, last_successful_run_date", test_get_bigquery_gcs_audit_records_exception_data)
@mock.patch("google.cloud.storage.Client")
def test_get_gcs_audit_records_exception(mock_storage_client, drs_policy, last_successful_run_date):
    mock_storage_client().get_bucket.side_effect = Exception(f'Unable to get bucket `test`')
    actual_result = get_gcs_audit_records(last_successful_run_date, drs_policy)
    assert actual_result is None


def test_merge_gcs_bigquery_audit_records():
    gcs_audit_records_config_1_df = pd.read_csv(
        os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'gcs_audit_records_config_1.csv'))
    gcs_audit_records_config_2_df = pd.read_csv(
        os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'gcs_audit_records_config_2.csv'))
    gcs_audit_records_dfs = [gcs_audit_records_config_1_df, gcs_audit_records_config_2_df]

    bigquery_audit_records_config_1_df = pd.read_csv(
        os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'bigquery_audit_records_config_1.csv'))
    bigquery_audit_records_config_2_df = pd.read_csv(
        os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'bigquery_audit_records_config_2.csv'))
    bigquery_audit_records_dfs = [bigquery_audit_records_config_1_df, bigquery_audit_records_config_2_df]

    expected_audit_records_df = pd.read_csv(
        os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'audit_records.csv'))

    actual_audit_records_df = merge_gcs_bigquery_audit_records(gcs_audit_records_dfs, bigquery_audit_records_dfs)

    actual_audit_records_df = actual_audit_records_df.drop(columns=['report_date'])
    assert_frame_equal(expected_audit_records_df, actual_audit_records_df)


def test_merge_gcs_bigquery_audit_records_no_gcs():
    gcs_audit_records_dfs = []

    bigquery_audit_records_config_1_df = pd.read_csv(
        os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'bigquery_audit_records_config_1.csv'))
    bigquery_audit_records_config_2_df = pd.read_csv(
        os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'bigquery_audit_records_config_2.csv'))
    bigquery_audit_records_dfs = [bigquery_audit_records_config_1_df, bigquery_audit_records_config_2_df]

    expected_audit_records_df = pd.read_csv(
        os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'audit_records_no_gcs.csv'))
    expected_audit_records_df['partition_date'] = pd.to_datetime(expected_audit_records_df['partition_date']).apply(
        lambda dt: dt.date())

    actual_audit_records_df = merge_gcs_bigquery_audit_records(gcs_audit_records_dfs, bigquery_audit_records_dfs)

    actual_audit_records_df = actual_audit_records_df.drop(columns=['report_date'])
    expected_audit_records_df[GCS_COLUMNS] = expected_audit_records_df[GCS_COLUMNS].astype('object')
    expected_audit_records_df[['days_to_retain']] = expected_audit_records_df[['days_to_retain']].astype('int64')
    assert_frame_equal(actual_audit_records_df, actual_audit_records_df, check_like=True)


def test_merge_gcs_bigquery_audit_records_no_gcs_no_bq():
    gcs_audit_records_dfs = []
    bigquery_audit_records_dfs = []
    expected_audit_records_df = pd.DataFrame()
    actual_audit_records_df = merge_gcs_bigquery_audit_records(gcs_audit_records_dfs, bigquery_audit_records_dfs)
    assert_frame_equal(actual_audit_records_df, expected_audit_records_df)


def test_merge_gcs_bigquery_audit_records_no_bq():
    gcs_audit_records_config_1_df = pd.read_csv(
        os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'gcs_audit_records_config_1.csv'))
    gcs_audit_records_config_2_df = pd.read_csv(
        os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'gcs_audit_records_config_2.csv'))
    gcs_audit_records_dfs = [gcs_audit_records_config_1_df, gcs_audit_records_config_2_df]

    bigquery_audit_records_dfs = []

    expected_audit_records_df = pd.read_csv(
        os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'audit_records_no_bq.csv'))

    actual_audit_records_df = merge_gcs_bigquery_audit_records(gcs_audit_records_dfs, bigquery_audit_records_dfs)

    actual_audit_records_df = actual_audit_records_df.drop(columns=['report_date'])
    expected_audit_records_df[BQ_COLUMNS] = expected_audit_records_df[BQ_COLUMNS].astype('object')
    expected_audit_records_df[['days_to_retain']] = expected_audit_records_df[['days_to_retain']].astype('int64')
    expected_audit_records_df[['total_partition_files']] = expected_audit_records_df[['total_partition_files']].astype(
        'int64')
    assert_frame_equal(actual_audit_records_df, expected_audit_records_df, check_like=True)


@pytest.mark.parametrize("config_file, drs_data", test_generate_report_data)
@mock.patch("generate_report.report.multiprocess_function")
@mock.patch("google.cloud.pubsub_v1.PublisherClient")
@mock.patch("google.cloud.bigquery.Client")
@mock.patch("google.cloud.storage.Client")
def test_generate_report(mock_storage_client, mock_bq_client, mock_pubsub_client, multiprocess_function, config_file,
                         drs_data, monkeypatch):
    gcs_audit_records_config_1_df = pd.read_csv(
        os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'gcs_audit_records_config_1.csv'))
    gcs_audit_records_config_2_df = pd.read_csv(
        os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'gcs_audit_records_config_2.csv'))
    gcs_audit_records_dfs = [gcs_audit_records_config_1_df, gcs_audit_records_config_2_df]

    bigquery_audit_records_config_1_df = pd.read_csv(
        os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'bigquery_audit_records_config_1.csv'))
    bigquery_audit_records_config_2_df = pd.read_csv(
        os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'bigquery_audit_records_config_2.csv'))
    bigquery_audit_records_dfs = [bigquery_audit_records_config_1_df, bigquery_audit_records_config_2_df]

    def mock_get_drs_policies(datastore_config):
        return drs_data

    def mock_write_to_gcs(bucket, file_location, audit_records_gcs_df):
        return 'test/test.csv'

    def mock_load_config(config_file_path):
        config = dict()
        config['project'] = "test"
        config['topic'] = "test"
        config['report_bucket'] = "test_bucket"
        config['report_location'] = "test/report"
        config['report_dataset'] = "test_dataset"
        config['report_table'] = "test_table"
        config['status_table'] = "source_table"
        config['datastore_config'] = "test"
        config['datastore_config'] = "test"
        config['slack_credentials_path'] = "gs://test/test.json"
        return config

    def mock_get_last_successful_run_date(project, dataset, status_table):
        return datetime.strptime('2020-06-06', '%Y-%m-%d')

    multiprocess_function.side_effect = [gcs_audit_records_dfs, bigquery_audit_records_dfs]
    monkeypatch.setattr('generate_report.report.get_drs_policies', mock_get_drs_policies)
    monkeypatch.setattr('generate_report.report.load_config', mock_load_config)
    monkeypatch.setattr('generate_report.report.write_to_gcs', mock_write_to_gcs)
    monkeypatch.setattr('generate_report.report.get_last_successful_run_date', mock_get_last_successful_run_date)
    generate_report(config_file)


@pytest.mark.parametrize("config_file, drs_data", test_generate_report_data)
@mock.patch("generate_report.report.multiprocess_function")
def test_generate_report_no_data(multiprocess_function, config_file, drs_data, monkeypatch):
    gcs_empty_records_df = pd.DataFrame(
        columns=['config_id', 'project_id', 'days_to_retain', 'partition_date', 'partition_files_size_gcs',
                 'anomaly_detected_gcs', 'previous_partition_date_gcs',
                 'total_previous_partition_files_size',
                 'variation_with_previous_partition_files_size'])
    bigquery_empty_records_df = pd.DataFrame(
        columns=['config_id', 'project_id', 'days_to_retain', 'dataset', 'table_name',
                 'partition_column', 'partition_date', 'partition_row_count',
                 'previous_partition_date_bq', 'previous_partition_row_count',
                 'variation_with_previous_partition_row_count'])

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

    def mock_get_drs_policies(datastore_config):
        return drs_data

    def mock_get_last_successful_run_date(project, dataset, status_table):
        return datetime.strptime('2020-06-06', '%Y-%m-%d')

    multiprocess_function.side_effect = [[gcs_empty_records_df], [bigquery_empty_records_df]]
    monkeypatch.setattr('generate_report.report.load_config', mock_load_config)
    monkeypatch.setattr('generate_report.report.get_drs_policies', mock_get_drs_policies)
    monkeypatch.setattr('generate_report.report.get_last_successful_run_date', mock_get_last_successful_run_date)

    generate_report(config_file)


@pytest.mark.parametrize("dt_utcnow", test_generate_report_payload_data)
def test_generate_report_payload(dt_utcnow):
    report_data_df = pd.read_csv(os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'audit_records.csv'))
    report_data_df.insert(0, 'report_date',
                          datetime.strptime(dt_utcnow.strftime('%Y-%m-%dT%H:%M:%S'), '%Y-%m-%dT%H:%M:%S'))
    expected_trash_date = dt_utcnow + timedelta(days=7)
    actual_df = generate_report_payload(report_data_df, expected_trash_date)

    expected_trash_date_str = expected_trash_date.strftime('%Y-%m-%d')
    expected_purge_date_str = (expected_trash_date + timedelta(days=7)).strftime('%Y-%m-%d')
    report_data_df['previous_partition_row_count'] = report_data_df['previous_partition_row_count'].astype('Int64')
    report_data_df['anomaly_detected_gcs_tmp'] = report_data_df['anomaly_detected_gcs'].fillna(False).astype(bool)
    report_data_df['anomaly_detected_bq_tmp'] = report_data_df['anomaly_detected_bq'].fillna(False).astype(bool)
    report_data_df['expected_trash_date'] = np.where(
        (report_data_df.anomaly_detected_gcs_tmp | report_data_df.anomaly_detected_bq_tmp),
        "", expected_trash_date_str)
    report_data_df['expected_purge_date'] = np.where(
        (report_data_df.anomaly_detected_gcs_tmp | report_data_df.anomaly_detected_bq_tmp),
        "", expected_purge_date_str)
    del report_data_df['anomaly_detected_gcs_tmp']
    del report_data_df['anomaly_detected_bq_tmp']

    assert_frame_equal(report_data_df, actual_df)


@pytest.mark.parametrize("bucket, report_location, dt_utcnow", test_write_to_gcs_data)
@mock.patch("google.cloud.storage.Client")
def test_write_to_gcs(mock_client, bucket, report_location, dt_utcnow):
    report_data_df = pd.read_csv(os.path.join(ROOT_DIR, 'tests', 'generate_report', 'resources', 'audit_records.csv'))
    expected_trash_date = dt_utcnow + timedelta(days=7)
    report_data_df.insert(1, 'report_date',
                          datetime.strptime(dt_utcnow.strftime('%Y-%m-%dT%H:%M:%S'), '%Y-%m-%dT%H:%M:%S'))
    expected_trash_date_str = expected_trash_date.strftime('%Y-%m-%d')
    expected_purge_date_str = (expected_trash_date + timedelta(days=7)).strftime('%Y-%m-%d')
    report_data_df['previous_partition_row_count'] = report_data_df['previous_partition_row_count'].astype('Int64')
    report_data_df['expected_trash_date'] = np.where(
        ~(report_data_df.anomaly_detected_gcs | report_data_df.anomaly_detected_bq), expected_trash_date_str, '')
    report_data_df['expected_purge_date'] = np.where(
        ~(report_data_df.anomaly_detected_gcs | report_data_df.anomaly_detected_bq), expected_purge_date_str, '')

    dt_utcnow_str = dt_utcnow.strftime('%Y-%m-%dT%H:%M:%S')
    expected_file_key = f"{report_location}/drs_{dt_utcnow_str}.csv"

    actual_file_key = write_to_gcs(bucket, report_location, report_data_df)

    mock_client().get_bucket.assert_called_with(bucket)
    mock_client().get_bucket().blob.assert_called_with(expected_file_key)
    mock_client().get_bucket().blob().upload_from_string.assert_called_with(report_data_df.to_csv(index=False),
                                                                            'text/csv')
    assert actual_file_key == expected_file_key


@pytest.mark.parametrize("expected_schema", test_get_job_config_date)
def test_get_job_config(expected_schema):
    job_config = get_job_config()
    assert len(job_config.schema) == 29
    assert job_config.create_disposition == "CREATE_IF_NEEDED"
    assert job_config.source_format == "CSV"
    assert job_config.skip_leading_rows == 1
    for schema_field, schema in zip(job_config.schema, expected_schema):
        assert schema_field.name == schema[0]
        assert schema_field.field_type == schema[1]


@pytest.mark.parametrize("project, dataset, table, uri", test_write_to_bq_data)
@mock.patch("generate_report.report.get_job_config")
@mock.patch("google.cloud.bigquery.Client")
def test_write_to_bq(mock_client, job_config, project, dataset, table, uri):
    write_to_bq(project, dataset, table, uri)
    mock_client().load_table_from_uri.assert_called_with(uri, f"{project}.{dataset}.{table}",
                                                         job_config=job_config())
    mock_client().load_table_from_uri().result.assert_called()


@pytest.mark.parametrize("project, topic, data", send_pubsub_message_data)
@mock.patch("google.cloud.pubsub_v1.PublisherClient")
def test_send_pubsub_message(mock_client, project, topic, data):
    send_pubsub_message(project, topic, data)
    topic_path = mock_client().topic_path.asset_called_with(project, topic)
    future = mock_client().publish(topic_path.return_value, data="DRS Report".encode('utf-8'), attrs=data)
    future.result.assert_called()


@pytest.mark.parametrize("project, dataset, status_table, expected_select_query",
                         test_get_last_successful_run_date_select_query_data)
def test_get_last_successful_run_date_select_query(project, dataset, status_table, expected_select_query):
    actual_select_query = get_last_successful_run_date_select_query(project, dataset, status_table)
    assert expected_select_query == actual_select_query


@pytest.mark.parametrize("project, dataset, source_table", test_get_last_successful_run_date_data)
@mock.patch("google.cloud.bigquery.Client")
def test_get_last_successful_run_date(mock_client, project, dataset, source_table):
    mock_client().query().result.return_value = [[datetime.strptime('2020-08-13', '%Y-%m-%d')]]
    last_successful_run_date = get_last_successful_run_date(project, dataset, source_table)
    assert last_successful_run_date == datetime.strptime('2020-08-13', '%Y-%m-%d').date()


@pytest.mark.parametrize("project, dataset, source_table", test_get_last_successful_run_date_data)
@mock.patch("google.cloud.bigquery.Client")
def test_get_last_successful_run_date_exception(mock_client, project, dataset, source_table):
    mock_client().query.return_value = [Exception('Table doesn\'t exist')]
    last_successful_run_date = get_last_successful_run_date(project, dataset, source_table)
    assert last_successful_run_date is None


@pytest.mark.parametrize("project, dataset, source_table", test_get_last_successful_run_date_data)
@mock.patch("generate_report.report.get_job_config_for_status_table")
@mock.patch("google.cloud.bigquery.Client")
def test_update_status_table_with_timestamp(mock_client, job_config, project, dataset, source_table):
    df = pd.DataFrame(data=[[datetime.utcnow()]], columns=["last_successful_run"])
    update_status_table_with_timestamp(project, dataset, source_table, df, job_config)
    mock_client().load_table_from_dataframe.assert_called()
    mock_client().load_table_from_dataframe().result.assert_called()


def test_get_job_config_for_status_table():
    job_config = get_job_config_for_status_table()
    assert len(job_config.schema) == 1
    assert job_config.create_disposition == "CREATE_IF_NEEDED"
    assert job_config.schema[0].name == 'last_successful_run'
    assert job_config.schema[0].field_type == 'TIMESTAMP'


def test_get_payload_for_status_table():
    df = get_payload_for_status_table()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.columns == ["last_successful_run"]
