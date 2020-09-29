import json
import re
import uuid
from datetime import timedelta, datetime

import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.cloud import datastore
from google.cloud import pubsub_v1
from google.cloud import storage

from common.utils import GcsUri, load_config, get_logger, ReportEvent, multiprocess_function, execute_query

logger = get_logger()

DATETIME_NOW = datetime.utcnow()

GCS_COLUMNS = ['config_id', 'project_id', 'days_to_retain', 'partition_date', 'partition_path',
               'total_partition_files_size', 'total_partition_files', 'anomaly_detected_gcs',
               'previous_partition_date_gcs', 'total_previous_partition_files_size',
               'variation_with_previous_partition_files_size']

BQ_COLUMNS = ['config_id', 'project_id', 'days_to_retain', 'partition_date', 'dataset', 'table_name',
              'partition_column', 'partition_row_count', 'anomaly_detected_bq', 'previous_partition_date_bq',
              'previous_partition_row_count', 'variation_with_previous_partition_row_count']

REPORT_COLUMNS = ["uid", "report_date", "config_id", "project_id", "days_to_retain", "partition_date", "partition_path",
                  "total_partition_files_size", "total_partition_files", "anomaly_detected_gcs",
                  "previous_partition_date_gcs", "total_previous_partition_files_size",
                  "variation_with_previous_partition_files_size", "dataset", "table_name", "partition_column",
                  "partition_row_count", "anomaly_detected_bq", "previous_partition_date_bq",
                  "previous_partition_row_count", "variation_with_previous_partition_row_count", "expected_trash_date",
                  "expected_purge_date", "trashed_date_gcs", "purged_date_gcs", "trashed_date_bq", "purged_date_bq",
                  "error_gcs", "error_bq"]


class ReportRecord:
    def __init__(self, partition_date, partition_size, anomaly=None, previous_partition_date=None,
                 previous_partition_size=None, variation=None):
        self.partition_date = partition_date
        self.partition_size = partition_size
        self.anomaly = anomaly
        self.previous_partition_date = previous_partition_date
        self.previous_partition_size = previous_partition_size
        self.variation = variation


def get_drs_policies(datastore_config: dict):
    ds_client = datastore.Client()
    kind = datastore_config['kind']
    namespace = datastore_config['namespace']
    query = ds_client.query(namespace=namespace, kind=kind)
    result = list()
    for entity in list(query.fetch()):
        if entity['retention_policy']['legalProcessing'].lower() == 'true':
            continue
        data = dict()
        data["config_id"] = entity.key.name
        data["days_retain"] = int(entity['retention_policy']['daysToRetain'])
        data["anomaly_detection"] = True if entity['retention_policy']['anomalyDetection'].lower() == 'true' else False
        data["anomaly_detection_variation"] = int(entity['retention_policy'].get('anomalyVariation', '10'))
        data['project_id'] = entity['retention_policy']['projectId']
        data["dataset"] = entity['retention_policy'].get('dataset')
        data["table_name"] = entity['retention_policy'].get('tableName')
        data["partition_column"] = entity['retention_policy'].get('partitionColumn')
        data['gcs_path'] = entity['retention_policy'].get('gcsPath')
        result.append(data)
    return result


def trash_and_anomaly_records(partition_data: list, anomaly_variation_threshold: int, last_retention_date):
    report_records = list()
    partition_date = partition_data[0][0]
    partition_size = partition_data[0][1]
    if last_retention_date and len(partition_data) == 1 and partition_date == last_retention_date:
        return report_records

    if len(partition_data) == 1:
        report_records.append(ReportRecord(partition_date, partition_size, False))
    else:
        if not last_retention_date:
            report_records.append(ReportRecord(partition_date, partition_size, False))

        for previous_partition, current_partition in zip(partition_data, partition_data[1:]):
            current_partition_date = current_partition[0]
            current_partition_size = current_partition[1]
            previous_partition_size = previous_partition[1]

            if previous_partition_size > 0:
                anomaly_variation = 100 * abs(
                    (current_partition_size - previous_partition_size) / previous_partition_size)
                if anomaly_variation > anomaly_variation_threshold:
                    previous_partition_date = previous_partition[0]

                    report_records.append(ReportRecord(current_partition_date, current_partition_size, True,
                                                       previous_partition_date, previous_partition_size,
                                                       round(anomaly_variation, 2)))
                else:
                    report_records.append(ReportRecord(current_partition_date, current_partition_size, False))
            else:
                report_records.append(ReportRecord(current_partition_date, current_partition_size, False))
    return report_records


def get_select_partition_date_count_query(table_id: str, partition_column: str, retention_date: datetime.date,
                                          last_retention_date: datetime.date):
    select = f"select format_datetime('%Y-%m-%d', datetime({partition_column})) as partition_date, count(*) " \
        f"as row_count from {table_id}"
    if last_retention_date:
        where_clause = f"where {partition_column} >= TIMESTAMP '{last_retention_date}' AND " \
            f"{partition_column} < TIMESTAMP '{retention_date}' group by format_datetime('%Y-%m-%d', " \
            f"datetime({partition_column}))"
    else:
        where_clause = f"where {partition_column} < TIMESTAMP '{retention_date}' group by " \
            f"format_datetime('%Y-%m-%d', datetime({partition_column}))"

    return f"{select} {where_clause}"


def get_current_last_retention_date(days_retain: int, anomaly_detection: bool, last_successful_run_date: datetime.date):
    retention_days = timedelta(days=7) - timedelta(days=days_retain)
    current_date = DATETIME_NOW.date()
    retention_date = current_date + retention_days

    if last_successful_run_date and anomaly_detection:
        retention_days = retention_days - timedelta(days=1)

    if last_successful_run_date:
        last_retention_date = last_successful_run_date + retention_days
    else:
        last_retention_date = None

    return retention_date, last_retention_date


def get_bigquery_audit_records(last_successful_run_date: datetime.date, drs_policy: dict):
    days_retain = drs_policy["days_retain"]
    dataset = drs_policy["dataset"]
    table_name = drs_policy["table_name"]
    project_id = drs_policy["project_id"]
    partition_column = drs_policy["partition_column"]
    anomaly_detection = drs_policy['anomaly_detection']
    table_id = f"`{project_id}.{dataset}.{table_name}`"

    try:
        retention_date, last_retention_date = get_current_last_retention_date(days_retain, anomaly_detection,
                                                                              last_successful_run_date)
        select_query = get_select_partition_date_count_query(table_id, partition_column, retention_date,
                                                             last_retention_date)
        bq_client = bigquery.Client(project=project_id)
        records_to_trash = list(execute_query(bq_client, select_query))
        bigquery_audit_records = list()
        if records_to_trash:
            records_to_trash = [(partition_date, row_count) for partition_date, row_count in
                                records_to_trash]
            sorted_records_to_trash = sorted(records_to_trash, key=lambda x: x[0])

            if anomaly_detection:
                report_records = trash_and_anomaly_records(sorted_records_to_trash,
                                                           drs_policy["anomaly_detection_variation"],
                                                           last_retention_date)
            else:
                report_records = [ReportRecord(partition_date, row_count) for partition_date, row_count in
                                  sorted_records_to_trash]
            if report_records:
                for report_record in report_records:
                    rec = {'partition_date': datetime.strptime(report_record.partition_date, '%Y-%m-%d').date(),
                           'partition_row_count': report_record.partition_size,
                           "anomaly_detected_bq": report_record.anomaly,
                           "previous_partition_date_bq":
                               datetime.strptime(report_record.previous_partition_date, '%Y-%m-%d').date()
                               if report_record.previous_partition_date else None,
                           "previous_partition_row_count": report_record.previous_partition_size,
                           "variation_with_previous_partition_row_count": report_record.variation
                           }
                    bigquery_audit_records.append(rec)
                bigquery_audit_records_df = pd.DataFrame(data=bigquery_audit_records,
                                                         columns=bigquery_audit_records[0].keys())
                bigquery_audit_records_df.insert(1, 'partition_column', drs_policy['partition_column'])
                bigquery_audit_records_df.insert(1, 'table_name', table_name)
                bigquery_audit_records_df.insert(1, 'dataset', drs_policy['dataset'])
                bigquery_audit_records_df.insert(0, 'days_to_retain', drs_policy['days_retain'])
                bigquery_audit_records_df.insert(0, 'project_id', drs_policy['project_id'])
                bigquery_audit_records_df.insert(0, 'config_id', drs_policy['config_id'])
                logger.info(f"Collected partition records for `{table_name}`")
                return bigquery_audit_records_df
            else:
                logger.info(f"No BigQuery partition records to collect for `{table_id}`")
                return pd.DataFrame(columns=BQ_COLUMNS)
        else:
            logger.info(f"No BigQuery partition records to collect for `{table_id}`")
            return pd.DataFrame(columns=BQ_COLUMNS)
    except Exception as ex:
        logger.error(f"Unable to get bigquery audit records for `{table_id}` due to `{ex}`")


def get_gcs_partition_data(project_id: str, gcs_path: str, retention_date: datetime.date,
                           last_retention_date: datetime.date) -> list:
    gcs_uri = GcsUri(gcs_path)
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(gcs_uri.bucket_name)

    partition_data = list()
    for blob in bucket.list_blobs(prefix=gcs_uri.file_key):
        match = re.match(r'.*/year=(\d{4})/month=([1-9]|1[0-2])/day=([1-9]|[12]\d|3[01])/', blob.name)
        if not blob.name.endswith('/') and match:
            year = match.group(1)
            month = match.group(2)
            day = match.group(3)
            partition_date = datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d').date()
            if last_retention_date:
                if last_retention_date <= partition_date < retention_date:
                    partition_data.append(
                        (partition_date, f"gs://{gcs_uri.bucket_name}/{match.group(0)}", blob.size))
            else:
                if partition_date < retention_date:
                    partition_data.append(
                        (partition_date, f"gs://{gcs_uri.bucket_name}/{match.group(0)}", blob.size))
    return partition_data


def get_gcs_audit_records_df(report_records: list, gcs_partition_records_grouped_df: pd.DataFrame, days_retain: int,
                             project_id: str, config_id: str):
    gcs_anomaly_records_df = pd.DataFrame(
        [(report_record.partition_date, report_record.anomaly, report_record.partition_size,
          report_record.previous_partition_date, report_record.previous_partition_size,
          report_record.variation) for report_record in report_records],
        columns=['partition_date', 'anomaly_detected_gcs', 'total_partition_files_size',
                 'previous_partition_date_gcs', 'total_previous_partition_files_size',
                 'variation_with_previous_partition_files_size'])
    gcs_audit_records_df = gcs_partition_records_grouped_df.merge(gcs_anomaly_records_df,
                                                                  on=['partition_date',
                                                                      'total_partition_files_size'])
    gcs_audit_records_df.insert(0, 'days_to_retain', days_retain)
    gcs_audit_records_df.insert(0, 'project_id', project_id)
    gcs_audit_records_df.insert(0, 'config_id', config_id)
    return gcs_audit_records_df


def get_gcs_audit_records(last_successful_run_date: datetime.date, drs_policy: dict):
    project_id = drs_policy['project_id']
    gcs_path = drs_policy['gcs_path']
    days_retain = drs_policy["days_retain"]
    anomaly_detection = drs_policy['anomaly_detection']
    try:
        retention_date, last_retention_date = get_current_last_retention_date(days_retain, anomaly_detection,
                                                                              last_successful_run_date)
        partition_data = get_gcs_partition_data(project_id, gcs_path, retention_date, last_retention_date)
        if partition_data:
            sorted_partition_data = sorted(partition_data, key=lambda x: x[0])
            gcs_partition_records_df = pd.DataFrame(data=sorted_partition_data,
                                                    columns=['partition_date', 'partition_path',
                                                             'total_partition_files_size'])
            gcs_partition_records_grouped_df = gcs_partition_records_df.groupby(['partition_date', 'partition_path'])[
                'total_partition_files_size']. \
                agg(total_partition_files_size='sum', total_partition_files='count').reset_index()

            sorted_grouped_partition_data = list(
                gcs_partition_records_grouped_df[['partition_date', 'total_partition_files_size']].itertuples(
                    index=False, name=None))
            if anomaly_detection:
                report_records = trash_and_anomaly_records(sorted_grouped_partition_data,
                                                           drs_policy["anomaly_detection_variation"],
                                                           last_retention_date)
            else:
                report_records = [ReportRecord(partition_date, files_size) for partition_date, files_size in
                                  sorted_grouped_partition_data]
            if report_records:
                gcs_audit_records_df = get_gcs_audit_records_df(report_records, gcs_partition_records_grouped_df,
                                                                drs_policy['days_retain'], drs_policy['project_id'],
                                                                drs_policy['config_id'])
                logger.info(f"Collected partition records for `{gcs_path}`")
                return gcs_audit_records_df
        logger.info(f"No GCS partition records for `{gcs_path}`")
        return pd.DataFrame(columns=GCS_COLUMNS)
    except Exception as ex:
        logger.error(f"Error getting GCS data for `{gcs_path}` due to `{ex}`")


def get_last_successful_run_date_select_query(project: str, dataset: str, status_table: str):
    table_id = f"{project}.{dataset}.{status_table}"
    return f"select * from `{table_id}`"


def get_last_successful_run_date(project: str, dataset: str, status_table: str):
    bq_client = bigquery.Client(project=project)
    try:
        select_query = get_last_successful_run_date_select_query(project, dataset, status_table)
        result = execute_query(bq_client, select_query)
        last_successful_run_date = list(result)[0][0].date()
        logger.info(f'Found last successful run date so running DRS in Delta load')
        return last_successful_run_date
    except Exception as ex:
        logger.info(f'Unable to get last successful run date so running DRS in Full load')


def generate_report(config_file_path: str):
    config = load_config(config_file_path)
    drs_policies = get_drs_policies(config['datastore_config'])
    last_successful_run_date = get_last_successful_run_date(config['project'], config['report_dataset'],
                                                            config['status_table'])

    drs_policies_bq = [drs_policy for drs_policy in drs_policies if
                       drs_policy.get('partition_column') and drs_policy.get('table_name') and drs_policy.get(
                           'dataset')]
    bigquery_audit_records_dfs = multiprocess_function(get_bigquery_audit_records, drs_policies_bq,
                                                       last_successful_run_date)
    bigquery_audit_records_filtered_dfs = [df for df in bigquery_audit_records_dfs if df is not None]

    drs_policies_gcs = [drs_policy for drs_policy in drs_policies if drs_policy.get('gcs_path')]
    gcs_audit_records_dfs = multiprocess_function(get_gcs_audit_records, drs_policies_gcs,
                                                  last_successful_run_date)
    gcs_audit_records_filtered_dfs = [df for df in gcs_audit_records_dfs if df is not None]
    merged_audit_records_df = merge_gcs_bigquery_audit_records(gcs_audit_records_filtered_dfs,
                                                               bigquery_audit_records_filtered_dfs)

    if not merged_audit_records_df.empty:
        trash_date = DATETIME_NOW + timedelta(days=7)
        audit_records_gcs_df = generate_report_payload(merged_audit_records_df, trash_date)
        file_object = write_to_gcs(config['report_bucket'], config['report_location'], audit_records_gcs_df)
        write_to_bq(config['project'], config['report_dataset'], config['report_table'],
                    f"gs://{config['report_bucket']}/{file_object}")

        report_event = ReportEvent(DATETIME_NOW.strftime('%Y-%m-%dT%H:%M:%S'), config['report_bucket'], file_object,
                                   trash_date.strftime('%Y-%m-%d'))
        event = json.dumps(report_event.to_json())
        send_pubsub_message(config['project'], config['topic'], event)

        df = get_payload_for_status_table()
        job_config = get_job_config_for_status_table()
        update_status_table_with_timestamp(config['project'], config['report_dataset'], config['status_table'], df,
                                           job_config)
        logger.info("Completed generating report")
    else:
        logger.warning("No data to report")


def merge_gcs_bigquery_audit_records(gcs_audit_records_dfs: [pd.DataFrame], bigquery_audit_records_dfs: [pd.DataFrame]):
    if not gcs_audit_records_dfs and not bigquery_audit_records_dfs:
        return pd.DataFrame()

    if not gcs_audit_records_dfs:
        gcs_audit_records_df = pd.DataFrame(columns=GCS_COLUMNS)
    elif len(gcs_audit_records_dfs) == 1:
        gcs_audit_records_df = gcs_audit_records_dfs[0]
    else:
        gcs_audit_records_df = pd.concat(gcs_audit_records_dfs)

    if not bigquery_audit_records_dfs:
        bigquery_audit_records_df = pd.DataFrame(columns=BQ_COLUMNS)
    elif len(bigquery_audit_records_dfs) == 1:
        bigquery_audit_records_df = bigquery_audit_records_dfs[0]
    else:
        bigquery_audit_records_df = pd.concat(bigquery_audit_records_dfs)

    audit_records_df = gcs_audit_records_df.merge(bigquery_audit_records_df, how='outer',
                                                  on=['config_id', 'project_id', 'days_to_retain', 'partition_date'])
    dt_utcnow = DATETIME_NOW.replace(microsecond=0)
    audit_records_df.insert(0, 'report_date', dt_utcnow)
    audit_records_df = audit_records_df.sort_values(['config_id', 'partition_date'])
    audit_records_df = audit_records_df.reset_index(drop=True)
    logger.info("Completed merging GCS and BigQuery audit records")
    return audit_records_df


def generate_report_payload(audit_records_df: pd.DataFrame, expected_trash_date: datetime):
    expected_purge_date_str = (expected_trash_date + timedelta(days=7)).strftime("%Y-%m-%d")
    expected_trash_date_str = expected_trash_date.strftime("%Y-%m-%d")

    def human_readable_size(size, units=('Bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB')):
        for unit in units:
            if size < 1024.0:
                return "%.2f %s" % (size, unit)
            size /= 1024.0

    if not audit_records_df['total_partition_files_size'].isnull().values.all():
        audit_records_df['total_partition_files_size'] = audit_records_df[
            'total_partition_files_size'].apply(lambda size: human_readable_size(size) if size else None)

    if not audit_records_df['total_previous_partition_files_size'].isnull().values.all():
        audit_records_df['total_previous_partition_files_size'] = audit_records_df[
            'total_previous_partition_files_size'].apply(lambda size: human_readable_size(size) if size else None)

    audit_records_df[
        ['days_to_retain', 'previous_partition_row_count', 'partition_row_count', 'total_partition_files']] = \
        audit_records_df[
            ['days_to_retain', 'previous_partition_row_count', 'partition_row_count', 'total_partition_files']].astype(
            'Int64')
    audit_records_df['anomaly_detected_gcs_tmp'] = audit_records_df['anomaly_detected_gcs'].fillna(False).astype(bool)
    audit_records_df['anomaly_detected_bq_tmp'] = audit_records_df['anomaly_detected_bq'].fillna(False).astype(bool)
    audit_records_df['expected_trash_date'] = np.where(
        (audit_records_df.anomaly_detected_gcs_tmp | audit_records_df.anomaly_detected_bq_tmp),
        "", expected_trash_date_str)
    audit_records_df['expected_purge_date'] = np.where(
        (audit_records_df.anomaly_detected_gcs_tmp | audit_records_df.anomaly_detected_bq_tmp),
        "", expected_purge_date_str)
    del audit_records_df['anomaly_detected_gcs_tmp']
    del audit_records_df['anomaly_detected_bq_tmp']
    audit_records_df.insert(0, 'uid', None)
    audit_records_df['uid'] = audit_records_df.apply(lambda uid: str(uuid.uuid4()), axis=1)
    audit_records_df['trashed_date_gcs'] = None
    audit_records_df['purged_date_gcs'] = None
    audit_records_df['trashed_date_bq'] = None
    audit_records_df['purged_date_bq'] = None
    audit_records_df['error_gcs'] = None
    audit_records_df['error_bq'] = None
    audit_records_df = audit_records_df[REPORT_COLUMNS]
    logger.info("Completed generating GCS payload")
    return audit_records_df


def write_to_gcs(bucket_name: str, file_location: str, df: pd.DataFrame):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    dt_utcnow_str = DATETIME_NOW.strftime('%Y-%m-%dT%H:%M:%S')
    file_object = f"{file_location}/drs_{dt_utcnow_str}.csv"
    bucket.blob(file_object).upload_from_string(df.to_csv(index=False), 'text/csv')
    logger.info(f'Successfully loaded the report `{file_object}` into GCS')
    return file_object


def get_job_config():
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job_config.schema = [
        bigquery.SchemaField("uid", "STRING"),
        bigquery.SchemaField("report_date", "TIMESTAMP"),
        bigquery.SchemaField("config_id", "STRING"),
        bigquery.SchemaField("project_id", "STRING"),
        bigquery.SchemaField("days_to_retain", "INT64"),
        bigquery.SchemaField("partition_date", "DATE"),
        bigquery.SchemaField("partition_path", "STRING"),
        bigquery.SchemaField("total_partition_files_size", "STRING"),
        bigquery.SchemaField("total_partition_files", "INT64"),
        bigquery.SchemaField("anomaly_detected_gcs", "BOOLEAN"),
        bigquery.SchemaField("previous_partition_date_gcs", "DATE"),
        bigquery.SchemaField("total_previous_partition_files_size", "STRING"),
        bigquery.SchemaField("variation_with_previous_partition_files_size", "FLOAT64"),
        bigquery.SchemaField("dataset", "STRING"),
        bigquery.SchemaField("table_name", "STRING"),
        bigquery.SchemaField("partition_column", "STRING"),
        bigquery.SchemaField("partition_row_count", "INT64"),
        bigquery.SchemaField("anomaly_detected_bq", "BOOLEAN"),
        bigquery.SchemaField("previous_partition_date_bq", "DATE"),
        bigquery.SchemaField("previous_partition_row_count", "INT64"),
        bigquery.SchemaField("variation_with_previous_partition_row_count", "FLOAT64"),
        bigquery.SchemaField("expected_trash_date", "DATE"),
        bigquery.SchemaField("expected_purge_date", "DATE"),
        bigquery.SchemaField("trashed_date_gcs", "TIMESTAMP"),
        bigquery.SchemaField("purged_date_gcs", "TIMESTAMP"),
        bigquery.SchemaField("trashed_date_bq", "TIMESTAMP"),
        bigquery.SchemaField("purged_date_bq", "TIMESTAMP"),
        bigquery.SchemaField("error_gcs", "STRING"),
        bigquery.SchemaField("error_bq", "STRING")
    ]
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    return job_config


def write_to_bq(project: str, dataset: str, table: str, uri: str):
    bq_client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"
    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=get_job_config())
    logger.info(f"Starting job {load_job.job_id}")
    load_job.result()
    logger.info(f'Successfully loaded the report into BiqQuery table `{table_id}`')


def get_payload_for_status_table():
    return pd.DataFrame(data=[[DATETIME_NOW]], columns=['last_successful_run'])


def get_job_config_for_status_table():
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job_config.schema = [
        bigquery.SchemaField("last_successful_run", "TIMESTAMP"),
    ]
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    return job_config


def update_status_table_with_timestamp(project: str, dataset: str, status_table: str, df: pd.DataFrame,
                                       job_config: bigquery.LoadJobConfig):
    bq_client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{status_table}"
    load_job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()
    logger.info(f"Updated status table `{table_id}` with timestamp")


def send_pubsub_message(project: str, topic: str, data: dict):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic)
    future = publisher.publish(topic_path, data="DRS Report".encode('utf-8'), attrs=data)
    future.result()
    logger.info(f"Published message to the topic `{topic}`")
