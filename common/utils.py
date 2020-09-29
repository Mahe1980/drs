import logging
import os
import re
from functools import partial
from multiprocessing import Pool

import pandas as pd
import yaml
from google.cloud import bigquery
from google.cloud import storage


class ReportEvent:
    def __init__(self, event_time, file_bucket, file_key, trash_date):
        self.event_time = event_time
        self.file_bucket = file_bucket
        self.file_key = file_key
        self.trash_date = trash_date

    def to_json(self):
        event_dict = {
            "eventType": "OBJECT_FINALIZE",
            "eventTime": self.event_time,
            "fileBucket": self.file_bucket,
            "fileKey": self.file_key,
            "trashDate": self.trash_date
        }
        return event_dict


class GcsUri:
    def __init__(self, uri):
        self.uri = uri

    @property
    def file_key(self):
        return re.search(r"gs://[\w-]*/(.*)", self.uri).groups()[0]

    @property
    def bucket_name(self):
        return re.search(r"(gs://[\w-]*/).*", self.uri).groups()[0][5:-1]


class PartitionMetadata:
    def __init__(self, uid, partition_date, project_id):
        self.uid = uid
        self.partition_date = partition_date
        self.project_id = project_id


class GCSPartitionMetadata(PartitionMetadata):
    def __init__(self, uid, partition_date, project_id, source_bucket_name, trash_bucket_name, partition_path):
        self.source_bucket_name = source_bucket_name
        self.trash_bucket_name = trash_bucket_name
        self.partition_path = partition_path
        super(GCSPartitionMetadata, self).__init__(uid, partition_date, project_id)


class BQPartitionMetadata(PartitionMetadata):
    def __init__(self, uid, partition_date, project_id, source_table_id, partition_column, trash_table_id):
        self.source_table_id = source_table_id
        self.partition_column = partition_column
        self.trash_table_id = trash_table_id
        super(BQPartitionMetadata, self).__init__(uid, partition_date, project_id)


def get_blob(bucket_name: str, file_key: str):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    return bucket.blob(file_key)


def load_config(config_file_path: str):
    gcs_uri = GcsUri(config_file_path)
    blob = get_blob(gcs_uri.bucket_name, gcs_uri.file_key)
    config = yaml.load(blob.download_as_string(), Loader=yaml.FullLoader)

    result = dict()
    result['project'] = config['drs']['project']
    result['topic'] = config['pubsub']['report_topic']
    result['report_bucket'] = config['gcs']['report_bucket']
    result['report_location'] = config['gcs']['report_location']
    result['report_dataset'] = config['big_query']['report_dataset']
    result['report_table'] = config['big_query']['report_table']
    result['status_table'] = config['big_query']['status_table']
    result['datastore_config'] = config['datastore']
    result['slack_credentials_path'] = config['slack_credentials']['path']

    return result


def get_logger():
    logger = logging.getLogger()
    logging.basicConfig(format='%(levelname)s:%(message)s', level=os.environ.get("LOG_LEVEL", "INFO"))
    return logger


def multiprocess_function(func, iterable, *args):
    with Pool(processes=10) as pool:
        results = pool.map(partial(func, *args), iterable)
    return results


def execute_query(bq_client: bigquery.Client, query: str):
    return bq_client.query(query).result()


def collect_partitions_metadata(project: str, query: str):
    bq_client = bigquery.Client(project=project)
    query_results = execute_query(bq_client, query)
    gcs_partitions_metadata = []
    bq_partitions_metadata = []
    logger = get_logger()
    for row in query_results:
        if row.partition_path:
            gcs_uri = GcsUri(row.partition_path)
            source_bucket_name = gcs_uri.bucket_name
            trash_bucket_name = f"{source_bucket_name}-trash"
            file_key = gcs_uri.file_key
            gcs_partitions_metadata.append(
                GCSPartitionMetadata(row.uid, row.partition_date, row.project_id, source_bucket_name, trash_bucket_name,
                                     file_key))
        if row.table_name and row.dataset and row.partition_column:
            bq_partitions_metadata.append(
                BQPartitionMetadata(row.uid, row.partition_date, row.project_id,
                                    f"{row.project_id}.{row.dataset}.{row.table_name}",
                                    row.partition_column,
                                    f"{row.project_id}.{row.dataset}_trash.{row.table_name}_trash"))
    partitions_metadata = {'gcs': gcs_partitions_metadata, 'bq': bq_partitions_metadata}
    log_message = "Collected partitions metadata" if gcs_partitions_metadata or bq_partitions_metadata \
        else "No partitions metadata found"
    logger.info(log_message)
    return partitions_metadata


def get_update_query_successful_gcs(project: str, report_dataset: str, report_table: str, date_gcs_column: str,
                                    gcs_successful_uids: list):
    gcs_successful_uids_str = ("'{}'".format("','".join(gcs_successful_uids)))
    update_statement = f"update `{project}.{report_dataset}.{report_table}` " \
        f"set {date_gcs_column}=current_timestamp, error_gcs='' "
    if date_gcs_column == 'trashed_date_gcs':
        where_clause = f"where uid in ({gcs_successful_uids_str})"
    else:
        where_clause = f"where uid in ({gcs_successful_uids_str}) and trashed_date_gcs is not null"
    return f"{update_statement}{where_clause}"


def get_update_query_successful_bq(project: str, report_dataset: str, report_table: str, date_bq_column: str,
                                   bq_successful_uids: list):
    bq_successful_uids_str = ("'{}'".format("','".join(bq_successful_uids)))
    update_statement = f"update `{project}.{report_dataset}.{report_table}` " \
        f"set {date_bq_column}=current_timestamp, error_bq='' "
    if date_bq_column == 'trashed_date_bq':
        where_clause = f"where uid in ({bq_successful_uids_str})"
    else:
        where_clause = f"where uid in ({bq_successful_uids_str}) and trashed_date_bq is not null"
    return f"{update_statement}{where_clause}"


def get_update_query_error_gcs(project: str, report_dataset: str, report_table: str):
    return f"update `{project}.{report_dataset}.{report_table}` a set error_gcs=e.error_gcs " \
        f"from `{project}.{report_dataset}.error_tbl` e " \
        f"where a.uid=e.uid"


def get_update_query_error_bq(project: str, report_dataset: str, report_table: str):
    return f"update `{project}.{report_dataset}.{report_table}` a set error_bq=e.error_bq " \
        f"from `{project}.{report_dataset}.error_tbl` e " \
        f"where a.uid=e.uid"


def update_audit_table(project: str, report_dataset: str, report_table: str, gcs_update_data: dict,
                       bq_update_data: dict):
    bq_client = bigquery.Client(project=project)
    logger = get_logger()
    gcs_errors = gcs_update_data['gcs_errors']
    bq_errors = bq_update_data['bq_errors']
    gcs_unsuccessful_uids = [error['uid'] for error in gcs_errors]
    bq_unsuccessful_uids = [error['uid'] for error in bq_errors]

    gcs_successful_uids = [uid for uid in gcs_update_data['gcs_uids'] if uid not in gcs_unsuccessful_uids]
    bq_successful_uids = [uid for uid in bq_update_data['bq_uids'] if uid not in bq_unsuccessful_uids]

    if gcs_successful_uids:
        update_query = get_update_query_successful_gcs(project, report_dataset, report_table,
                                                       gcs_update_data['date_gcs_column'], gcs_successful_uids)
        execute_query(bq_client, update_query)

    if bq_successful_uids:
        update_query = get_update_query_successful_bq(project, report_dataset, report_table,
                                                      bq_update_data['date_bq_column'], bq_successful_uids)
        execute_query(bq_client, update_query)

    if gcs_errors:
        gcs_error_df = pd.DataFrame(gcs_errors)
        gcs_error_df.to_gbq(f"{report_dataset}.error_tbl", f"{project}", if_exists='replace')
        update_query = get_update_query_error_gcs(project, report_dataset, report_table)
        execute_query(bq_client, update_query)
        execute_query(bq_client, f"drop table `{project}.{report_dataset}.error_tbl`")

    if bq_errors:
        bq_error_df = pd.DataFrame(bq_errors)
        bq_error_df.to_gbq(f"{report_dataset}.error_tbl", f"{project}", if_exists='replace')
        update_query = get_update_query_error_bq(project, report_dataset, report_table)
        execute_query(bq_client, update_query)
        execute_query(bq_client, f"drop table `{project}.{report_dataset}.error_tbl`")
    logger.info('Completed updating audit table')
