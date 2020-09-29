from datetime import datetime

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound

from common.utils import load_config, collect_partitions_metadata, get_logger, GCSPartitionMetadata, \
    BQPartitionMetadata, execute_query, update_audit_table, multiprocess_function

DATETIME_NOW = datetime.utcnow()
DATE_TODAY = DATETIME_NOW.date()

logger = get_logger()


def get_delete_query(trash_table_id, partition_column, partition_date):
    return f"delete from `{trash_table_id}` where {partition_column}=TIMESTAMP '{partition_date}'"


def purge_bigquery_data(partition_to_be_purged: BQPartitionMetadata):
    bq_client = bigquery.Client(project=partition_to_be_purged.project_id)
    partition_date = partition_to_be_purged.partition_date
    trash_table_id = partition_to_be_purged.trash_table_id
    try:
        partition_column = partition_to_be_purged.partition_column
        delete_query = get_delete_query(trash_table_id, partition_column, partition_date)
        execute_query(bq_client, delete_query)
        logger.info(f"Completed purge for partition_date `{partition_date}` from table `{trash_table_id}`")
    except Exception as e:
        logger.info(f"Unable to perform purge for partition_date `{partition_date}` from table `{trash_table_id}` "
                    f"due to {str(e)}")
        return {'uid': partition_to_be_purged.uid,
                'error_bq': str(e)}


def purge_gcs_data(partition_to_be_purged: GCSPartitionMetadata):
    storage_client = storage.Client(project=partition_to_be_purged.project_id)
    partition_path = partition_to_be_purged.partition_path
    trash_bucket_name = partition_to_be_purged.trash_bucket_name

    try:
        trash_bucket = storage_client.get_bucket(trash_bucket_name)
        for trash_blob in trash_bucket.list_blobs(prefix=partition_path):
            try:
                trash_bucket.delete_blob(trash_blob.name)
            except NotFound:
                pass

        logger.info(f"Completed purge for GCS from `gs://{trash_bucket_name}/{partition_path}`")
    except Exception as e:
        logger.info(
            f"Unable to perform purge for parquet_path `gs://{trash_bucket_name}/{partition_path}` due to {str(e)}")
        return {'uid': partition_to_be_purged.uid,
                'error_gcs': str(e)}


def collect_partitions_metadata_query_for_purge(project: str, report_dataset: str, report_table: str):
    query = f"SELECT uid, format_datetime('%Y-%m-%d',datetime(partition_date)) as partition_date," \
        f"project_id, dataset, table_name, partition_column, partition_path " \
        f"FROM `{project}.{report_dataset}.{report_table}` " \
        f"WHERE expected_trash_date is not null and " \
        f"((trashed_date_gcs is not null and purged_date_gcs is null) or " \
        f"(trashed_date_bq is not null and purged_date_bq is null)) and " \
        f"format_datetime(\'%Y-%m-%d\',datetime(expected_purge_date)) <= '{DATE_TODAY}' "
    return query


def purge_data(config_file: str):
    config = load_config(config_file)
    project = config['project']
    select_query = collect_partitions_metadata_query_for_purge(project, config['report_dataset'],
                                                               config['report_table'])
    partitions_metadata = collect_partitions_metadata(project, select_query)
    gcs_partitions_to_be_purged = partitions_metadata['gcs']
    bq_partitions_to_be_purged = partitions_metadata['bq']
    if not gcs_partitions_to_be_purged and not bq_partitions_to_be_purged:
        logger.info("No partition records to purge")
        return

    bq_errors = list()
    gcs_errors = list()
    gcs_uids = list()
    bq_uids = list()
    if bq_partitions_to_be_purged:
        for bq_partition_to_be_purged in bq_partitions_to_be_purged:
            result = purge_bigquery_data(bq_partition_to_be_purged)
            if result:
                bq_errors.append(result)
        bq_uids = [bq_partition_to_be_purged.uid for bq_partition_to_be_purged in bq_partitions_to_be_purged]

    if gcs_partitions_to_be_purged:
        gcs_errors = multiprocess_function(purge_gcs_data, gcs_partitions_to_be_purged)
        gcs_errors = list(filter(None, gcs_errors))
        gcs_uids = [gcs_partition_to_be_purged.uid for gcs_partition_to_be_purged in gcs_partitions_to_be_purged]

    gcs_update_data = {'gcs_errors': gcs_errors, 'gcs_uids': gcs_uids, 'date_gcs_column': 'purged_date_gcs'}
    bq_update_data = {'bq_errors': bq_errors, 'bq_uids': bq_uids, 'date_bq_column': 'purged_date_bq'}
    update_audit_table(config['project'], config['report_dataset'], config['report_table'], gcs_update_data,
                       bq_update_data)
    logger.info("Completed purge successfully!")
