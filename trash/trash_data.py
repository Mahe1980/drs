from datetime import datetime

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound

from common.utils import collect_partitions_metadata, GCSPartitionMetadata, BQPartitionMetadata, \
    multiprocess_function, load_config, get_logger, update_audit_table, execute_query

DATETIME_NOW = datetime.utcnow()
DATE_TODAY = DATETIME_NOW.date()

logger = get_logger()


def collect_existing_trash_tables_query(project_id: str, trash_table_schema: str):
    query = (
        f"SELECT concat(table_catalog,'.',table_schema,'.',table_name) AS trash_table_id "
        f"FROM {project_id}.{trash_table_schema}.INFORMATION_SCHEMA.TABLES "
        f"where table_type='BASE TABLE'")
    return query


def collect_existing_trash_table_ids(trash_table_ids: list):
    existing_trash_table_ids = []
    for trash_table_id in trash_table_ids:
        project_id, trash_table_schema, trash_table_name = trash_table_id.split('.')
        schema_query = collect_existing_trash_tables_query(project_id, trash_table_schema)
        bq_client = bigquery.Client(project_id)
        query_result = execute_query(bq_client, schema_query)
        if query_result:
            existing_trash_table_ids = [row.trash_table_id for row in query_result]
    return existing_trash_table_ids


def collect_source_metadata_query(project: str, source_table_schema: str, source_table_name: str):
    query = (f"SELECT column_name,data_type FROM {source_table_schema}.INFORMATION_SCHEMA.COLUMNS "
             f"where table_catalog='{project}' and table_schema='{source_table_schema}' "
             f"and table_name='{source_table_name}' and is_partitioning_column='YES'")
    return query


def get_partition_by(query_result: list):
    if query_result:
        if query_result[0][1] == 'DATE':
            return f"partition by {query_result[0][0]}"
        elif query_result[0][1] == 'TIMESTAMP':
            return f"partition by DATE ({query_result[0][0]})"
    return ""


def get_missing_trash_table_ids(bq_partitions_to_be_trashed: list):
    trash_table_ids = list(
        set([bq_partition_to_be_trashed.trash_table_id for bq_partition_to_be_trashed in bq_partitions_to_be_trashed]))
    existing_trash_table_ids = collect_existing_trash_table_ids(trash_table_ids)
    return list(set(trash_table_ids) - set(existing_trash_table_ids))


def collect_create_trash_table_query(source_table_id: str, trash_table_id: str, partition_by: str):
    query = (f"create table {trash_table_id} {partition_by} as "
             f"select * from {source_table_id} where 1=0")
    return query


def create_missing_trash_tables(missing_trash_table_with_source_ids: list):
    for source_table_id, trash_table_id in missing_trash_table_with_source_ids:
        project_id, source_table_schema, source_table_name = source_table_id.split('.')
        source_metadata_query = collect_source_metadata_query(project_id, source_table_schema, source_table_name)
        bq_client = bigquery.Client(project_id)
        result = list(execute_query(bq_client, source_metadata_query))
        partition_by = get_partition_by(result)
        create_trash_table_query = collect_create_trash_table_query(source_table_id, trash_table_id, partition_by)
        execute_query(bq_client, create_trash_table_query)
        logger.info(f"Created Trash table `{trash_table_id}` successfully!")


def collect_source_table_rowcount_query(partition_column: str, source_table_id: str, partition_date: str):
    query = (f"select count({partition_column}) as count from `{source_table_id}` where "
             f"{partition_column} = TIMESTAMP '{partition_date}'")
    return query


def collect_insert_query(trash_table_id: str, source_table_id: str, partition_column: str, partition_date: str):
    insert_query = (f"insert into `{trash_table_id}` "
                    f"select * from `{source_table_id}` where "
                    f"{partition_column}=TIMESTAMP '{partition_date}'")
    return insert_query


def collect_delete_query(source_table_id: str, partition_column: str, partition_date: str):
    delete_query = (f"delete from `{source_table_id}` where "
                    f"{partition_column}= TIMESTAMP '{partition_date}'")
    return delete_query


def trash_bigquery_data(partition_to_be_trashed: BQPartitionMetadata):
    bq_client = bigquery.Client(project=partition_to_be_trashed.project_id)
    source_table_id = partition_to_be_trashed.source_table_id
    partition_date = partition_to_be_trashed.partition_date
    try:
        trash_table_id = partition_to_be_trashed.trash_table_id
        partition_column = partition_to_be_trashed.partition_column

        select_query = collect_source_table_rowcount_query(partition_column, source_table_id, partition_date)
        results = execute_query(bq_client, select_query)
        row_count = list(results)[0][0]
        if int(row_count) == 0:
            logger.info(f"No records found for partition_date `{partition_date}` from table `{source_table_id}`")
            return

        insert_query = collect_insert_query(trash_table_id, source_table_id, partition_column, partition_date)
        execute_query(bq_client, insert_query)

        delete_query = collect_delete_query(source_table_id, partition_column, partition_date)
        execute_query(bq_client, delete_query)

        logger.info(f"Completed trash for partition_date `{partition_date}` from table `{source_table_id}`")
    except Exception as e:
        logger.info(f"Unable to perform trash for partition_date `{partition_date}` from table `{source_table_id}` "
                    f"due to {str(e)}")
        return {'uid': partition_to_be_trashed.uid,
                'error_bq': str(e)}


def create_missing_trash_buckets(source_trash_buckets_with_project: list):
    for source_bucket_name, trash_bucket_name, project_id in source_trash_buckets_with_project:
        storage_client = storage.Client(project=project_id)
        trash_bucket = storage_client.lookup_bucket(trash_bucket_name)
        if not trash_bucket:
            source_bucket = storage_client.get_bucket(source_bucket_name)
            trash_bucket = storage_client.bucket(trash_bucket_name)
            trash_bucket.location = source_bucket.location
            trash_bucket.create()


def trash_gcs_data(partition_to_be_trashed: GCSPartitionMetadata):
    storage_client = storage.Client(project=partition_to_be_trashed.project_id)
    partition_path = partition_to_be_trashed.partition_path
    source_bucket_name = partition_to_be_trashed.source_bucket_name

    try:
        src_bucket = storage_client.get_bucket(source_bucket_name)
        trash_bucket = storage_client.bucket(partition_to_be_trashed.trash_bucket_name)
        src_blobs = list(src_bucket.list_blobs(prefix=partition_path))
        if len(src_blobs) == 0:
            logger.info(f"No files found to trash from `gs://{source_bucket_name}/{partition_path}`")
            return

        for src_blob in src_blobs:
            src_bucket.copy_blob(src_blob, trash_bucket)
            try:
                src_bucket.delete_blob(src_blob.name)
            except NotFound:
                pass

        logger.info(f"Completed trash for GCS from `gs://{source_bucket_name}/{partition_path}`")
    except Exception as e:
        logger.info(
            f"Unable to perform trash for parquet_path `gs://{source_bucket_name}/{partition_path}` due to {str(e)}")
        return {'uid': partition_to_be_trashed.uid,
                'error_gcs': str(e)}


def collect_partitions_metadata_query_for_trash(project: str, report_dataset: str, report_table: str):
    query = f"SELECT uid, format_datetime('%Y-%m-%d',datetime(partition_date)) as partition_date," \
        f"project_id, dataset, table_name, partition_column, partition_path " \
        f"FROM `{project}.{report_dataset}.{report_table}` " \
        f"WHERE expected_trash_date is not null and (trashed_date_gcs is null and trashed_date_bq is null) " \
        f"and format_datetime(\'%Y-%m-%d\',datetime(expected_trash_date)) <= '{DATE_TODAY}' "
    return query


def trash_data(config_file: str):
    config = load_config(config_file)
    project = config['project']
    select_query = collect_partitions_metadata_query_for_trash(project, config['report_dataset'],
                                                               config['report_table'])
    partitions_metadata = collect_partitions_metadata(project, select_query)
    gcs_partitions_to_be_trashed = partitions_metadata['gcs']
    bq_partitions_to_be_trashed = partitions_metadata['bq']
    if not gcs_partitions_to_be_trashed and not bq_partitions_to_be_trashed:
        logger.info("No partition records to trash")
        return

    bq_errors = list()
    gcs_errors = list()
    gcs_uids = list()
    bq_uids = list()
    if bq_partitions_to_be_trashed:
        missing_trash_table_ids = get_missing_trash_table_ids(bq_partitions_to_be_trashed)
        if missing_trash_table_ids:
            missing_trash_table_with_source_ids = list(set([
                (bq_partition_to_be_trashed.source_table_id, bq_partition_to_be_trashed.trash_table_id) for
                bq_partition_to_be_trashed in bq_partitions_to_be_trashed if
                bq_partition_to_be_trashed.trash_table_id in missing_trash_table_ids]))
            create_missing_trash_tables(missing_trash_table_with_source_ids)

        for bq_partition_to_be_trashed in bq_partitions_to_be_trashed:
            result = trash_bigquery_data(bq_partition_to_be_trashed)
            if result:
                bq_errors.append(result)
        bq_uids = [bq_partition_to_be_trashed.uid for bq_partition_to_be_trashed in bq_partitions_to_be_trashed]

    if gcs_partitions_to_be_trashed:
        source_trash_buckets_with_project = list(
            set([(gcs_partition_to_be_trashed.source_bucket_name, gcs_partition_to_be_trashed.trash_bucket_name,
                  gcs_partition_to_be_trashed.project_id)
                 for gcs_partition_to_be_trashed in gcs_partitions_to_be_trashed]))
        create_missing_trash_buckets(source_trash_buckets_with_project)

        gcs_errors = multiprocess_function(trash_gcs_data, gcs_partitions_to_be_trashed)
        gcs_errors = list(filter(None, gcs_errors))
        gcs_uids = [gcs_partition_to_be_trashed.uid for gcs_partition_to_be_trashed in gcs_partitions_to_be_trashed]

    gcs_update_data = {'gcs_errors': gcs_errors, 'gcs_uids': gcs_uids, 'date_gcs_column': 'trashed_date_gcs'}
    bq_update_data = {'bq_errors': bq_errors, 'bq_uids': bq_uids, 'date_bq_column': 'trashed_date_bq'}
    update_audit_table(config['project'], config['report_dataset'], config['report_table'], gcs_update_data,
                       bq_update_data)
    logger.info("Completed trash successfully!")
