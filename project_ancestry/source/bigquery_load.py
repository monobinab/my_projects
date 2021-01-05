# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.cloud import bigquery
import logging
import json

# delete and insert single record
# after each line is read from input file, a "select count(*)" query is run to check if that projectid exists.
# Assume projectid is unique in this table
# if count(*) result is greater than 0, then it is deleted because that may be old information.
# Then, new project hierarchy info is inserted.
# else if count(*) result is not greater than 0, the new project hierarchy is just added


def bq_check_delete(key_file, bq_project, dataset_id, table_id, projectid):
    """
    Check if projectid exists in BQ table then delete that record.
    The parameters are passed from get_ancestry_by_projectid_folder_cols.get_ancestry

    :param key_file: Key file for service account
    :param bq_project: Big Query project id
    :param dataset_id: Big Query dataset id
    :param table_id: Big Query table id
    :param projectid: projectid column value in the table
    :return: It returns delete results object.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    client = bigquery.Client.from_service_account_json(key_file)

    # remove old entry
    query_str = "select count(*) as cnt from `" + bq_project + "." + dataset_id + "." + table_id + "` where projectid='"\
                + projectid + "';"
    query_job = client.query(query_str)
    results = query_job.result()
    for row in results:
        row_count = row.cnt
    if row_count > 0:
        delete_query_str = "delete  from `" + bq_project + "." + dataset_id + "." + table_id + "` where projectid='" + \
                           projectid + "';"
        delete_query_job = client.query(delete_query_str)
        delete_results = delete_query_job.result()
        return delete_results


def bq_insert(key_file, bq_project, dataset_id, table_id, projectid, org_display_name, folder_dict_list):
    """
    This function inserts into Big Query.
    The parameters are passed from get_ancestry_by_projectid_folder_cols.get_ancestry

    :param key_file: Key file for service account
    :param bq_project: Big Query project id
    :param dataset_id: Big Query dataset id
    :param table_id: Big Query table id
    :param projectid: projectid column value in the table
    :param org_display_name: organization_name column value
    :param folder_dict_list: list of folder dictionaries e.g. [{"folder_name": "DP - Test Billing Folder",
    "folder_level": 1}, {"folder_name": "Billing", "folder_level": 2}]
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    client = bigquery.Client.from_service_account_json(key_file)
    folder_dict_list_json = json.dumps(folder_dict_list)
    insert_query_str = get_insert_query(bq_project, dataset_id, folder_dict_list_json, org_display_name, projectid,
                                        table_id)
    query_job = client.query(insert_query_str)
    query_job.result()


def get_insert_query(bq_project, dataset_id, folder_dict_list_json, org_display_name, projectid, table_id):
    """
    Prepares the Insert SQL string
    :param bq_project: Big Query Project id where the dataset exists
    :param dataset_id: Big Query dataset id
    :param folder_dict_list_json: Folder Hierarchy information that we obtained for the specific project id
    :param org_display_name: Organization Name
    :param projectid: Particular Project id we are trying to insert
    :param table_id: BQ table name where we are inserting.
    :return: Insert SQL
    """
    table_qualifier = bq_project + '.' + dataset_id + '.' + table_id

    insert_sql = "insert into `" + table_qualifier + "` (" \
    "projectid, organization_name, folder_structure, create_user, create_timestamp ) " \
    "WITH `folder_structure` AS ( select " + "'" + folder_dict_list_json + "' as json_blob ) " \
    "select '" + projectid + "','" + org_display_name + "',"\
         "ARRAY( select struct( " \
                    "JSON_EXTRACT_SCALAR(split_items, '$.folder_name') as folder_name, " \
                    "CAST(JSON_EXTRACT_SCALAR(split_items, '$.folder_level') as INT64) as folder_level  " \
                    ")" \
                "FROM ( " \
                	"SELECT CONCAT('{', REGEXP_REPLACE(split_items, r'^\[{|}\]$', ''), '}') AS split_items " \
                	"FROM UNNEST(SPLIT(json_blob, '}, {')) AS split_items " \
                ")" \
          ")" \
         ", session_user(), current_timestamp() " \
    "from `folder_structure`;"

    return insert_sql


def bq_load(key_file, outfile, project_id, dataset_id, table_id):
    """
    This function loads data from a file into BQ table
    :param key_file: Service Account Key file.
    :param outfile: Data file
    :param project_id: BQ project where dataset is located
    :param dataset_id: BQ dataset id
    :param table_id: BQ table id
    """
    client = bigquery.Client.from_service_account_json(key_file)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True
    filename = open(outfile, 'rb')

    job = client.load_table_from_file(
        filename, table_ref, job_config=job_config
    )
    job.result()

