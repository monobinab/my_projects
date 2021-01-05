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


import unittest
from google.cloud import bigquery
import os
import sys

CONFIG_FILE = 'config_file'

sys.path.append('..')

from source.folder_look_up import folder_lkp
from source.get_org_nm import get_org_nm
from source.get_ancestry import get_ancestry
from source.bigquery_load import bq_insert, bq_check_delete
import constants
import logging



class TestOrgFolderHierarchy(unittest.TestCase):
    """
    This class is a test suite to test functions used in main.py
    """

    def setUp(self):
        """
        Set up the test environment. It uses test.properties file to get the variables from the environment.
        Also it sets up authorization

        """
        self.key_file = self.get_env_var(constants.Constants.KEY_FILE)
        self.test_org_id = self.get_env_var(constants.Constants.TEST_ORG_ID)
        self.config_file = self.get_env_var(constants.Constants.CONFIG_FILE)
        self.test_folder_id = self.get_env_var(constants.Constants.TEST_FOLDER_ID)
        self.test_folder_name = self.get_env_var(constants.Constants.TEST_FOLDER_NAME)
        self.test_org_name = self.get_env_var('test_org_name')
        self.bq_project_id = self.get_env_var(constants.Constants.BQ_PROJECT_ID)
        self.dataset_id = self.get_env_var(constants.Constants.DATASET_ID)
        self.table_name = self.get_env_var(constants.Constants.TABLE_NAME)
        self.folder_name_level_1 = self.get_env_var(constants.Constants.FOLDER_NAME_LEVEL_1)
        self.folder_name_level_2 = self.get_env_var(constants.Constants.FOLDER_NAME_LEVEL_2)
        self.folder_level_1 = self.get_env_var(constants.Constants.FOLDER_LEVEL_1)
        self.folder_level_2 = self.get_env_var(constants.Constants.FOLDER_LEVEL_2)
        self.org_display_name = self.get_env_var(constants.Constants.ORG_DISPLAY_NAME)
        self.project_id = self.get_env_var(constants.Constants.PROJECT_ID)
        self.error_file_obj = open(constants.Constants.ERROR_FILE, 'w')

        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.ERROR)

    @staticmethod
    def get_env_var(key_name):
        """
        Get the environment variables and save it to a variable.
        :param key_name: e.g. key_file
        """
        return os.environ.get(key_name, 'Key name is not set')

    @staticmethod
    def select_from_table(self, select_sql):
        """
        Method to run a "select" query from a table and returns results.
        :param select_sql:
        :param self:
        :return:
        """
        client = bigquery.Client.from_service_account_json(self.key_file)
        query_job = client.query(select_sql)
        result = query_job.result()
        return result

    def test_folder_look_up(self):
        """
        test folder_look_up.py
        """
        folder_display_name = folder_lkp(self.test_folder_id, self.config_file)
        self.assertEqual(folder_display_name, self.test_folder_name)

    def test_org_look_up(self):
        """
        test organization look up get_org_nm.py
        :return:
        """
        org_display_name = get_org_nm(self.test_org_id, self.config_file)
        self.assertEqual(org_display_name, self.test_org_name)

    def test_bq_insert(self):
        """
        test bq_insert function, which inserts into BQ table programmatically
        :return:
        """
        folder_dict_list = list()
        folder_dict_1 = dict()
        folder_dict_2 = dict()
        folder_dict_1['folder_name'] = self.folder_name_level_1
        folder_dict_1['folder_level'] = self.folder_level_1
        folder_dict_2['folder_name'] = self.folder_name_level_2
        folder_dict_2['folder_level'] = self.folder_level_2

        folder_dict_list.append(folder_dict_1)
        folder_dict_list.append(folder_dict_2)

        bq_insert(self.key_file, self.bq_project_id, self.dataset_id, self.table_name, self.project_id, self.org_display_name, folder_dict_list)

        select_sql = "select * from `" + self.dataset_id + '.' + self.table_name + "` join unnest(folder_structure) fs " \
                     "where projectid = '" + self.project_id + "' and fs.folder_level = 1"
        result = self.select_from_table(self,select_sql)
        for row in result:
            folder_name = row['folder_name']
            folder_level = row['folder_level']
            output_folder_structure = (folder_name, folder_level)
            expected_folder_structure_level1 = (self.folder_name_level_1, self.folder_level_1)
            expected_folder_structure_level2 = (self.folder_name_level_2, self.folder_level_2)
            if folder_level == 1:
                assert cmp(output_folder_structure, expected_folder_structure_level1)
            elif folder_level == 2:
                assert cmp(output_folder_structure, expected_folder_structure_level2)

    def test_bq_check_delete(self):
        """
        test bq_check_delete function.
        """
        bq_check_delete(self.key_file, self.bq_project_id, self.dataset_id, self.table_name, self.project_id)

        select_sql = "select count(*) as cnt from `" \
                     + self.dataset_id + '.' + self.table_name + "` where projectid = '" + self.project_id + "'"
        logging.info("SQL is : ", select_sql)
        result = self.select_from_table(self, select_sql)
        for row in result:
            count_row = row['cnt']
            assert (count_row == 0)

    def test_get_ancestry(self):
        """
        Tests get ancestry function.
        """
        folder_dict_list, org_display_name = get_ancestry(self.project_id, self.error_file_obj, self.config_file)
        logging.info(folder_dict_list)
        logging.info(org_display_name)
        for folder_dict in folder_dict_list:
            self.assertTrue(folder_dict.keys(), self.folder_name_level_1)
            self.assertTrue(folder_dict.keys(), self.folder_name_level_2)


##################################################################################################################
if __name__ == "__main__":
    unittest.main()