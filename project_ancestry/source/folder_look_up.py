#!/usr/bin/python

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

import access


# folder name look up by folder id. This script is called in get_ancestry_by_projectid.py
def folder_lkp(resource_id, config_file):
    """
    This sub-module can lookup folder name if folder id and config file is passed.
    It is called by get_ancestry_by_projectid_folder_cols.get_ancestry.

    :param resource_id: folder id
    :param config_file: input parameter file to make connection with API.
    :return: It returns folder name
    """
    service = access.service2(config_file)
    folder_name = 'folders' + '/' + resource_id
    request_folder = service.folders().get(name=folder_name)
    response_folder_obj = request_folder.execute()
    folder_display_name = response_folder_obj['displayName']
    return folder_display_name
