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


# get organization name by organization id.  This script is called in get_ancestry_by_projectid.py
def get_org_nm(resource_id, config_file):
    service1 = access.service1(config_file)
    org_name = 'organizations' + '/' + resource_id
    request_org = service1.organizations().get(name=org_name)
    response_org_obj = request_org.execute()
    org_display_name = response_org_obj['displayName']
    return org_display_name