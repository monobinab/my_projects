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

from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
import httplib2
import os
import ConfigParser


def service1(config_file):
    """
    This function creates the service from service account key file and org_api_version.

    :param config_file: Input config file contains key file location, which is used for creating credentials.
    :return: It returns service object.
    """
    parser = ConfigParser.SafeConfigParser()
    parser.read(config_file)
    try:
        key_file = parser.get('property', 'key_file')
    except ConfigParser.NoOptionError:
        print "Please Provide Service Account Key File"
        raise ValueError


    try:
        org_api_version = parser.get('property', 'org_api_version')
    except:
        org_api_version = 'v1'

    scope = ['https://www.googleapis.com/auth/cloud-platform',
             'https://www.googleapis.com/auth/cloudplatformorganizations.readonly']

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_file

    credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file, scope)
    http = httplib2.Http()
    credentials.authorize(http)
    service1 = build('cloudresourcemanager', org_api_version, credentials=credentials)
    return service1


def service2(config_file):
    """
    This function creates the service from service account key file and folder_api_version.

    :param config_file: Input config file contains key file location, which is used for creating credentials.
    :return: It returns service object.
    """
    parser = ConfigParser.SafeConfigParser()
    parser.read(config_file)
    key_file = parser.get('property', 'key_file')

    try:
        folder_api_version = parser.get('property', 'folder_api_version')
    except:
        folder_api_version = 'v2'

    scope = ['https://www.googleapis.com/auth/cloud-platform',
             'https://www.googleapis.com/auth/cloudplatformorganizations.readonly']

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_file

    credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file, scope)
    http = httplib2.Http()
    credentials.authorize(http)
    service2 = build('cloudresourcemanager', folder_api_version, credentials=credentials)
    return service2


def get_logfile_name(scriptname):
    """
    Creates log file name based on the main calling script.

    :param scriptname: Name of the main calling script.
    :return: log file name
    """
    basename = os.path.basename(scriptname)
    log_file_basename = os.path.splitext(basename)[0]
    log_file = log_file_basename+'.log'
    return log_file


def get_errorfile_name(scriptname):
    """
    This function creates error file name based on the main calling script.

    :param scriptname: Name of the main calling script.
    :return: error file name
    """
    basename = os.path.basename(scriptname)
    error_file_basename = os.path.splitext(basename)[0]
    error_file = error_file_basename+'.err'
    return error_file
