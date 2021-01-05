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

# Purpose: Get Google Cloud resource names of project hierarchy by project id lookup.
# It takes 1 parameter: a config file, where you pass input, output files information,
# BQ table etc (e.g. gcp_org_hierarchy.config.)
# There are two outputs of this script: 1. a output file (e.g. project_ancestry.txt) in this example
# 2. BigQuery table update (e.g. cardinal-data-piper-sbx.public.project_ancestry) in this example

import sys
import get_ancestry
#import get_ancestry_by_projectid_folder_cols
import logging
import access
import ConfigParser


########################################################################


def main(**kwargs):
    """
    This is the main calling script. It takes one/two arguments.
    1. Config file (mandatory parameter): Contains key file, api versions, BigQuery table information (optional).
    2. project_id or projectid_file (optional parameter): in python keyword argument format.

    The script can be run like this:
    python main.py config_file=gcp_org_hierarchy.config
    Or
    python main.py config_file=gcp_org_hierarchy.config project_id = <project id value>
    Or
    python main.py config_file=gcp_org_hierarchy.config projectid_file = <project id file location>

    :param kwargs:
    :return:
    """
    log_filename = access.get_logfile_name(sys.argv[0])
    logging.basicConfig(format='%(asctime)s %(message)s', filename=log_filename, level=logging.ERROR)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    print "Beginning Project Hierarchy Lookup."

    errorfile_obj = open(access.get_errorfile_name(sys.argv[0]), 'w')

    if 'project_id' in kwargs.keys() and 'config_file' in kwargs.keys():
        projectid = kwargs['project_id']
        config_file = kwargs['config_file']
        logging.info("Beginning Project Hierarchy Lookup.")
        get_ancestry.project_ancestry_bq_update(projectid, errorfile_obj, config_file)
        # get_ancestry_by_projectid_folder_cols.project_ancestry_bq_update(projectid, errorfile_obj, config_file)

    elif 'config_file' in kwargs.keys():
        config_file = kwargs['config_file']
        parser = ConfigParser.SafeConfigParser()
        parser.read(config_file)
        infile = open(parser.get('property', 'projectid_file'))

        logging.info("Reading Input File")

        for line in infile:
            if line.strip():
                projectid = line.strip()
                logging.info("Beginning Project Hierarchy Lookup.")
                get_ancestry.project_ancestry_bq_update(projectid, errorfile_obj, config_file)
                # get_ancestry_by_projectid_folder_cols.project_ancestry_bq_update(projectid, errorfile_obj, config_file)

    else:
            print "Error: Please provide project id or project id file and config file."
            logging.error("Error: Please provide project id or project id file and config file.")
            exit(1)


    logging.info("End of Process. Please check error files for any errors.")
    print ("End of Process. Please check log and error files for errors.")


if __name__ == "__main__":
    main(**dict(arg.split('=') for arg in sys.argv[1:]))





