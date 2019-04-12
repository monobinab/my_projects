#!/usr/bin/python

import httplib2
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.client import GoogleCredentials
from google.cloud import bigquery
import os
import csv,io,json
import json
import logging

scope = 'https://www.googleapis.com/auth/cloud-platform'
# for JSON_CERTIFICATE_FILES
print 'Enter your Service Account Key File with Path'
key_file = raw_input()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/monobina/Documents/svcmsaha.json"

credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file)

#credentials = GoogleCredentials.get_application_default()
if credentials.create_scoped_required():
    credentials = credentials.create_scoped(scope)

http = httplib2.Http()
credentials.authorize(http)

crm = build('cloudresourcemanager', 'v1', http=http)


def resource_with_append_labels(instance, inputlabels):
    for label in inputlabels:
        key, value = label.split(":")
        if key and value:
            instance['labels'][key.strip()] = value.strip()
    return instance

def project_with_append_labels(project, inputlabels):
    for label in inputlabels:
        key, value = label.split(":")
        if key and value:
            project['labels'][key.strip()] = value.strip()
    return project

def bigquery_with_append_labels(dataset, inputlabels):
    for label in inputlabels:
        key, value = label.split(":")
        if key and value:
            dataset.labels[key.strip()] = value.strip()
    return dataset


if __name__ == "__main__":


    logging.basicConfig(format='%(asctime)s %(message)s',filename='update_resource.log',level=logging.DEBUG)

    print 'Enter type of Resource you want to Update: [ project | compute engine | bigquery ]'
    #accepted values: project
    resource_type = raw_input()
    if resource_type is not None and resource_type.strip().lower() in ('project', 'compute engine', 'bigquery'):
        resource_type = resource_type.strip().lower()
    else:
        logging.error("Invalid resource type")
        print('Invalid resource type')
        exit(1)

    print 'Enter Input File'
    input_file = raw_input()

    # Open the input label file if exits
    try:
        f = open(input_file, 'r')
    except IOError:
        print "Input File Not Found"
        exit(1)

    print 'Does it have header? Type: Y/N'
    contains_header = raw_input()
    if contains_header and contains_header.strip().upper().startswith("Y"):
        contains_header = "Y"

    line_index = 0
    # creating error file name
    basename = os.path.basename(input_file)
    error_file_basename = os.path.splitext(basename)[0]
    error_file = open(error_file_basename+'_err.log', 'w')

    #  start reading input file line by line and get resource name and add to the labels dictionary
    for line in f:
        # Skip first line
        if contains_header == "Y" and line_index == 0:
            line_index = line_index + 1
            continue

        if resource_type == 'project':
            logging.info ("Updating Project Labels")
            # if project id and label both are not given then the record cannot be processed
            if line:
                try:
                    strtokens = line.split(",")
                    projectid = strtokens[0]
                    projectlabels = strtokens[1].split("|")
                except Exception as inst:
                    logging.error(inst)
                    error_file.write(line + '|' + str(inst))
                    continue;
                logging.info(projectid)
                logging.info(projectlabels)

                logging.info("Getting Project Detail")

                try:
                        project = crm.projects().get(projectId=projectid).execute()
                        project = project_with_append_labels(project, projectlabels)
                        project = crm.projects().update(
                            projectId=projectid, body=project).execute()
                except Exception as inst:
                        logging.warning(inst)
                        error_file.write(line + '| This project is not found: ' + projectid + "\n")
                        exit(1)



        elif resource_type == 'compute engine':
            logging.info ("Updating Instances Labels")
            if line:
                try:

                    strtokens = line.split(",")
                    projectid = strtokens[0]
                    instanceid = strtokens[1]
                    zonename = strtokens[2]
                    resourcelabels = strtokens[3].split("|")

                except Exception as inst:
                    logging.error(inst)
                    error_file.write(line+'|'+str(inst))
                    continue;

                logging.info(projectid)
                logging.info(instanceid)

                try:

                    crm_compute = build('compute', 'v1', http=http)
                    instance = crm_compute.instances().get(project=projectid, instance=instanceid, zone=zonename).execute()
                except Exception as inst:
                    logging.warning(inst)
                    error_file.write(line+'|'+str(inst))
                    exit(1)

                instance = resource_with_append_labels(instance, resourcelabels)
                #instance.update()
                try:
                    instance = crm_compute.instances().setLabels(
                        project = projectid, instance=instanceid, body=instance, zone = zonename).execute()
                except Exception as inst:
                    logging.error(inst)
                    exit(1)

        elif resource_type == 'bigquery':
            logging.info ('Updating BigQuery Labels')
            if line:
                try:
                    strtokens = line.split(",")
                    projectid = strtokens[0].strip()
                    datasetid = strtokens[1].strip()
                    datasetid = projectid+'.'+datasetid
                    bigquerylabels = strtokens[2].strip().split("|")
                except Exception as inst:
                    logging.error(inst)
                    error_file.write(line + '|' + str(inst))

            logging.info(bigquerylabels)

            client = bigquery.Client.from_service_account_json(key_file)
            try:
                datasetref = bigquery.dataset.DatasetReference.from_string(datasetid)
                dataset = client.get_dataset(datasetref)
                dataset = bigquery_with_append_labels(dataset,bigquerylabels)
                dataset = client.update_dataset(dataset, ['labels'])
            except Exception as inst:
                logging.warning(inst)
                error_file.write(line + '|' + str(inst))







