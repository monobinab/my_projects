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
credentials = GoogleCredentials.get_application_default()
if credentials.create_scoped_required():
    credentials = credentials.create_scoped(scope)

http = httplib2.Http()
credentials.authorize(http)

crm = build('cloudresourcemanager', 'v1', http=http)



#projects = crm.projects().list().execute()


def instance_with_append_labels(instance, inputlabels):
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

def bigquery_with_append_labels(bigquery, inputlabels):
    for label in inputlabels:
        key, value = label.split(":")
        if key and value:
            bigquery['labels'][key.strip()] = value.strip()
    return bigquery

if __name__=="__main__":

    logging.basicConfig(format='%(asctime)s %(message)s',filename='update_resource.log',level=logging.DEBUG)

    print 'Enter type of Resource you want to Update: [ project | compute engine | big query ]'
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
        exit()

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
                    projectid = strtokens[0].strip()
                    projectlabels = strtokens[1].split("|").strip()
                except Exception as inst:
                    logging.error(inst)
                    error_file.write(line+'|' + str(inst))
                    continue

                logging.info (projectid)
                logging.info (projectlabels)

                logging.info ("Getting Project Detail")

                try:
                    project = crm.projects().get(projectId=projectid).execute()
                except:
                    logging.warning('Project Not Found')
                    error_file.write(line + '| This project is not found: ' + projectid + "\n")
                    continue

                project = project_with_append_labels(project, projectlabels)
                # if there is any authentication error or somehow this update didn't happen; then it will exit out after recording the error
                try:

                    project = crm.projects().update(
                        projectId=projectid, body=project).execute()
                except Exception as inst:
                    logging.error(inst)
                    exit(1)

        elif resource_type == 'compute engine':
            logging.info ("Updating Instances Labels")
            strtokens = line.split(",")
            projectid = strtokens[0]
            instanceid = strtokens[1]
            zonename = strtokens[2]
            resourcelabels = strtokens[3].split("|")
            logging.debug ("Resource labels is: ", resourcelabels)

            crm_compute = build('compute', 'v1', http=http)
            instance = crm_compute.instances().get(project=projectid, instance=instanceid, zone=zonename).execute()
            print(json.dumps(instance, indent=4, sort_keys=True))

            instance = instance_with_append_labels(instance, resourcelabels)
            #instance.update()

            instance = crm_compute.instances().setLabels(
                project = projectid, instance=instanceid, body=instance, zone = zonename).execute()

        elif resource_type == 'bigquery':
            print 'Updating BigQuery'
            strtokens = line.split(",")
            projectid = strtokens[0]
            datasetid = strtokens[1]
            bigquerylabels = strtokens[2]
            datasetref = bigquery.dataset.DatasetReference.from_string(datasetid)
            dataset_list = bigquery.Client(project=projectid).get_dataset(datasetref)
            print dataset_list


            bigquery = bigquery_with_append_labels(bigquery, bigquerylabels)
            client = bigquery.Client.update_dataset(projectid,dataset,bigquery)
                #(credentials=credentials, project=projectid)
            #crm = build('bigquery', 'v1', http=http)


            bigquery = crm.bigQuery().get(project=projectid, dataset=dataset).execute()
            print (bigquery)

            bigquery = crm.update_dataset(dataset,bigquery_labels(bigquery,bigquerylabels))
            assert dataset.labels == labels





