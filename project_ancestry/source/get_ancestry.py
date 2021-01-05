#!/usr/bin/python
import access, bigquery_load, folder_look_up
import json
import get_org_nm
import logging
import ConfigParser
import pprint


def get_ancestry(projectid, errorfile_obj, config_file):
    """
    THis function uses a http get call to retrieve project hierarchy by project id.
    It then look up the resource names by individual resource ids of ancestors of a project.
    It returns folder names by folder levels and organization name.

    :param projectid: Projectid for which folders will be looked up.
    :param errorfile_obj: error file object where errors will be entered.
    :param config_file: Config file from where input parameters are passed.
    :return: It doesn't return anything.
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    parser = ConfigParser.SafeConfigParser()
    parser.read(config_file)
    key_file = parser.get('property', 'key_file')

    service1 = access.service1(config_file)

    logging.info("project id is :" + projectid)

    # make http call to get ancestry of a project
    request_project = service1.projects().getAncestry(projectId=projectid)

    # the ancestor resources get added in resource dict to form a dictionary containing project,
    # folders and organization
    folder_list = list()  # there can be multiple folders as parent of a project, so itself saved in a separate dict
    folder_id_list = list()
    resource_dict = dict()
    folder_dict_list = list()
    folder_path = ""  # as the parent folders will be searched they will get added in the folder path
    folder_id_path = ""
    org_id = ""
    org_display_name = ""

    try:
        response_project = request_project.execute()
        logging.info("Got ancestry info for projectid " + projectid)

        for key, ancestor_list in response_project.iteritems():  #looping through the http response dictionary

            for resource_obj in ancestor_list:
                for k,v in resource_obj.items():
                    for ancestor_type, ancestor_id in v.items():
                        if ancestor_type == "type":
                            resource_type = ancestor_id
                        if ancestor_type == "id":
                            resource_id = ancestor_id

                            if resource_type == "organization":  # look up organization name
                                logging.info ("Getting organization Info")
                                org_id = resource_id
                                org_display_name = get_org_nm.get_org_nm(resource_id, config_file)
                                resource_dict[resource_type] = org_display_name

                            elif resource_type == "project":  # project name is not looked up as project id will
                                # be used in output
                                projectid = resource_id
                                resource_dict[resource_type] = projectid

                            elif resource_type == "folder":  # look up folder names
                                logging.info("Getting Folder Info")
                                folder_id_list.append(resource_id)
                                folder_id_path = "/" + resource_id + folder_id_path
                                folder_display_name = folder_look_up.folder_lkp(resource_id, config_file)
                                folder_list.append(folder_display_name)

            folder_list.reverse()  # to have folder directly under organization as folder_level_1

            i = 1
            for item in folder_list:
                folder_hierarchy_dict = dict()
                folder_hierarchy_dict["folder_name"] = item
                folder_hierarchy_dict["folder_level"] = i
                folder_dict_list.append(folder_hierarchy_dict)
                i += 1

        resource_dict["folder_hierarchy"] = folder_dict_list
        folder_id_path = "/" + org_id + folder_id_path

        print "Project ancestry for ", projectid, "is:"
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(json.dumps(resource_dict))
        #folder_id_list.reverse()
        #pp.pprint(folder_id_list)
    except Exception as inst:  # errors are logged in .log file and in .err file if any service returned error
        logging.error(inst)
        logging.info("This Account doesn't have permission to get ancestry for projectid: " + projectid)
        logger.error(inst)
        errorfile_obj.write("ProjectId" + ":" + projectid + "|" + "Error Message" + ":" + str(inst) + "\n")
        print("ProjectId" + ":" + projectid + "|" + "Error Message" + ":" + str(inst) + "\n")
        raise inst

    return folder_dict_list, org_display_name


def project_ancestry_bq_update(projectid, errorfile_obj, config_file):
    # updating project hierarchy information to BigQuery table and errors are logged
    try:
        parser = ConfigParser.SafeConfigParser()
        parser.read(config_file)

        dataset_id = parser.get('property', 'dataset_id')
        bq_project = parser.get('property', 'bq_project')  # these info are supplied from config file
        table_id = parser.get('property', 'table_id')
        key_file = parser.get('property', 'key_file')

        folder_dict_list, org_display_name = get_ancestry(projectid, errorfile_obj, config_file)

        # delete the project id entry from the table
        bigquery_load.bq_check_delete(key_file, bq_project, dataset_id, table_id, projectid)

        bigquery_load.bq_insert(key_file, bq_project, dataset_id, table_id, projectid, org_display_name, folder_dict_list)

        logging.info("ProjectId: " + projectid + "|" + "Project Updated in BigQuery")
        print("ProjectId: " + projectid + "|" + "Project Updated in BigQuery")

    except Exception as inst:
        #logging.error(inst)
        errorfile_obj.write("ProjectId" + ":" + projectid + "|" + "Error Message" + ":" + str(inst) + "\n")
        print ("Big Query Information not provided. So project not updated in BigQuery.")
        logging.error("Big Query Information not provided. So project not updated in BigQuery.")
        #pass












