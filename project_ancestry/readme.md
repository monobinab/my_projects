Purpose:
========
Get ancestry of a GCP project by project id and it prints out the ancestry. It can also insert an entry into a BigQuery table if BigQuery table is created 
beforehand and the table information is passed in the config file and optionally project id can be separately supplied if the user wants to look up folder hierarchy for one project id.
 It takes one/two parameters, which is a config file. The config file should define key_file value
and few optional variables. 

Environment Set Up
==================
- Google Cloud SDK must be set up in the machine from where the script will be run.
- Google Cloud Service Account must be created and service account key file needs to be stored securely in the machine
as that will be used by the program for authorization. Please follow the steps in ./Set_Up_Python_Env.md
- Grant organizationViewer and folderViewer roles to the Service Account at the org level.
- Also few python packages need to be installed as mentioned. Please follow the steps in ./Set_Up_Python_Env.md


Main Scripts
============
Script name : gcp_org_hierarchy.py
Parameter : gcp_org_hierarchy.config
Optional parameter : project id (for single project id lookup)


Example Config file
===================
Please refer to gcp_org_hierarchy.config 


How to execute
===============
cd project_ancestry

python ./source/main.py config_file=./config/gcp_org_hierarchy.config

This has project id file location in the config file. Otherwise we can look up for one project:

python ./source/main.py config_file=./config/gcp_org_hierarchy.config project_id=cardinal-data-piper-sbx


Log files:
=========
The script also creates a log file for the entire process and an error file in the same directory as script 
if any update fails:

gcp_org_hierarchy.log
gcp_org_hierarchy.err

Troubleshooting:
===============
If the process run till the end, it prints out "End of Process. Please check log and error files for any errors." The 
user should check the error file for any projectid, whose hierarchy could not be found by the api call or BigQuery could 
not update it in the table.

Few common errors that you might face:
- Key file not found : Google Cloud Service Account key file is not downloaded or cannot be found in the location.
- The projects that Service Account don't have access to get, will not be returned and there will be error messages in 
.err file with <HttpError 403 when requesting ..... returned "The caller does not have permission">
