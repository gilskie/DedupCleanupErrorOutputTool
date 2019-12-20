import pyodbc
import configparser
import sys
import time
import os
import mmap
import datetime
import shutil


def load_configuration_file():
    config = configparser.ConfigParser()
    json_configuration = {}

    # for sandbox configuration
    complete_executable_path = sys.path[0] + '\configurationFile.ini'

    # for live configuration
    # complete_executable_path = sys.executable.replace("DedupCleanupErrorOutputTool.exe", "configurationFile.ini")

    config.read(complete_executable_path)

    # print(f"executable path {complete_executable_path}")
    default_settings = config['DEFAULT']
    json_configuration['erroneous_directory'] = default_settings['ErroneousDirectory']
    json_configuration['status_flag'] = default_settings['StatusFlag']

    num_of_files_flag = config['NUMOFFILESFLAG']
    json_configuration['check_no_of_files'] = num_of_files_flag['CheckNoOfFiles']
    json_configuration['number_of_files_count'] = num_of_files_flag['NoOfFilesCount']
    json_configuration['wms_process_id_to_reset_only'] = num_of_files_flag['WMSProcessIDToResetOnly']

    num_of_files_flag = config['SQLCONNECTIONFORRESET']
    json_configuration['server_name'] = num_of_files_flag['ServerName']
    json_configuration['database_name'] = num_of_files_flag['DatabaseName']
    json_configuration['database_user_id'] = num_of_files_flag['UserID']
    json_configuration['database_password'] = num_of_files_flag['DatabasePassword']

    # return erroneous_directory, status_flag, check_no_of_files, number_of_files_count
    return json_configuration


def generate_jobs_for_delete(erroneous_directory, status_flag, check_no_of_files, number_of_files_count):
    # print(f"{erroneous_directory}, {status_flag}, {check_no_of_files}, {number_of_files_count}")
    folder_list_files = os.listdir(erroneous_directory)
    flagged_folder = []

    for job_name in folder_list_files:
        # print(f"sts directory:{os.path.join(erroneous_directory, job_name, job_name + '.sts')}")
        # breakpoint()

        with open(os.path.join(erroneous_directory, job_name, job_name + '.sts'), 'rb', 0) as file, \
                mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as s:
            if s.find(b'Ongoing') != -1:
                flagged_folder.append(job_name)

    # print(f"complete jobs with ongoing status: {flagged_folder}")
    return flagged_folder


def check_date_duration(jobs, erroneous_directory, server, db_name, db_user, db_pass, wms_process_id_to_reset_only):
    # print(f"jobs for checking: {jobs}")
    # get modified date
    for job in jobs:
        sts_modified_date_time_value = time.ctime(os.path.getmtime(os.path.join(erroneous_directory, job, job + '.sts')))

        print(f"job: {job}, last modified: {sts_modified_date_time_value}")
        sts_date_time_object = datetime.datetime.strptime(sts_modified_date_time_value, '%a %b %d %H:%M:%S %Y')
        get_date_now = datetime.date.today()
        get_date_file_modified = sts_date_time_object.date()
        delta = get_date_now - get_date_file_modified
        # print(f"Date: {get_date_file_modified}, date now: {get_date_now}, days difference: {delta.days}")
        if delta.days > 1:
            print(f"valid for deletion! deleting root folder...")
            delete_directory_folder(job, erroneous_directory, server, db_name, db_user, db_pass, wms_process_id_to_reset_only)


def delete_directory_folder(job, erroneous_directory, server, db_name, db_user, db_pass, wms_process_id_to_reset_only):
    directory_for_delete = os.path.join(erroneous_directory, job)
    if os.path.exists(directory_for_delete) and os.path.isdir(directory_for_delete):
        shutil.rmtree(directory_for_delete)
        print(f"Successfully deleted {directory_for_delete}")

    # try to reset if status is not new on database!
    reset_database_status_if_not_new(job, server, db_name, db_user, db_pass, wms_process_id_to_reset_only)


def reset_database_status_if_not_new(job, server, db_name, db_user, db_pass, wms_process_id_to_reset_only):
    # note: application will only reset jobs found in [wms_JobsBatchInfo] into new if it is only ongoing
    try:
        sql_statement = "SELECT JobId, processid, StatusId, batchname, DateUpdated " \
                        "FROM [WMS_CHVDP].[dbo].[wms_JobsBatchInfo] " \
                        "WHERE processid = "+wms_process_id_to_reset_only+" " \
                        "and statusid = 3 " \
                        "and batchname in('"+job+"')"

        conn = pyodbc.connect('DRIVER={SQL Server};'
                              'SERVER='+server+
                              ';DATABASE='+db_name+
                              ';UID='+db_user+
                              ';PWD='+db_pass+
                              ';Trusted_Connection=No')

        cursor = conn.cursor()
        cursor.execute(sql_statement)

        for row in cursor:
            sql_statement_reset_to_new = "UPDATE [dbo].[wms_JobsBatchInfo] " \
                                         "SET [StatusId] = 1 " \
                                         "WHERE batchname ='"+job+\
                                         "' and processid = "+wms_process_id_to_reset_only+""

            # breakpoint()
            cursor.execute(sql_statement_reset_to_new)
            cursor.commit()
            print(f"Successfully reset to new for {job}.")
            # breakpoint()

    except Exception as e:
        print(f"Error: {e}")


def main():
    json_configuration_file = load_configuration_file()
    job_lists = generate_jobs_for_delete(json_configuration_file['erroneous_directory'],
                                         json_configuration_file['status_flag'],
                                         json_configuration_file['check_no_of_files'],
                                         json_configuration_file['number_of_files_count'])

    print(f"jobs with ongoing status: {job_lists}"
          f"\r\nverifying if more than 1 day")

    check_date_duration(job_lists,
                        json_configuration_file['erroneous_directory'],
                        json_configuration_file['server_name'],
                        json_configuration_file['database_name'],
                        json_configuration_file['database_user_id'],
                        json_configuration_file['database_password'],
                        json_configuration_file['wms_process_id_to_reset_only'])


main()
