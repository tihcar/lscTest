# Databricks notebook source
# MAGIC %md
# MAGIC #### All connected notebooks are hyperlinked below. 
# MAGIC <a href="$../Common/utilities">Utilities: </a> Notebook containing multi-use functions <br /> 
# MAGIC <a href="$../Business/Processing/maildat_extraction">Extraction: </a> Notebook containing the code for handing file movements and extractions<br />
# MAGIC <a href="$../Business/Validations/raw_validations">Validations: </a> Notebook containing code with all functions used for performing validations<br />
# MAGIC <a href="$../Business/Processing/raw_processing">Processing: </a> Notebook containing code with all functions used forprocessing and transforming files <br />
# MAGIC <a href="$../Business/Orchestration/raw_orchestration">Orchestration: </a> Notebook containing code with all SQL queries <br />
# MAGIC <a href="$../Common/config">Config: </a> Notebook containing connection configurations <br />

# COMMAND ----------

# MAGIC %run /Shared/Common/config

# COMMAND ----------

# MAGIC %run /Shared/Business/Processing/maildat_extraction

# COMMAND ----------

# MAGIC %run /Shared/Business/Validations/raw_validations

# COMMAND ----------

# MAGIC %run /Shared/Business/Orchestration/raw_orchestration

# COMMAND ----------

# MAGIC %run /Shared/Common/utilities

# COMMAND ----------

# MAGIC %run /Shared/Business/Processing/raw_processing

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

#Initial Checks
processing_status = Md_Queries().processing_row_count()

#If files not being processed in DB but exist in processing folder
if processing_status ==0:
  print("No files under processing in DB")
  if len(dbutils.fs.ls(processing_folder_path)) != 0:
    print("Files still in processing folder")
    return

#If files being processed in db
if processing_status != 0:
  print("Process triggered but but ETL table has rows in processing state in ELT table")
  return

#Check input folder if it's empty - if no, start processing. if empty, halt
if len(dbutils.fs.ls(input_folder_path)) == 0:
  print("Process Triggered but no file in input folder so exiting")
  return

# COMMAND ----------

files_list = [WIND21384au40001PlanFileRequestPreliminaryPostageStatement.zip,
WIND21385au40001PlanFileRequestPreliminaryPostageStatement.zip,
WIND21386au40001PlanFileRequestPreliminaryPostageStatement.zip,
WIND21387au40001PlanFileRequestPreliminaryPostageStatement.zip,
WIND21388au40001PlanFileRequestPreliminaryPostageStatement.zip,
WIND21389au40001PlanFileRequestPreliminaryPostageStatement.zip,
WIND21390au40001PlanFileRequestPreliminaryPostageStatement.zip,
WIND21391au40001PlanFileRequestPreliminaryPostageStatement.zip,
WIND21392au40001PlanFileRequestPreliminaryPostageStatement.zip,
WIND21393au40001PlanFileRequestPreliminaryPostageStatement.zip,
WIND21394au40001PlanFileRequestPreliminaryPostageStatement.zip,
WIND21395au40001PlanFileRequestPreliminaryPostageStatement.zip,
LANW21396au40003PlanFileRequestPreliminaryPostageStatement.zip,
LANW21397au40003PlanFileRequestPreliminaryPostageStatement.zip,
LANW21398au40003PlanFileRequestPreliminaryPostageStatement.zip,
LANW21399au40003PlanFileRequestPreliminaryPostageStatement.zip,
LANW21400au40003PlanFileRequestPreliminaryPostageStatement.zip]

# COMMAND ----------

#Extracting files from the source
try:
  df_files = spark.createDataFrame(dbutils.fs.ls(input_folder_path))
#   df_files = spark.createDataFrame(files_list)
except Exception as e:
  print("Exception {} was encountered at starting of the process while extracting files".format(e))

#Creating RDD
listing = sc.parallelize(df_files)

# COMMAND ----------

#Creating a main function
def main_maildat_raw(full_file_name):
  name_split = os.path.splitext(full_file_name)
  file_name = name_split[0]
  file_extension = str(name_split[1][1:]).lower()
  
  # if file is a zip, move to processing folder else move to error and log the information.
  if file_extension == "zip":
      
      # Move from input to processing folder
      try:
          shutil.move(os.path.join(input_folder_path, full_file_name), os.path.join(processing_folder_path, full_file_name))
      except Exception as e:
          print("Exception {} encountered while transferring file {} to processing folder ".format(e,file_name))
      
      # Create temp folder to extract files, start staging process for the file in temp folder
      try:
          #MAIN CODE#
          # 1. extract the zip file and create the temp folder with extracted files.
          try:
              temp_processing_folder_path = ExtractFiles().extract_zip_file(folder_path=processing_folder_path,file_name=file_name)
              zip_length = ExtractFiles().zip_length(folder_path=temp_processing_folder_path)
          except Exception as e:
              print("Exception {} encountered while creating temp folder for file {}, moving to error folder".format(e, file_name))
              Movefiles().From_To_Folder(processing_folder_path, error_folder_path)

          # 2.Get job details from header file
          try:
              version_number, job_number, job_year, job_pool_week, user_license_code = ExtractFiles().md_hdr_detail(folder_path=temp_processing_folder_path)
          except Exception as e:
              print("Exception {} was encountered while checking details for file {}".format(e, str(file_name)))
              Movefiles().From_To_Folder(processing_folder_path, error_folder_path)

          # 3. Business Validations
          try:
              if (Md_HdrValidations().file_process_status(file_name=file_name) is True) and \
                      (Md_HdrValidations().user_license_code(user_license_code=user_license_code) is True) and \
                      (Md_HdrValidations().job_pool_week(pool_week=job_pool_week) is True) and \
                      (Md_HdrValidations().job_number(job_number) is True) and \
                      (Md_HdrValidations().job_year(job_year) is True):
                  business_validation_status = True
              else:
                  business_validation_status = False
                  print("file {} failed business validation, move files to error folder".format(file_name))
                  Movefiles().From_To_Folder(processing_folder_path, error_folder_path)
          except Exception as e:
              print("Exception {} was encountered while processing business validation for file {}".format(e, file_name))
              Movefiles().From_To_Folder(processing_folder_path, error_folder_path)

          #4. Getting versionID
          try:
              maildatversion_id = Md_Queries().get_version_id(version_number=version_number)
              if maildatversion_id == 0:
                  print("MailDat Version Mapping not found for Mapping {} for file {} ".format(str(version_number),file_name))
                  Movefiles().From_To_Folder(processing_folder_path, error_folder_path)
          except Exception as e:
              print("Exception {} was encountered while retrieving MailDat Version id for Mapping {} for file {}".format(e,str(version_number),file_name))
              Movefiles().From_To_Folder(processing_folder_path, error_folder_path)

          # 5. Getting month number
          month = 0
          try:
              month = Md_HdrValidations().pool_month(job_pool_week)
          except Exception as e:
              print("Exception {} encountered while getting the month detail for file {}".format(e, file_name))

          #6. Creating first Maildat entry in info table
          try:
              Md_Queries().create_maildatinfo_entry(job_number=job_number, job_year=job_year, job_pool_week=job_pool_week,
                                                    file_name=file_name,job_month=month, user_license_code=user_license_code, zip_count=zip_length)
          except Exception as e:
              print("Exception {} encountered while inserting file detail into Maildatinfo Table for file {}".format(e,file_name))

          #7. Getting Maildat Info ID
          try:
              maildatinfoid = Md_Queries().get_maildat_info_id(file_name=file_name)
              if maildatinfoid == 0:
                  print("Maildatinfoid is incorrect for the {}, maildatinfoid was found to be = {}".format(file_name, maildatinfoid))
          except Exception as e:
              print("Exception {} occurred while retrieving maildatinfo id for the file {}".format(e, file_name))
              Movefiles().From_To_Folder(processing_folder_path, error_folder_path)

          #8. # start processing child files sequentially
          try:
              file_processing_state = Md_Multiprocessor().process_child_files_sequentially(
                  temp_processing_folder_path=temp_processing_folder_path,
                  file_name=file_name,
                  maildatversion_id=maildatversion_id,
                  maildatinfoid=maildatinfoid,
                  month=month)
          except Exception as e:
              print("Exception {} encountered while processing file {}".format(e, file_name))
              file_processing_state = False
              Md_RaiseError().raise_error(file_name)

          #9.
          #In case of any failure of any child file the whole parent file data will be discarded
          ## Create folder with timestamp and move within it. 
          try:
              if file_processing_state is True:
                  Md_Queries().update_entry_status(maildatinfoid=maildatinfoid, status="SUCCESS")
                  print("{} processed successfully".format(file_name))
                  Movefiles().From_To_Folder(processing_folder_path, archive_folder_path)
              else:
                  print("{} failed processing".format(file_name))
                  Md_RaiseError().raise_error(file_name)
          except Exception as e:
              print("exception {} encountered while updating the final status for file {} in etl table".format(e,file_name))
              Movefiles().From_To_Folder(processing_folder_path, error_folder_path)

      except Exception as e:
          print("exception {} encountered while processing file {} ".format(e, file_name))
  else:
      print("Error: FileNameDetail extension is not zip for file = {}".format(str(file_name)))
      try:
          shutil.move(os.path.join(input_folder_path, full_file_name), os.path.join(error_folder_path, full_file_name))
      except Exception as e:
          print("FileNameDetail extension was not zip, exception {} occurred while moving the file {} "
                "to processing folder".format(e, file_name))

# COMMAND ----------

#Running the main code in parallel
listing.foreach(lambda main: main_maildat_raw(df_files))