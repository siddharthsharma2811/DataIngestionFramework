CREATE IF NOT EXISTS DATABASE datalakepoc_metadata;
CREATE TABLE datalakepoc_metadata.dataset_metadata (                                       
  dataset STRING,                                                                         
  table_name STRING,                                                                      
  datafile_location STRING COMMENT 'Location where data files are present',               
  data_year INT COMMENT 'Year for which the data is available',                           
  stage_db STRING COMMENT 'Database which contains external tables',                      
  final_db STRING COMMENT 'Database which contains the curated data',
  sas_dataset STRING  
)                                                                                         
 COMMENT 'Contains tables details present with in a given dataset '                       
STORED AS TEXTFILE;

CREATE TABLE datalakepoc_metadata.table_definition (                                       

tableid INT,                                                                            

table_name STRING,                                                                      

table_description STRING,                                                               

table_dataset STRING,                                                                   

table_delimiter STRING,                                                                 

table_data_file_format STRING,                                                          

header_count INT, 

footer_count INT,                                                                      

load_type STRING,

raw_table_name STRING                                                                        

)                                                                                         

 COMMENT 'Contains Table level details for data ingestion'                                

STORED AS TEXTFILE;                                                           ;

CREATE TABLE datalakepoc_metadata.column_definition (      
  tableid INT,                                            
  column_id INT,                                          
  column_name STRING,                                     
  column_description STRING,                              
  column_datatype STRING,
  column_position STRING,
  date_format STRING,    
  sas_column_name STRING
)                                                         
 COMMENT 'Contains column metadata for the Data Ingestion'
STORED AS TEXTFILE;                                        


 CREATE TABLE datalakepoc_metadata.analyzer_to_explorer (                    
  record_id STRING,                                                         
  dataset_name STRING,                                                      
  security_classification STRING,                                           
  date_created STRING,                                                      
  region STRING,                                                            
  dataset_type STRING,                                                      
  country STRING,                                                           
  request_id STRING,                                                        
  update_date STRING                                                        
)                                                                           
 COMMENT 'This table is used to store the metadata recevied from explorer ' 
STORED AS TEXTFILE;

 CREATE TABLE datalakepoc_metadata.dt2analyzer ( 
   unique_id BIGINT,                            
   dataset_id STRING,                           
   destination_path STRING,                     
   processing_priority STRING,                  
   table_name STRING,                           
   delimiter STRING                             
 )                                              
 STORED AS TEXTFILE;


CREATE TABLE datalakepoc_metadata.dt2analyzer_status (
   refid BIGINT,                                     
   status STRING,
   rec_insert_date TIMESTAMP,
   dataset_id STRING,
   table_name STRING
 )                                                   
 STORED AS TEXTFILE;

 CREATE TABLE datalakepoc_metadata.dataset_definition_dynamic (                          
  sourceid STRING COMMENT 'unique ID for a dataset, FK to table dataset_definition_static',
  delivery_ts TIMESTAMP COMMENT 'time of delivery for dataset',                         
  dqc_ts TIMESTAMP COMMENT 'data quality check timestamp',                              
  dq_row_cnt BIGINT COMMENT 'row count for dq checks',                                  
  ingestion_ts TIMESTAMP COMMENT 'timestamp of completion ingestion',                   
  ingestion_row_cnt BIGINT COMMENT 'count of rows inserted as part of ingestion',       
  patient_cnt BIGINT COMMENT 'count of patients inserted as part of ingestion'          
)                                                                                       
 COMMENT 'Contains dataset definition dynamic metadata for the Data Ingestion'          
STORED AS TEXTFILE ;

CREATE TABLE datalakepoc_metadata.dataset_definition_static (                                         
  dataset STRING COMMENT 'name of dataset',                                                          
  sourceid STRING COMMENT 'unique ID for a dataset, FK to table dataset_definition_dynamic',            
  source_name STRING COMMENT 'source name for the dataset',                                          
  landing_path STRING COMMENT 'location of dataset in raw s3 bucket',                                
  update_frequency STRING COMMENT 'weekly/monthly/quarterly',                                        
  refresh_type STRING COMMENT 'Full/Incremental',                                                    
  security_classification STRING COMMENT 'Public/Confidential',                                      
  region STRING COMMENT 'region of input data for the dataset',                                      
  country STRING COMMENT 'country of input data for the dataset',                                    
  data_dictionary_path STRING COMMENT 'path of input data dictionary',                               
  data_transformation_rules STRING COMMENT 'transformation rules for the dataset',                   
  exploration STRING,                                                                                
  patient_info_table STRING COMMENT 'name of table having patient information',                      
  patient_field STRING COMMENT 'name of field in patient_info_table can be used to get patient count'
)                                                                                                    
 COMMENT 'Contains dataset definition static metadata for the Data Ingestion'                        
STORED AS TEXTFILE;

CREATE VIEW datalakepoc_metadata.vw_dataset_definition AS 
SELECT dataset, a.sourceid, source_name, landing_path, update_frequency, refresh_type, security_classification, region, country, data_dictionary_path, data_transformation_rules, exploration, delivery_ts, dqc_ts, dq_row_cnt, ingestion_ts, ingestion_row_cnt, Patient_cnt FROM datalakepoc_metadata.dataset_definition_static a INNER JOIN datalakepoc_metadata.dataset_definition_dynamic b ON a.sourceid = b.sourceid INNER JOIN (SELECT sourceid, max(ingestion_ts) ingestion_tsp FROM datalakepoc_metadata.dataset_definition_dynamic GROUP BY sourceid) c ON c.sourceid = b.sourceid AND c.ingestion_tsp = b.ingestion_ts ;

CREATE TABLE datalakepoc_metadata.audit_job_run_log (         
   aud_job_id BIGINT,                                     
   aud_data_set STRING,                                   
   aud_table STRING,                                      
   aud_data_suorce_file_location STRING,                  
   aud_target_database STRING,                            
   aud_data_year INT,                                     
   aud_table_record_count BIGINT,                         
   aud_table_column_count BIGINT,                         
   aud_job_run_status STRING,                             
   aud_job_start_timestamp STRING,                        
   aud_job_end_timestamp STRING,                          
   aud_job_log_path STRING                                
 )                                                        
  COMMENT 'Contains AUDIT information for Data ingestion '
 STORED AS TEXTFILE;
 
 