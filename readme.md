# Date Warehousing Pipeline of SCD Type 01 Logic 

## Client Requirement 

* Daily a file will be coming from the client side of customer purchase data

* The will be new records every day and there might also be old records that need to be updated

* Client requires SCD TYPE 01 logic to be in  the warehouse 

* Also the records end the processing need to be reconciled 

## Data ingestion 

* Data is loaded into MYSQL DBMS using command prompt loading

* The data after some pre-processing then ingested to HDFS using sqoop 

## Date Summarisation and Warehousing 

* Hive is used to manage the Warehousing part

* Implemented SCD TYPE 02 logic

* Implemented Partitioning on Year & Month for fast retrieval

## Validation

* Once the pipeline is completed the data is at last checked with input records for the count

* After every successful operation or failure the log is generated and can be seen for the report and analysis

## Script

* Entire warehousing solution is automated using bash scripts

* All credentails,output direotries, DBMS details are made dynamic using parmeter file and crednetial files

## Execution

#### Project Work Flow
![Alt text](https://github.com/SubhashMurugesan/Hadoop_Warehosue_Data_Pipeline/blob/main/Project_workflow.jpeg?raw=true "Project Flow Daigram")

#### Datasets Folder  [here](https://github.com/SubhashMurugesan/Hadoop_Warehosue_Data_Pipeline/tree/main/datasets)

* The place where daily data comes in and there are some sub folders which is used for testing while coding dev

#### Scripts [here](https://github.com/SubhashMurugesan/Hadoop_Warehosue_Data_Pipeline/tree/main/scripts)

* Entire automation is done here, you can find the entire logic here and intermediate files generated

#### Env [here](https://github.com/SubhashMurugesan/Hadoop_Warehosue_Data_Pipeline/tree/main/env)

* Support files required for the Script file are kept under this directory

#### Reference File [here](https://github.com/SubhashMurugesan/Hadoop_Warehosue_Data_Pipeline/tree/main/ref_files)
* Under this direcotry you can find the details regarding the column description at schema level 
