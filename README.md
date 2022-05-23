# About Project
In the first part of this project, we are designing an ETL pipeline.
Where, we first EXTRACT the batch data locally or from S3 bucket.
Second, we TRANSFORMED the data. 
And third, LOAD the data to postgres sql database. 

The goal of the first part is to design an ETL pipeline, which can be used for 
any type (or most) of the data. And, we want to load these data in the postgres sql database tables. 
In order to load these different data we might need to perform some data transformation 
such as convert data to datetime datatype.

In the second part, we will create a star schema dimensional model. 



The flow of this project is described below,

### Extract

- The raw data is available in the s3 bucket or reaches into the S3 bucket.
- The S3 have archived folder, where the raw data is archived.
- The data will move to the archived folder if the data is processed, loaded to sql db, 
and dimensional model executed successfully.

### Transform
- First, the raw data is read in pandas dataframe.
- Second, the data is transformed in multiple steps.
- Third, the transformed data is loaded into postgres sql database.
- The above three steps are performed for all type of data. 
- Different transformation functions have been added or can be added in the transform.py.
These functions can be used as per the requirements of different data.

### Load
- The raw data is loaded into the postgres sql db.
- The schema is defined in the sql scrips and the sql scripts can be saved in the tables folder.

### Dimensional Modelling
- Based on the available data in the table reviews and metadata, the dimensional model is created.
- The dimensional model is executed right after the above ETL process is performed successfully.
- Airflow trigger the process 3 times a day. Airflow pick the docker image from docker container.



#Important to know
- The working directory is Bestseller
- Run Build.sh to download required packages


# Docker-compose build command for bestseller package
The below command will build the docker image of the bestseller package and postgres sql in docker. 
Make sure your current working directory is Bestseller and run it in terminal.

`docker-compose build`

# Run Docker container
The below command will run the docker container which consist bestseller package and postgres sql.

`docker-compose up`



# Airflow

Below steps will help you set the Airflow UI.
The DAG called `process_bestseller_transactions`. The DAG will run at 02:00, 10:00 and 18:00 everyday.

## Initialise the Airflow Database
Change the current working directory to Airflow_docker.
`cd Airflow_docker`

Initialise the Airflow Database by first starting the airflow-init container:

`docker-compose up airflow-init`


## Start Airflow services
Get Airflow services up and running

`docker-compose up`

## Access Airflow UI
In order to access Airflow User Interface simply head to your preferred browser and open localhost:8080.
`airflow` is username and password.

# Update the config.yaml file in this repository.
- Navigate to config_file/config.yaml and open this file in your favorite editor
- Under the key input/data create a new key for your new data source. See example of review_data and meta_data.
- Under the key of our new source, we can add the key value pairs that are necessary
to process this data correctly. This is the complete list of keys that are currently
supported.

| key | value / key | value (optional) |  Default  | Required |
|----------|------------|------------|:----------:|:--------:|
| `bucket_name`| Name of your S3 bucket  | |    -       |    yes   |
| `bucket_source_folder`| Name of folder where s3 files will be end up  | |    sources      |    yes   |
| `bucket_archive_folder`| Name of folder archived s3 folder | |    archive_data      |    yes   |
| | `path`| sources/file initial name*  | |    -       |    yes   |
| | `mode` | `append`|  |  `append` |    yes  |
| | `database_schema`|  The name of the target schema in database | |   `public`       |    yes   |
| | `table_name`| The name of the target table | |   -       |    yes   |
| | `create_table_query_path`| path to query with CREATE TABLE statement | |    -       |    yes/no |
| | `columns_to_transform_date`| list of columns| |    -       |    no |
| | `columns_to_transform_date`| list of columns| |    -       |    no |
| | `columns_to_json`| list of columns| |    -       |    no |
| | `columns_to_flat`| list of columns| |    -       |    no |

