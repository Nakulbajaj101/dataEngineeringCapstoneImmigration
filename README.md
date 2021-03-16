# Immigration Analytical Engine 

The purpose of this project to create an open analytical database for consumption by US Government so they are able to understand immigration patterns.

## Motivation

The analytical team wants to assist stake holders to make and monitor policy decisions around immigration so that industries dependent on immigration such as tourism, business and international student industry are able to strive successfully. The immigration patterns can further help in making infrastructure based decisions. The analytical database should serve daily to long-term decision making needs. The team is typically interested to know how people travel to USA, for what purpose, from where and in which season. They would also want to know do people coming from overseas have a preference where they wanna live and is it dependent on demographic profile of the region.

## Data Sources

* I94 Immigration Data: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace.
* World Temperature Data: This dataset came from Kaggle. You can read more about it [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
* U.S. City Demographic Data: This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
* Airport Code Table: This is a simple table of airport codes and corresponding cities. It comes from [here](https://datahub.io/core/airport-codes#data).

## Built With

The section covers tools used to enable the project.

1. Amazon EMR process data and output to s3
2. PySpark to process and carry out ETL
3. Bash to create the infrastructure and delete the infrastructure

## Tables and Dictionaries

Located in dataDictionary.txt
[here](https://github.com/Nakulbajaj101/dataEngineeringCapstoneImmigration/blob/master/dataDictionary.txt)


## Data Model

![alt text](https://github.com/Nakulbajaj101/dataEngineeringCapstoneImmigration/blob/master/DataModel.png?raw=true)

## Project Files and Folders

1. etl.py - Contains all the logic to extract data from S3 and process data on spark and then load data as parquet files into the s3 folder and region specified by user.
2. sparkSubmit.sh - Shell file that will get executed on the emr cluster and will call the spark submit on the cluster and will run the etl.py
3. createCluster.sh - Contains the pipeline to automate infrastructure requirements which will create the emr cluster, load the etl.py file on the cluster and load data from udacity s3 into user specific s3 bucket
4. terminateCluster.sh - Contains the pipeline to destroy the infratsructure associated with the project.

## Extra files

Captsone Project Template.ipynb contains all the exploring, building the 
the data pipeline steps, including quality checks and answers all the project related questions in more detail.

helperFunctions.py has some functions that will be used by spark 
to run jobs.

i94MetadataMappings.py has various mappings of fields and codes that 
are used by immigration. These mappings are leveraged in spark job to perform etl and build some dimension tables

qualityTests.py has quality tests that are used by etl to assess quality
of the data, so no poor quality data is created

## Running the ETL pipeline

1. Create the editor role in aws iam
2. Configure the aws cli with the editor role access key and secret.
3. Create the ssh key pair for ec2 instance using aws cli, give it a name such as my-key-pair. Make sure key is stored in root directory and is in the same region in which emr cluster/ec2 instances will be created.
   `aws ec2 create-key-pair --key-name my-key-pair --query "KeyMaterial" --output text > my-key-pair.pem`

4. If you're connecting to your instance from a Linux computer, it is recommended that you use the following command to set the permissions of your private key file so that only you can read it.
   `chmod 400 MyKeyPair.pem`
   
5. Open terminal
6. Run createCluster.sh script to create the emr cluster and execute the spark job on the cluster
   .Pass the cluster name as first choice of argument and name of the key associated with ec2 instance

    `bash createCluster.sh <cluster_name> <keyName>`


## Destroying the infrastructure to avoid costs

1. Run the terminateCluster.sh that will terminate the cluster
   
    `bash terminateCluster.sh`
 
# Contact for questions or support


# Future State

Enhance the project and add another step in pipeline by pushing data to redshift cluster from s3. 


Nakul Bajaj
