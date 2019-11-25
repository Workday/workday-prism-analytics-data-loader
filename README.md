# Workday Prism Analytics Data Loader

Workday Prism Analytics Data Loader is a free to use sample client for the Workday Prism Analytics data API. This is a community Project and not offcially supported by Workday.

## Downloading Workday Prism Analytics Data Loader

Download the latest version from [releases](https://github.com/Workday/workday-prism-analytics-data-loader/releases) and follow the examples below:

## Running Workday Prism Analytics Data Loader

## Prerequisite

### Download and install Java JDK (not JRE)

* [JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)

After installation is complete. open a console and check that the java version is 1.8 or higher by running the following command:

``java -version``

### Regsiter API client for Integration

In your Workday tenant run the task "Register API Client for Integrations" to register client. 

Once you register a new client note down the following info from your task:

* Workday REST API Endpoint
* Client ID
* Client Secret	
* Refresh Token

### CSV File format

Prism Data Loader assumes files are comma seperated and have a header line. The max uncompressed single file size is 24GB

### Console Mode

Open a terminal and type in the following command and follow the prompts on the console: 

``java -jar workday-prism-analytics-data-loader-<version>.jar``

Or you can pass in all the param in the command line:
 
``java -jar workday-prism-analytics-data-loader-<version>.jar --action load --endpoint WokdayRestAPIEndpoint --u clientId --p clientSecret --dataset datasetName --inputFile inputFile``

Input Parameter

``--action``  : load, loadAll, createSchema, detectEncoding. Use load for loading csv file or folder to a dataset, loadall for loading all files in a folder to individual datasets

``--u``       : Workday Rest API Client Id

``--p``       : Workday Rest API Client Secret. If you don't specify client secret you will be prompted for it on the console.

``--token``   : Workday Rest API Refresh Token

``--endpoint``: The Workday Rest API Endpoint URL. Example: https://wd2-impl-services1.workday.com/ccx/api/v2/{tenantName}

``--inputFile``: The input csv file or directory (or directory if action is loadall)

``--dataset`` : (Optional) the dataset name (required if action=load)

``--fileEncoding`` : (Optional) the encoding of the inputFile default UTF-8

``--operation`` : (Optional) the dataset update mode 'append' or 'replace'. Default is 'replace'


Example 1: Upload a csv to a dataset in append mode

``java -jar workday-prism-analytics-data-loader-<version>.jar --action load --operation append --endpoint https://wd2-impl-services1.workday.com/ccx/api/v2/{tenantName} --u 12345#! --p @#@#@# --token A1B2C3A1B2C3A1B2C3A1B2C3 --inputFile workers.csv --dataset test``

Example 2: Upload all files in a folder to Prism

``java -jar workday-prism-analytics-data-loader-<version>.jar --action loadAll --endpoint https://wd2-impl-services1.workday.com/ccx/api/v2/{tenantName} --u 12345#! --p @#@#@# --token A1B2C3A1B2C3A1B2C3A1B2C3 --inputFile dataDirectory``

Example 3: Generate the schema file from CSV

``java -jar workday-prism-analytics-data-loader-<version>.jar --action createSchema --inputFile workers.csv``

Example 4: detect file encoding

``java -jar workday-prism-analytics-data-loader-<version>.jar --action detectEncoding --inputFile workers.csv``

## Building Workday Prism Analytics Data Loader

``git clone git@github.com:workday/workday-prism-analytics-data-loader.git``

``mvn clean install``
