# <img height="25" src="./images/AILLogoSmall.png" width="40"/> Data Ingestor

<a href="https://www.legosoft.com.mx"><img height="150px" src="./images/Icon.png" alt="AI Legorreta" align="left"/></a>
Python microservice for data ingestor. This microservice is developed in Python, PySpark and the Spark 
framework to import and data type to the AI Legorreta Market place. Many type fo files with different formats 
are supported (e.g., cvs, .xls, avro, json, etc.) and also do user defined valiadation.

This microservice is the client that run inside the spark-master declared in port 7077.

The ingestor objective is to import information from any external source (.xls, csv, avro, etc) and for future versions
stream data, parse it to do validation and to send Kafka events to any listener to do processing of the 
received data. The actual example is for orders.


## Parts of the ingestor:

Te ingestor architecture is composed by the following parts:

- Spark docker framework.
- A PySpark app (explained in this document). This Python app is responsible to read the files and import and validate the data. The correct data is
using Kafka events.
- The listeners tha process the data.
- The `acme-ui` app is the responsible application to define `datasources` in two parts:
  - Configuration os the data source (e.g., where output data goes: email, kafka events or output file).
  - The fields and validation: expected fields and validation process that must be processed per field.


## Launch a Python Spark application

Building and running your PySpark application on top of the Spark cluster is as simple as extending a template Docker 
image.  See directory

* [Python template](/template)

The example of the Python application is the spark-ingestor (see next section) code is stored in the /pySpark directory. 
See the Readme.md file for more details.


# Ingestor project. (Spark client) 'spark-ingestor'


The ingestor uses the project structure that is described in the Spark docker GitHub.

```
https://github.com/big-data-europe/docker-spark
```

note: Many project Python structures are proposed, but this one is the one that works best with stand-alone application
as dockerize clients.

The ingestor code is stored in the /spark-ingestor directory where the image is build and run. Please read the Read.me file in 
the directory for more documentation

To create the client docker image run this command

```bash
cd ..
bash build.sh ingestor spark-ingestor

```

To build and run the image created inside a new container

```bash
docker run --network ailegorretaNet --name ingestor bde2020/spark-ingestor:2.0.0
```

Or we can execute the docker-compose file:

````bash
cd spark-ingestor
docker-compose up
````

## Ingestor Validator

### Sources

The ingestor spark validator starting point is from :

```
https://github.com/pranab/chombo
```

For the blog see :

```
https://pkghosh.wordpress.com/2018/08/20/pluggable-rule-driven-data-validation-with-spark/
```

And his Github is in :

```
https://github.com/pranab/chombo
```

### Anaconda

Installation of Anaconda is required. For documentation of Anaconda see:

```
https://docs.anaconda.com/
```

### Python version requirements

The Python environment must be Python version 3.7.13 in order to be compatible with ths Spark master. Use Anaconda
Navigator to create this environment


## Data Validation

This microservice is a pluggable rule driven data validation solution implemented on Spark.

### Anatomy of a Validator

A validator essentially a simple predicate that expresses some constraint on the data. Here are some examples

- Value of a numeric field should be less than xxx
- A date field should have the format xxx
- A date should later than a specified date
- A text field size should be xxx
- A text field should comply with email format
- A categorical field value should be member of a pre-defined set of values

 There are more than 30 such validators available out of the box. The complete list of validators with description are 
 described in the next section..
 
 When multiple validators are applied to a field, if at least one of them fails, the field is considered to have failed 
 validation. For a given, field, even after one validator has failed, the rest of the validators in the chain are still 
 executed, so that we can get complete diagnostic of a field that failed validation.
 
 ### Out of the box validators
 
 Here is a list of validators available out of the box. More will be added in future as needed. They are listed under 
 different categories. The following validators are generic in nature and do not depend on the field data type.

| Tag            | Comment                         |
|----------------|---------------------------------|
| `notMissing`   | Ensures field is not missing.   |
| `ensureInt`    | Ensures field value is integer. |
| `ensureLong`   | Ensures field value is long.    |
| `ensureDouble` | Ensures field value is double.  |
| `ensureDate`   | Ensures field value is a date.  |


The following validators are available for string fields. The pattern validator can be used for phone number, email, 
date or any other data with pattern.


| Tag                 | Comment                                                                 |
|---------------------|-------------------------------------------------------------------------|
| `minLength`         | Ensures field length is greater than or equal to specified length.      |
| `maxLength`         | Ensures field length is less than or equal to specified length.         |
| `exactLength`       | Ensures field length is equal to specified length.                      |
| `min`               | Ensures that field is  greater than or equal to specified string value. |
| `max`               | Ensures that field is  less than or equal to  specified string  value.  |
| `pattern`           | Ensures that field matches given regex pattern.                         |
| `preDefinedPattern` | Ensures that field matches the selected out-of-the-box regex pattern.   |




The validator pre-defined Pattern supports many common regular expression patterns out of the box, so that the user does
not have to define them. The different attribute types supported are

- SSN
- phoneNumber
- streetAddress
- city
- zip
- state
- currency
- ID

The regex pattern desired should be indicated by selecting one of the names from the list and providing that as part of
the validator configuration.

If the regex pattern for these fields does not meet your requirement, you should use the more generic pattern validator 
and provide your own regex pattern through configuration.

The following validators are for numeric fields. For zscoreBasedRange validation, there is another map reduce class that
can be used to calculate mean and standard deviation of all numeric fields. The file path for the stats is provided 
through the parameter stat.file.path

For validation using more robust statistics based on median and median average divergence, the validator 
robustZscoreBasedRange can be used.

| Tag                      | Comment                                                      |
|--------------------------|--------------------------------------------------------------|
| `min`                    | Ensures field is greater than or equal to specified value.   |
| `max`                    | Ensures field is less than or equal to specified value.      |
| `zscoreBasedRange`       | Validates range based on mean and std deviation              |
| `robustZscoreBasedRange` | Validates range based on median and median average deviation |


The following validators are for date fields. Date value can be either formatted or in epoch time.


| Tag   | Comment                                                       |
|-------|---------------------------------------------------------------|
| `min` | Ensures field is later than or equal to specified date value. |
| `max` | Ensures field is earlier than or equal to specified value.    |


The following variables are for categorical fields. The only validator currently available is for membership check.


| Tag          | Comment                                                     |
|--------------|-------------------------------------------------------------|
| `membership` | Ensures field value is member of the categorical value set. |


All the validators covered so far apply to specified columns in the data. However, sometimes the validation logic spans 
the whole record and involves multiple columns simultaneously. For example, when one column has certain value the value
for another columns may be constrained in some ways.  Here are the row wise validators supported.


| Tag               | Comment                                                                           |
|-------------------|-----------------------------------------------------------------------------------|
| `notMissingGroup` | Among a group of columns, at least one of them needs to a have a non empty value. |
| `valueDependency` | When a column has certain value, the value in another column is constrained e.g.  |
|                   | member of a set of values for categorical data.                                   |


Validator for a single attribute:

Date time tags


| Tag           | Comment                                                                                              |
|---------------|------------------------------------------------------------------------------------------------------|
| `isHoliday`   | Check if the day is holiday; weeking and holiday days stored in the paramDB.                         |
| `isPast`      | Check that the date is previous from yesterday defined in 'system date' stored in parmDB.            |
| `isPastInc`   | Check that the date is previous from yesterday inclusive defined in 'system date' stored in paramDB. |
| `isFuture`    | Check that the date is posterior to tomorrow defined in 'system date' stored in parmDB.              |
| `isFutureInc` | Check that the date is posterior to tomorrow inclusive defined in 'system date' stored in paramDB.   |
| `isToday`     | Check that the dat is 'today' stored in paramDB.                                                     |

Number tags


| Tag            | Comment                                                                 |
|----------------|-------------------------------------------------------------------------|
| `isMultiple`   | For integer types and defines if the number is multiple.                |
| `isMoney`      | For decimal values and check it has just two decimals (not for floats). |
| `isPercentage` | For float and double numbers only.                                      |
| `isIva`        | Check is it has the 11 or 16 value.                                     |


### Json schema example

The JSON schema file contains various metadata required for different validators. One such example is the minimum 
value for min validator.

Example por product data test case: 

```
{
        "nombre" : "retailOrders",
        "atributos" :
        [
            {
                "nombre" : "date",
                "posicion" : 0,
                "tipo" : "Fecha",
                "patronFecha" : "%Y-%m-%d",
                "fechaInicio" : "2018-06-30",
                "validadores" : ["pasado", "diaHabil"]
            },
            {
                "nombre" : "storeID",
                "posicion" : 1,
                "id" : true,
                "tipo" : "Texto",
                "longitud" : 36,
                "validadores" : []
            },
            {
                "nombre" : "zip",
                "posicion" : 2,
                "tipo" : "Conjunto",
                "elementos" : ["95126", "95137", "94936", "95024", "94925", "94501", "94502", "94509", "94513", "94531", "94561", "94565",     "94541", "94546", "94552", "94577", "94578"],
                "validadores" : []
            },
            {
                "nombre" : "orderID",
                "posicion" : 3,
                "id" : true,
                "tipo" : "Texto"
            },
            {
                "nombre" : "productID",
                "posicion" : 4,
                "id" : true,
                "tipo" : "Texto",
                "validadores" : ["perteneceExt"]
            },
            {
                "nombre" : "category",
                "posicion" : 5,
                "tipo" : "Conjunto",
                "elementos" : ["general", "produce", "poultry", "dairy", "beverages", "bakery", "canned food", "frozen food", "meat", "cleaner", "paper goods"],
                "validadores" : []
            },
            {
                "nombre" : "quantity",
                "posicion" : 6,
                "tipo" : "Entero",
                "min" : 5,
                "lote" : 5,
                "validadores" : ["multiplo"]
            },
            {
                "nombre" : "amount",
                "posicion" : 7,
                "tipo" : "Decimal",
                "max" : 1000.0,
                "validadores" : ["dinero"]
            }
        ],
        "validadoresReg" : ["pipedMaxMonetaryAmount"]
}
```

The validators for a field may be  defined through property configuration file. Here is one example: 
validator.1=membership,notMissing. The left hand side contains the word validator followed by the ordinal of the field.
The right hand side contains the list of validators to be applied to the field.

Validator configuration may also be provided through hconf configuration file. For complex validators, this is the 
only option.

When multiple validators are applied to a field, if at least one of them fails, the field is considered to have 
failed validation. For a given, field, even after one validator has failed, the rest of the validators in the chain are still executed, so that we can get complete diagnostic of a field that failed validation.


### Validation configuration schema

The configuration is a JSON file, for example the metadata file for the retail order data show before.  It provides
metadata on the fields in data set being used.

Lots of validator configuration can be gleaned from the metadata files. This is natural as part of metadata consists 
of  various constraints on the data. This metadata file has a list of validator tags for each field in the data set 
that needs validation. Additional configuration is also available . For example, if we are enforcing max value for 
some numeric field, we can use max metadata for that field.

There are cases where the validator may require more complex configuration or for the kind of configuration required 
metadata file is not the natural home. For example, we have a validation check for membership with product ID for our 
use case (i.e., UDV).

When the cardinality of the set is small, the members can be defined in the metadata file. That’s not the case for 
product IDs. In our case we get the set of valid product ID, by reading certain column of a product table or file. The
configurations related to this are defined in the configuration file, which is based on Typesafe configuration format.

#### Default configuration

Default initial values for configuration are:

```
{
    "delimitadores": {
        "entrada" : ",",
        "salida" : ",",
        "encabezado": false"
    },
    "formato_entrada" : "csv",
    "tipo_salida": "archivo",
    "archivo_salida" : "file:///Users/rlh/LocalDeveloper/LMASSPocs/ingestor/err-files",
    "archivo_errores_salida" = "file:///Users/rlh/LocalDeveloper/LMASSPocs/ingestor/err-files"
    "debug": true,
    "perteneceAExt_productoID" : {
        "archivo" : "file:////Users/rlh/LocalDeveloper/LMASSPocs/ingestor/testData/data/producs.txt",
        "indice": 0,
        "delimitador": ",",
    },
    "dvMonto" :  {
        "codigo": "./check_max_amount.py",
        "params": ["cantidad", "monto"]
    },
    "udvAVGMonetaryAmount" {
        "codigo": "./check_max_amount.py",
        "params" : ["monto"],
        "archivo" : "/Users/pranab/Projects/bin/chombo"
    }
}
```

#### Configuration example 

Every few days each store uploads sales data since the last upload to the company data center, where is gets collected,
consolidated, cleaned and analyzed.

One of the tasks in the pipeline is data validation. Here are the list of fields along with validators configured for 
each field in the data set.

1.- Date of order – (ensureDate, min)
2.- Store ID – (exactLength)
3.- Store zip  code – (notMissing, membership)
4.- Order ID
5.- Product ID – (notMissing, membershipExtSrc)
6.- Product category – (notMissing, membership)
7.- Order quantity – (ensureInt, min)
8.- Monetary amount – (max)

Input example:

```
2018-07-29,DU7PIQ80,94509,4T16WXSXU3DP,4E0B1WFTK4,general,4,26.16
2018-07-29,DU7PIQ80,94509,4T16WXSXU3DP,KQJ6X2LD08,bakery,3,20.91
2018-07-29,DU7PIQ80,94509,BDI8Q9548ZMV,5Q78LFUM12,frozen food,1,4.16
2018-07-29,DU7PIQ80,94509,BDI8Q9548ZMV,8X4U3K5IX3,paper goods,5,9.25
2018-07-29,DU7PIQ80,94509,BDI8Q9548ZMV,2VJ4IF6S5F,produce,3,16.05
2018-07-29,DU7PIQ80,94509,BDI8Q9548ZMV,GMM4848K54,canned food,4,5.92
2018-07-29,DU7PIQ80,94509,BDI8Q9548ZMV,TA9M4BLKUW,canned food,1,1.92
```

## Technical notes

Some important python classes for the validator are defined in this section.

The microservice starts in main.py which runs and listen any file that is stored in the `in-files` directory 
(/Users/SHared/in-files; when a new file arrives in order to process it. The file name must have the next format:

```
<datasource_name>_<date or any other info>.txt
```
or for json multi-line
```
<datasource_name>_<date or any other info>.jsom
```

In order to make difference if the file is a csv or a json file is defined in the configuration, i.e., for mail channel
all is in json.

The `datasource_name` in the file must exist in the paramDB database in order to be processed.  The file is processed
and make all validations defined in the datasource configuration and all json validation.


## Environment variables

The variables are store in the following:

```
.env -> for the localNoDocker environment.
local.env -> is for the local environment (ie. inside the docker file definition)
```

note: when use the 'local' environment the 'load_dotenv()' file must be commented in order to use the file 'local.env' 
defined in the docker-compose.yml file.

## Docker-mages

The spark images where taken from: https://github.com/big-data-europe/docker-spark

Bitnami was not taken because it does not have any PySpark docker example. But for future research 
see: https://stackoverflow.com/questions/60630832/i-cannot-use-package-option-on-bitnami-spark-docker-container

## Python libraries

The python libraries must be imported using the requirement.txt file.

But if the user uses Pycharm it has to import these libraries using the spark-client environment created by PyCham.

```
watchdog==2.1.9
```
conda utilizes 'watchdog-2.1.6'

### Note: about Pyspark package

The package that 'must' be installed manually is version 3.3.1 to avoid this error when opening a session:

Unable to make private java.nio.DirectByteBuffer(long,int) accessible: module java.base does not "opens java.nio" to unnamed module

```
pip install 'pyspark==3.3.0'
```

## Note for importing Kafka jars in PySpark (or other jars)

When we run spark to use Kafka, Pyspark needs to have the jars included.

### Running in a IDE environment

When the spark-ingestor microservice is run inside the IntelliJ we need to add the next lines:

```
SparkSession.builder.config("spark.jars.packages", ",".join(packages)) ...
```

Where the packages are: 
- org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}' 
- org.apache.kafka:kafka-clients:2.8.1. 

These packages are downloaded at execution time inside the .ivy2/jars cache repository. The directory is local.

### Running inside Docker

The problem for running inside the docker is that the .ivy2/jars directory does nos exist and the jars are not loaded 
and PySpark does not send any error.

The solution was taken from: https://github.com/big-data-europe/docker-spark/issues/137

The solution is to download the jars manually, stored them in a /jar directory inside the project. Then the 'Docker' 
file copies the jar files inside the /spark/jars directory. After the file are copied the session.py imports the jars:

```
SparkSession.builder.config("spark.jars", ",".join(packages))...
```

And the packages are declared with all its dependencies:

- /spark/jars/org.apache.spark_spark-sql-kafka-0-10_{scala_version}-{spark_version}.jar
- /spark/jars/org.apache.spark_spark-streaming-kafka-0-10_{scala_version}-{spark_version}.jar
- /spark/jars/org.apache.kafka_kafka-clients-2.8.1.jar
- /spark/jars/org.apache.commons_commons-pool2-2.11.1.jar
- /spark/jars/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.3.0.jar

With these jars we assure that PSpark run correctly.

### note Pyspark image

The Pyspark image used is bde2020/spark-submit:3.3.0-hadoop3.3. This image has 'pip' inside it and it's and old version
that cannot install Pyhton packages declared in 'requirements.txt' file like 'python_graphql_client'.

The solution taken was to include in the 'Docker' file the

```
pip3 install --update pip
```

Instruction. This is executed every time the image is created.


# Running the application on a Spark standalone cluster via Docker

## Create the spark-master and spark-worker microservices

First you have to go to the '/third-party-services/docker-platform-third/spark' directory.

To run the Spark application, execute the following steps:

Set up a Spark cluster as described on `http://github.com/big-data-europe/docker-spark` by just running: 

```bash
git https://github.com/big-data-europe/docker-spark.git
cd ingestor
docker-compose up -d
```

note: Don´t forget to remove before the container and the image. To delete de image use:

```bash
docker image rm spark-ingestor-ingestor
```

note: This Spark image is version 3.3.0 and 'not' use the Bitnami image. This is because the bitnami image does not have
a python-client to be stored as a docker (i.e., pyspark client inside a docker)

## Run inside PyCharm

Use the .env variable to obtain the spark-master uri and any other environment variable. Select the main.py and run it.

## Run with docker-compose

This is the recommended method to run inside the Docker desktop that creates a container called 'spark-ingestor'

```
cd ingestor
docker compose up
```

## Run using the bash shell

First you have to go to the '/ingestor' directory.

1.-Build the Docker Pyhton image:

```bash
bash build.sh pyingestor spark-ingestor
```

2.-Run the Docker container:

```bash
docker rm pyingestor
docker run --network lmassNet --name pyingestor bde2020/spark-pyingestor:1.0.0
```

### Data validation spark job

The data validator Spark job is implemented in python object DataValidator. The output can be configured in multiple 
ways. All the output modes can be controlled with proper configuration of the Ingestor.

The invalid data go to out-file directory.

The invalid fields could be annotated in two ways. Each field in each record could be annotated with all the validators
for which the field failed. Alternatively the invalid fields could have overwritten with a mask. Here is some  sample
invalid records.

```
2018-07-29,DU7PIQ80,94509,R1N29FU8GI32,5H1758IPCY,GeaM#membership,0#min,6.02
2018-07-29,DU7PIQ80,94509,R1N29FU8GI32,W8I61V5I6G,bakery,4,#max
2018-07-29,DU7PIQ80H75#exactLength,94509,4M04TT2PB7VM,8S331G92T7,bKkBry#membership,1,2.82
2018/07/29#ensureDate#min,DU7PIQ80,94509,8K0H0LH9X6TI,59PO8KQEP4,general,0#min,992.32
2018-07-29,DU7PIQ80,94509,6692CU1RY735,JIS3ASRD36,KaperWgoods#membership,3,7.89
2018-07-29,DU7PIQ806#exactLength,94509,6692CU1RY735,335Q54KCYS,cleaner,1,2.76
```

We can see the validator tags separated by a configurable delimiter  and appended to the field in the output. These 
tags correspond to the validators that failed on the field. The invalid data could be used in many ways, For example, 
the invalid data could be be quarantined, until root causes have been identified and appropriate remedial actions have
been taken.


### Custom Validators

So far, we have applied validators independently on a per-column basis. But sometimes the validation logic is more 
complex and involves multiple fields. For example, if the value of field `a` is x then the value of field `b`
should be greater than y.

Piping to an external Pyhton script is a valid solution. Record wise validation by piping to an external Python script
is supported. With the current example we are doing record wise validation involving product ID and monetary amount 
fields.

A custom validator is defined as follows. A custom validator can be though of as an UDF (define UDV, User Defined 
Validator) that takes a field value as input and SOME CODE IN A Blockly fashion that 'always' returns true/false value.

A custom validator needs to have these characteristics:

- It has a unique tag
- Always return true/false value.
- Defined as a Blockly block code.

 Here are the steps for configuring row wise validators.

- Define tag for your validator, which has to start with piped. The tag for our validator is pipedMaxMonetaryAmount. 
The name is pipe + UDV.
- Define all row validators in the schema file. Look for rowValidators element in the JSON file.
- Define configuration for validator in the configuration file. The parameter script defines the script you want to run. 
In our case it’s a python script. The parameter config defines a list of configuration parameters needed by the script.
The parameter udvDir contains the working directory for the python script.
- All Python scripts and any file referred by the script should be copied to the udvDir in each node of the Spark 
cluster using the SYS UI application.

Here one record that failed row validation. Notice the failed row validators are tagged at the end of the 3rd record, 
separated by ##.

```
2018-07-29,DU7PIQ80ID#exactLength,#notMissing#membership,XE2BI1NHI6NC,JW9FDE906N,cFeanJr#membership,2,7.78
2018-07-29,DU7PIQ80,94509,AE43VPQ39XU7,8S4666972K,cleaner,0#min,736.91##pipedMaxMonetaryAmount
2018/07/29#ensureDate#min,DU7PIQ80O#exactLength,94509,AE43VPQ39XU7,0KSL332LAT,baVeAy#membership,2,852.31##pipedMaxMonetaryAmount
2018-07-29,DU7PIQ80Q#exactLength,94509,EX3T97U5SGC0,8ABHZY1689,cleUneU#membership,3,3.15
2018-07-29,DU7PIQ80,94509,XTJ3FJAET93N,CFE3H9T4RB,pouBtrK#membership,1,3.77
```

### Statistical Modeling in Data Validation

Statistical modeling  can play an important role in data validation. All the validation rules have one or more 
parameters. In some cases the parameters values are static and known. For example, it may be known that product ID 
field is supposed to be exactly 12 characters long.

However, in many other cases the parameters are dynamic and statistical models could be used to estimate them. 
For example, how do we know what the max value for monetary amount a customer has paid for certain product should be.

In other words, we want to define a max threshold such that any amount above that will be considered anomalous and 
flagged as invalid.

One approach will be to assume a normal distribution for the monetary amount for each product  and set the threshold 
to mean plus some multiple of standard deviation.  Historical data could be used for this purpose. This multiple is 
also known as z score. The monetary amount is proportional to the quantity. So the same technique could be applied for 
the quantity, instead of the monetary amount.

`Not developed yet`.


## Stress testing 

With 50,000 order that are all ok:

Configuration:

- Three spark workers:
  - Worker-1: 4 cores 4GB 1GB used
  - Worker-2: 4 cores 4GB 1GB used
  - Worker-3: 2 cores 4GB 1GB used
  - Master: 1GB per executor, driver memory: 4GB

Results:

- Generated them: 3.06 min.
- Process them: 9.08 min
- Process with errors: 8.54min
 

## Spark docker notes

For more detail it is better to read Spark framework in the director /ailegorreta/third-party-deployment/...

Docker images to:

* Set up a standalone [Apache Spark](https://spark.apache.org/) cluster running one Spark Master and multiple Spark workers
* Build Spark applications in Java, Scala or Python to run on a Spark cluster

<details open>
<summary>Currently supported versions:</summary>

* Spark 3.3.0 for Hadoop 3.3 with OpenJDK 8 and Scala 2.12

One spark master is created and three spark-workers. Depending on the data volume and hardware we can increase
or decrease the number of spark workers.
 

## Kubernetes deployment

The BDE Spark images can also be used in a Kubernetes environment.

To deploy a simple Spark standalone cluster issue

`kubectl apply -f https://raw.githubusercontent.com/big-data-europe/docker-spark/master/k8s-spark-cluster.yaml`

This will set up a Spark standalone cluster with one master and a worker on every available node using the default namespace and resources. The master is reachable in the same namespace at `spark://spark-master:7077`.
It will also setup a headless service so spark clients can be reachable from the workers using hostname `spark-client`.

Then to use `spark-shell` issue

`kubectl run spark-base --rm -it --labels="app=spark-client" --image bde2020/spark-base:3.3.0-hadoop3.3 -- bash ./spark/bin/spark-shell --master spark://spark-master:7077 --conf spark.driver.host=spark-client`

To use `spark-submit` issue for example

`kubectl run spark-base --rm -it --labels="app=spark-client" --image bde2020/spark-base:3.3.0-hadoop3.3 -- bash ./spark/bin/spark-submit --class CLASS_TO_RUN --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.host=spark-client URL_TO_YOUR_APP`

You can use your own image packed with Spark and your application but when deployed it must be reachable from the workers.
One way to achieve this is by creating a headless service for your pod and then use `--conf spark.driver.host=YOUR_HEADLESS_SERVICE` whenever you submit your application.


### Contact AI Legorreta

Feel free to reach out to AI Legorreta on [web page](https://legosoft.com.mx).


Version: 2.0
©LegoSoft Soluciones, S.C., 2023
