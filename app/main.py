# Copyright (c) 2022, LMASS Desarrolladores, S.C.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are not permitted.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
#  main.py
#
#  Developed 2022 by LMASS Desarrolladores, S.C. www.legosoft.com.mx
#
import os
import logging
import time
import re
from datetime import datetime
from decimal import Decimal
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from time import sleep
# from dotenv import load_dotenv
# ^ note: this package is 'not' necessary to list it in the requirement.txt
# to install it run: pip install python-dotenv
from pyspark.sql.types import *
import pyspark.sql.utils

# Local module imports
from configuration import Configuration, OutputType
from session import session
from config_system_dates import ConfigSystemDates
from system_dates import SystemDates
from schema_in import *

# =========================================================================
# Global variables & initialization for the environment variables
# load_dotenv()  # note: this line must be commented when we want to dockerize it
configuration = None
schema = None
system_dates = None


# =============================================================================
# Do an infinite loop and observer any file that will be added in the directory
#
def observe():
    obs = Observer()
    obs.schedule(ObserverDirectory(), path=os.getenv("WATCH_DOG_PATH"))
    obs.start()

    try:
        while True:
            sleep(int(os.getenv("WATCH_DOG_SLEEP")))
    except KeyboardInterrupt:
        obs.stop()

    obs.join()


# Observer class for the directory 'in-files
class ObserverDirectory(PatternMatchingEventHandler):
    patterns = ["*.txt", "*.json"]  # adjust as required

    def process(self, event):
        # your actual code goes here

        # event.src_path will be the full file path
        # event.event_type will be 'created', 'moved', etc.
        logging.info(f'{event.event_type} observed on {event.src_path}')
        process_file(event.src_path)

    def on_created(self, event):
        self.process(event)


# ============================================================================================================
# Validation functions. These function must me in the main.py because they are called from the lambda function
# and cannot be in any module

# Validates an specific column in the RDD
def validate(value, attr):
    if value is None:
        if attr['isRequired']:
            return '901', f"El campo es requerido {attr['isRequired']}"
        else:
            return None, None       # nothing to validate since is None value
    attr_type = attr['type']
    converted_value = None  # The value as its type
    if attr_type == AttrType.STRING:
        if attr['length'] and len(value) != attr['length']:
            return '100', f"la longitud {len(value)} no es de {attr['length']}"
        if attr['minLength'] and len(value) < attr['minLength']:
            return '101', f"la longitud {len(value)} es menor a {attr['minLength']}"
        if attr['maxLength'] and len(value) > attr['maxLength']:
            return '102', f"la longitud {len(value)} es mayor a {attr['maxLength']}"
        if attr['pattern']:
            if re.fullmatch(attr['pattern'], value) is None:
                return '103', f"el valor '{value}' no coincide con el patrón '{attr['pattern']}'"
        if attr['min']:
            min_value = attr['min']
            if value < min_value:
                return '104', f"el valor {value} es menor a {attr['min']}"
        if attr['max']:
            max_value = attr['max']
            if value > max_value:
                return '105', f"el valor {value} es mayor a {attr['max']}"
        converted_value = value
    elif attr_type == AttrType.DATE:
        if attr['datePattern']:
            date_pattern = attr['datePattern']
        else:
            date_pattern = '%Y-%m-%d'
        try:
            converted_value = datetime.strptime(value, date_pattern)
            if attr['startDate']:
                start_date = datetime.strptime(value, '%Y-%m-%d')
                if converted_value < start_date:
                    return '121', f"la fecha {value} es anterior a la fecha de inicio {attr['startDate']}"
        except:
            return '120', f"la fecha {value} no corresponde al patrón {date_pattern}"
    elif attr_type == AttrType.DATE_TIME:
        if attr['datePattern']:
            date_time_pattern = attr['datePattern']
        else:
            date_time_pattern = '%Y-%m-%d, %H:%M:%S'
        try:
            converted_value = datetime.strptime(value, date_time_pattern)
            if attr['startDate']:
                start_date_time = datetime.strptime(value, '%Y-%m-%d')
            if converted_value < start_date_time:
                return '131', f"la fecha:hora {value} es anterior a la fecha:hora de inicio {attr['startDate']}"
        except:
            return '130', f"la fecha y tiempo {value} no corresponde al patrón {date_time_pattern}"
    elif attr_type == AttrType.INTEGER:
        try:
            converted_value = int(value)
            if attr['min']:
                min_value = int(attr['min'])
                if converted_value < min_value:
                    return '141', f"el valor {value} es menor a {attr['min']}"
            if attr['max']:
                max_value = int(attr['max'])
                if converted_value > max_value:
                    return '142', f"el valor {value} es mayor a {attr['max']}"
        except:
            return '140', f"El valor {value} no es un entero."
    elif attr_type == AttrType.DOUBLE:
        try:
            converted_value = float(value)
            if attr['min']:
                min_value = float(attr['min'])
                if converted_value < min_value:
                    return '151', f"el valor {value} es menor a {attr['min']}"
            if attr['max']:
                max_value = float(attr['max'])
                if converted_value > max_value:
                    return '152', f"el valor {value} es mayor a {attr['max']}"
        except:
            return '150', f"El valor {value} no es un valor con decimal."
    elif attr_type == AttrType.DECIMAL:
        try:
            converted_value = Decimal(value)
            if attr['min']:
                min_value = Decimal(attr['min'])
                if converted_value < min_value:
                    return '161', f"el valor {value} es menor a {attr['min']}"
            if attr['max']:
                max_value = Decimal(attr['max'])
                if converted_value > max_value:
                    return '162', f"el valor {value} es mayor a {attr['max']}"
        except:
            return '160', f"El valor {value} no es un valor 'decimal'."
    elif attr_type == AttrType.ENUM:
        if not attr['elements']:
            return '171', f"El valor {value} no tiene definido el conjunto de 'elementos'."
        elif attr['elements'] and not value in attr['elements']:
            return '170', f"el valor '{value}' no pertenece a '{attr['elements']}'"
        converted_value = value
    elif attr_type == AttrType.UNKNOWN:
        return '900', f"Se requiere definir el 'tipo' de valor."
    # This call is for validators
    for validator in attr['validators']:
        res = validator(converted_value, attr)
        if res is not None:
            return res

    return None, None


# Validates a row. Is makes a loop for all columns in the RDD
def validate_row(row, columns, sch):
    result_list = []
    err_col = err_val = err_desc = ''
    for col in columns:
        err = validate(row[col], sch[col])
        if err[0] is not None:
            err_col += col + '|'
            err_val += err[0] + '|'
            if err_desc is None or len(err_desc) < 256:
                err_desc += err[1] + '|'
        result_list.append(row[col])
    if len(err_col) == 0:
        return tuple(result_list) + (None, None, None)  # no errors
    else:
        return tuple(result_list) + (err_col, err_val, err_desc)  # errors found


# ============================================================================================================
# Process a file. This function has to be in the main.py because it uses a lambda function
def process_file(file_path: str) -> None:
    start = time.time()
    # Init configuration and schema from the Database
    global configuration
    global schema
    configuration = Configuration('')
    file_path_parts = file_path.split('/')
    file_path_parts.reverse()
    conf_name = file_path_parts[0]
    if conf_name.find('_') == -1:
        if conf_name.find('.') != -1:
            conf_name = conf_name[0:conf_name.find('.')]
    else:
        conf_name = conf_name[0:conf_name.rfind('_')]
    configuration.initConfiguration(conf_name)
    if configuration.config == {}:
        logging.error(f'No se procesó el archivo {file_path}')
        return
    else:
        logging.info('The configuration has been read')
    schema = Schema()
    if configuration.schema:
        schema.initSchema(configuration.schema, system_dates)
        schema.finishSchema()
        session.spark_cntxt.broadcast(schema.columns)
    columns = schema.columns
    attributes = schema.attributes
    # Extract the file
    try:
        pre_schema = schema.preSchema()
        if configuration.fileInputType() == 'json':      # like a multi-line file
            if file_path.find('.') and file_path[file_path.find('.') + 1:] == 'json':
                df = session.spark.read.option("multiline", "true").json(file_path)
            else:  # like a .txt file
                df_from_txt = session.spark.read.text(file_path)
                from pyspark.sql.functions import col, from_json
                df = df_from_txt.withColumn("jsonData", from_json(col("value"), pre_schema)) \
                                .select("jsonData.*")
        else:   # 'csv' file type
            df = session.spark.read.csv(file_path,
                                        sep=configuration.fieldDelIn(),
                                        header=configuration.withHeader(),
                                        schema=pre_schema,
                                        encoding='ISO-8859-1')
        session.spark.conf.set("spark.sql.ansi.enabled", "false")
        # apply validators to each field in each line to create RDD of tagged records
        rdd = df.rdd.map(lambda row: validate_row(row, columns, attributes))
        new_schema = pre_schema.add("ERROR_COL", StringType(), True) \
                               .add("ERROR_VAL", StringType(), True) \
                               .add("ERROR_DESC", StringType(), True)
        res_df = rdd.toDF(new_schema)
        # now divide the records that have errors and records free of errors
        error_df = res_df.filter("ERROR_COL IS NOT NULL")
        cols = ("ERROR_COL", "ERROR_VAL", "ERROR_DESC")
        no_error_df = res_df.filter("ERROR_COL IS NULL").drop(*cols)
        if configuration.output() == OutputType.FILE:
            no_error_df.write.mode("overwrite").csv(configuration.outputPath() + '/' + conf_name)
        elif no_error_df.count() > 0:
            if configuration.output() == OutputType.EVENT:
                logging.info('Send to Kafka')
                no_error_df.createOrReplaceTempView('table')
                from pyspark.sql.functions import monotonically_increasing_id
                mapping_df = session.spark.sql(configuration.mapping.sqlMapping(no_error_df.dtypes) + ' from table')
                row = Row("value")
                kafka_df = mapping_df.toJSON().map(row).toDF()
                from pyspark.sql.functions import monotonically_increasing_id
                kafka_df = kafka_df.withColumn('key', monotonically_increasing_id())
                kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                        .write \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BROKERS")) \
                        .option("topic", conf_name) \
                        .save()
            else:
                logging.info('Send to MAIL')
                row = Row("value")
                kafka_df = no_error_df.toJSON().map(row).toDF()
                from pyspark.sql.functions import monotonically_increasing_id
                kafka_df = kafka_df.withColumn('key', monotonically_increasing_id())
                kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                        .write \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BROKERS")) \
                        .option("topic", "sendSparkMail") \
                        .save()

        # now write the error frame. Independent for the configuration.output always save it in a file
        error_df.write.mode("overwrite").csv(configuration.outputPathError() + '/' + conf_name)
        end = time.time()
        logging.info(f'Se ha terminado el proceso del archivo:{(end - start):.2f} segundos.')
    except pyspark.sql.utils.AnalysisException as e:
        logging.error(f'Error en la lectura, proceso o escritura del archivo {file_path}')
        logging.error(str(e))
        return
    except pyspark.sql.utils.IllegalArgumentException as e:
        logging.error(
            f'Error: el esquema definido no corresponde a los campos del archivo:{file_path} no se procesó la información.')
        logging.error(str(e))
        return


# ============================================================================================================
# Main body
# ============================================================================================================
if __name__ == '__main__':
    global spark
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    logging.info(f'Start application:{os.getenv("APP_NAME")} version:{os.getenv("VERSION")}')
    system_dates = SystemDates()
    config_system_dates = ConfigSystemDates()
    system_dates.allSystemDates(config_system_dates.getAllSystemDates())
    if os.getenv("RUN_SPARK") == "true":
        session.openSession()
        logging.info('The Spark session has been open!!')
        observe()
        session.spark.stop()
        logging.info('The Spark session has been close!!')
    else:
        configuration.initConfiguration('ordenes')
        logging.info("End of program")

# ============================================================================================================
# Some performance measures: 1.64GB 50,000 orders
# One master one worker 1GB per executor 3,288 secs = 54 minutes
# One master three workers (4 + 4 + 2 cores) 1GB used , total 10 cores = 659.47 secs = 10.98 minutes
# One master three workers (4 + 4 + 2 cores) 4GB used, total = 736.94 secs = 12.26 minutes
#
# One master three workers (4 + 4 + 2 cores) 1GB used, total 6 cores = 17, 836 orders = 13.15 seg
#                                                    4,379 errors and 13,457 ok  Neo4j = 2.29 min.
