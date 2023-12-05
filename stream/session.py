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
#  session.py
#
#  Developed 2022 by LMASS Desarrolladores, S.C. www.legosoft.com.mx
#
import os
from pyspark.sql import SparkSession


class Session:

    def __init__(self):
        self.spark = None
        self.spark_cntxt = None

    def openSession(self):

        scala_version = '2.12'
        spark_version = '3.3.0'  # Ensure these versions match
        packages = [
            f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
            'org.apache.kafka:kafka-clients:2.8.l'
        ]
        # .config("spark.shuffle.service.enabled", "false") \
        # .config("spark.dynamicAllocation.enabled", "false") \
        print("packages")
        print(",".join(packages))
        print("===========")
        if os.getenv("ENVIRONMENT") == "local":
            self.spark = SparkSession.builder.config("spark.jars.packages", ",".join(packages))\
                                             .config("spark.executor.memory", "1g") \
                                             .appName(os.getenv("APP_NAME")).getOrCreate()
        else:
            self.spark = SparkSession.builder.config("spark.jars.packages", ",".join(packages))\
                                             .config("spark.executor.memory", "1g") \
                                             .config("spark.executor.cores", "3") \
                                             .config("spark.dynamicAllocation.enabled", "false") \
                                             .config("spark.shuffle.service.enabled", "false") \
                                             .config("spark.sql.files.ignoreMissingFiles", "true") \
                                             .appName(os.getenv("APP_NAME")).master(os.getenv("URL_SPARK")) \
                                             .getOrCreate()
        self.spark_cntxt = self.spark.sparkContext
        # self.spark_cntxt.addPyFile('system_dates.py')
        # self.spark_cntxt.addPyFile('numbers_validation.py')
        # self.spark_cntxt.addPyFile('schema_in.py')
        self.spark_cntxt.addPyFile('session.py')


session = Session()