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
#  resp_api.py
#
#  Developed 2022 by LMASS Desarrolladores, S.C. www.legosoft.com.mx
#
import requests
import json
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType


# ============================================================================================================
# Rest API calls using PySPark
# for more information see:
# https://medium.com/geekculture/how-to-execute-a-rest-api-call-on-apache-spark-the-right-way-in-python-4367f2740e78
#
# For How to manage Python Dependencies in spark see:
# https://www.databricks.com/blog/2020/12/22/how-to-manage-python-dependencies-in-pyspark.html
#
# We create the ingestor_batch_env.tar.gz package from conda
#
# os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"
#
#
# config("spark.archives",  # 'spark.yarn.dist.archives' in YARN.
#        "ingestor_batcH-env.tar.gz#environment")\
#
# ...
# self.spark_cntxt.addPyFile('rest_api.py')
#
# in main.py
# if no_error_df.count() > 0:
#   rest_api = RestApi()
#   rest_api_df = rest_api.createRestApiDF(session, no_error_df)
#   rest_api.callRestApiDF(session, rest_api_df)
#   no_error_df.show()
#
#
class RestApi:

    def __init__(self):
        schema = StructType([
                            StructField("Count", IntegerType(), True),
                            StructField("Message", StringType(), True),
                            StructField("SearchCriteria", StringType(), True),
                            StructField("Results", ArrayType(
                                StructType([
                                    StructField("Make_ID", IntegerType()),
                                    StructField("Make_Name", StringType())
                                ])
                            ))
                        ])
        # declare the UDF from executeRestApi
        self.udf_executeRestApi = udf(self.executeRestApi, schema)

    # ============================================================================================================
    # UDF for Spark columns to be executed
    def executeRestApi(self, verb, url, headers, body):
        res = None
        # Make API request, get response object back, create dataframe from above schema.
        try:
            return json.loads('{hola}')

            if verb == "get":
                res = requests.get(url, data=body, headers=headers)
            else:
                res = requests.post(url, data=body, headers=headers)
        except Exception as e:
            return e
        if res is not None and res.status_code == 200:
            return json.loads(res.text)
        return None

    def createRestApiDF(self, session, data_df):
        from pyspark.sql import Row

        headers = {
                    'content-type': "application/json"
                }
        body = json.dumps({
                })
        RestApiRequestRow = Row("verb", "url", "headers", "body")
        request_df = session.spark.createDataFrame([
                        RestApiRequestRow("get", "https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json", headers, body)
                    ])
        return request_df

    def callRestApiDF(self, session, request_df):
        df = request_df.withColumn("result", self.udf_executeRestApi(col("verb"), col("url"), col("headers"), col("body")))
        result_df = df.select(explode(col("result.Results")).alias("results"))
        result_df.show()  # TODO write to a result file

