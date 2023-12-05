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
import logging
import os
from dotenv import load_dotenv
from pyspark.sql.types import StructType, StructField, StringType

from session import session

# =========================================================================
# Spark Streamming POC.
# see:
# https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch
# TODO  example is incomplete
# Global variables & initialization for the environment variables
load_dotenv()  # this line must be commented when we want to dockerize it


def schema():
    sc = StructType()
    sc.add(StructField('to', StringType(), True))
    sc.add(StructField('template', StringType(), True))
    sc.add(StructField('from', StringType(), True))
    sc.add(StructField('subject', StringType(), True))
    sc.add(StructField('body', StringType(), True))

    return sc


# =============================================================================
# Define the streaming in the streaming directory
def startStreaming():
    sc = schema()
    df = session.spark.readStream\
                      .schema(sc)\
                      .json('file:///Users/Shared/stream-files')
    df.printSchema()
    out_put = df.writeStream.format("console") \
                .option("checkpointLocation", 'file:///Users/Shared/checkpoint') \
                .outputMode("append").start() \
                .awaitTermination()


# ============================================================================================================
# Main body for Spark streamming
# ============================================================================================================
if __name__ == '__main__':
    global spark
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    logging.info(f'Start application:{os.getenv("APP_NAME")} version:{os.getenv("VERSION")}')
    if os.getenv("RUN_SPARK") == "true":
        session.openSession()
        logging.info('The Spark session has been open!!')
        startStreaming()
        session.spark.stop()
        logging.info('The Spark session has been close!!')
    else:
        logging.info("End of program")
