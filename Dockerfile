FROM bde2020/spark-submit:3.3.0-hadoop3.3
LABEL maintainer="(Rlh) AI Legorretae"

ENV SPARK_MASTER_NAME spark-master
ENV SPARK_MASTER_PORT 7077
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/main.py

# Copy the requirements.txt first, for separate dependency resolving and downloading
COPY requirements.txt /app/
# Before install the python packages declared requirements.txt we need to upgrade pip3 version store inside the image
RUN cd /app \
      && pip3 install --upgrade pip \
      && pip3 install -r requirements.txt
# Copy the Kafka jars needed for Spark
COPY ./jars /spark/jars
# Copy the source code
COPY app /app/
