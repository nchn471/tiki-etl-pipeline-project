FROM python:3.11.8-slim

WORKDIR /opt/dagster/app
COPY requirements.txt /opt/dagster/app
COPY vietnamese-stopwords.txt .
RUN apt update -y \
    && apt install -y openjdk-17-jdk \
    && apt-get clean && rm -rf /var/lib/apt/lists/* 

RUN pip install --upgrade pip \
    && pip install -r requirements.txt --index-url https://mirrors.aliyun.com/pypi/simple/

# RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.29.43/s3-2.29.43.jar \
#     && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.780/aws-java-sdk-1.12.780.jar \
#     && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
#     && curl -O https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
#     && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar \
#     && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
#     && curl -O https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
#     && mkdir -p /usr/local/spark/jars \
#     && mv s3-2.29.43.jar /usr/local/spark/jars \
#     && mv aws-java-sdk-1.12.780.jar /usr/local/spark/jars \
#     && mv aws-java-sdk-bundle-1.12.262.jar /usr/local/spark/jars \
#     && mv delta-spark_2.12-3.2.1.jar /usr/local/spark/jars \
#     && mv delta-storage-3.2.1.jar /usr/local/spark/jars \
#     && mv hadoop-aws-3.3.4.jar /usr/local/spark/jars \
#     && mv postgresql-42.7.4.jar /usr/local/spark/jars/postgresql-42.7.4.jar

RUN mkdir -p /usr/local/spark/jars  
COPY jars/ /usr/local/spark/jars



# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
WORKDIR /opt/dagster/app
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m","etl_pipeline.definitions"] 