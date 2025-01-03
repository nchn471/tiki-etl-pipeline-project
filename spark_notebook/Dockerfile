FROM jupyter/all-spark-notebook:python-3.11

USER root 

# RUN curl -O https://dlcdn.apache.org/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz \
#     && tar zxvf spark-3.5.4-bin-hadoop3.tgz \
#     && rm -rf spark-3.5.4-bin-hadoop3.tgz \
#     && mv spark-3.5.4-bin-hadoop3/ /usr/local/ \
#     && rm -rf /usr/local/spark \
#     && rm -rf spark-3.5.0-bin-hadoop3 \
#     && ln -s /usr/local/spark-3.5.4-bin-hadoop3 /usr/local/spark

RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.29.43/s3-2.29.43.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.780/aws-java-sdk-1.12.780.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -O https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
    && mv s3-2.29.43.jar /usr/local/spark/jars \
    && mv aws-java-sdk-1.12.780.jar /usr/local/spark/jars \
    && mv aws-java-sdk-bundle-1.12.262.jar /usr/local/spark/jars \
    && mv delta-spark_2.12-3.2.1.jar /usr/local/spark/jars \
    && mv delta-storage-3.2.1.jar /usr/local/spark/jars \
    && mv hadoop-aws-3.3.4.jar /usr/local/spark/jars \
    && mv postgresql-42.7.4.jar /usr/local/spark/jars/postgresql-42.7.4.jar


RUN pip install jupyterlab_darkside_theme
