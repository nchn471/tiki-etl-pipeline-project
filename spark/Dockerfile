FROM bitnami/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y curl

RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.29.43/s3-2.29.43.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.780/aws-java-sdk-1.12.780.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar \
    && mv s3-2.29.43.jar /opt/bitnami/spark/jars \
    && mv aws-java-sdk-1.12.780.jar /opt/bitnami/spark/jars \
    && mv delta-spark_2.12-3.2.1.jar /opt/bitnami/spark/jars \
    && mv delta-storage-3.2.1.jar /opt/bitnami/spark/jars 