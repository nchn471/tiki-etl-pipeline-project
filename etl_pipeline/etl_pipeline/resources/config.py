MINIO_CONFIG = {
    "endpoint_url": "minio:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}

PSQL_CONFIG = {
    "host": "de_psql",
    "port": 5432,
    "database": "postgres",
    "user": "admin",
    "password": "admin123",
}

SPARK_CONFIG = {
    "spark.jars": "/usr/local/spark/jars/s3-2.29.43.jar,/usr/local/spark/jars/aws-java-sdk-1.12.780.jar,"
                  "/usr/local/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/usr/local/spark/jars/delta-spark_2.12-3.2.1.jar,"
                  "/usr/local/spark/jars/delta-storage-3.2.1.jar,/usr/local/spark/jars/hadoop-aws-3.3.4.jar",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minio",
    "spark.hadoop.fs.s3a.secret.key": "minio123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "master_url" : "spark://spark-master:7077",
    "bucket" : "warehouse",
}
