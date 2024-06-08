# description
PoC Java Spark Minio 

# configurations

At the time of writing this answer, Spark does not support Java 17 - only Java 8/11 (source: https://spark.apache.org/docs/latest/). You must add this configuration as VM argument:

```
--add-exports java.base/sun.nio.ch=ALL-UNNAMED
```

# minio
To start minio locally read this repository

[PoC minio with docker](https://github.com/masalinas/poc-minio-docker/tree/master)
