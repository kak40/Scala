# Scala pour Spark

1. Chargement Spark dans Ambari

        sudo su spark
        export SPARK_MAJOR_VERSION=2   #préciser version 2
        cd /usr/hdp/current/spark2-client/
        hdfs dfs -copyFromLocal /etc/hadoop/conf/log4j.properties /tmp/data.txt
        ./bin/spark-shell

