Below are the commands to run, However the pyspark script has been parameterised for cluster and standalone run, it can be called via shell script as well:

For running standalone :
spark-submit --master yarn-cluster log_analytics.py ''

For running in cluster:
spark-submit --master yarn-cluster --conf spark.executor.extraClassPath=/usr/share/java/mysql-connector-java-5.1.29.jar,/usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar,/usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar,/usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar,/usr/share/java/sqljdbc4.jar --driver-class-path /usr/share/java/mysql-connector-java-5.1.29.jar,/usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar,/usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar,/usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar,/usr/share/java/sqljdbc4.jar --jars /usr/share/java/mysql-connector-java-5.1.29.jar,/usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar,/usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar,/usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar,/usr/share/java/sqljdbc4.jar log_analytics.py 'cluster'
