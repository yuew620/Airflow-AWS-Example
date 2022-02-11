Hive提交方法

1、使用command命令，执行SQL语句。

hive -e 'show database;'
hive -e 'select * from kds_lab1_input limit 1;'

2、使用command命令，执行SQL脚本。

hive -f s3://emr-wy1/hivesql/hiveTableQuery.sql -hivevar table=kds_lab1_input -hivevar startTime= '2022-01-25T00:00:00Z' -hivevar  endTime='2022-01-25T23:59:59Z'

hiveTableQuery.sql的内容如下
set mapred.job.queue.name=default;
set hive.exec.reducers.max=48;
set mapred.reduce.tasks=48;
set mapred.job.name=hive;
select * from ${table} where tuptime>='${startTime}' and tuptime<='${endTime}' limit 1;

完整的SQL
select * from kds_lab1_input where tuptime >= '2022-01-25T00:00:00Z ' and tuptime <= "2022-01-25T23:59:59Z" limit 1;

3、使用beeline命令，执行SQL脚本

aws s3 -cp s3://emr-wy1/hivesql/hiveTableQuery.sql ./
beeline -u "jdbc:hive2://localhost:10000" -f hiveTableQuery.sql -hivevar table=kds_lab1_input -hivevar startTime= '2022-01-25T00:00:00Z' -hivevar  endTime='2022-01-25T23:59:59Z'


4、Airflow SQL

https://airflow.apache.org/docs/apache-airflow-providers-apache-hive/stable/_api/airflow/providers/apache/hive/operators/hive/index.html#module-airflow.providers.apache.hive.operators.hive