set mapred.job.queue.name=default;
set hive.exec.reducers.max=48;
set mapred.reduce.tasks=48;
set mapred.job.name=hive;


select * from ${table} where tuptime>='${startTime}' and tuptime<='${endTime}' limit 1;