测试Airflow的多账户的隔离

Airflow的User与IMA的User是一一对应的。在IAM的policy中可以对Airflow的ROLE进行控制。
在Airflow中可以设置Airflow的Role的权限，设置具体的airflow的操作，可以细粒度到DAG级别。
通过IAM与Airflow的权限的联合，实现租户的概念。

1、在IAM中新建用户，用户授予MWAA的权限

2、使用新的用户登陆Airflow之后，默认是Admin权限。这里会有奇怪的现象，会自动的给Airflow User添加Airflow的Role，包括Admin，Op, Viewer 等默认权限

3、在Airflow的Security的界面中List Role中，新建一个role，例如user DataEngineer权限，可以实现基于DAG的细粒度控制
   在list user中，给新的user添加新建的Role，user DataEngineer权限

4、在Airflow中viewer的权限，去掉allDag的读权限。

5、在IAM中，去掉用户的Airflow的Admin等权限，只留下viewer权限。具体配置如下

`{
"Version": "2012-10-17",
"Statement": [
{
"Effect": "Allow",
"Action": "airflow:CreateWebLoginToken",
"Resource": [
"arn:aws:airflow:ap-southeast-1:061088380739:role/mwaa-environment-public-network-MwaaEnvironment/userDataEngineer"
]
},
{
"Effect": "Allow",
"Action": "airflow:*",
"Resource": [
"arn:aws:airflow:ap-southeast-1:061088380739:*"
]
},
{
"Effect": "Deny",
"Action": "airflow:CreateWebLoginToken",
"Resource": [
"arn:aws:airflow:ap-southeast-1:061088380739:role/mwaa-environment-public-network-MwaaEnvironment/Admin"
]
},
{
"Effect": "Deny",
"Action": "airflow:CreateWebLoginToken",
"Resource": [
"arn:aws:airflow:ap-southeast-1:061088380739:role/mwaa-environment-public-network-MwaaEnvironment/Public"
]
},
{
"Effect": "Deny",
"Action": "airflow:CreateWebLoginToken",
"Resource": [
"arn:aws:airflow:ap-southeast-1:061088380739:role/mwaa-environment-public-network-MwaaEnvironment/Op"
]
},
{
"Effect": "Deny",
"Action": "airflow:CreateWebLoginToken",
"Resource": [
"arn:aws:airflow:ap-southeast-1:061088380739:role/mwaa-environment-public-network-MwaaEnvironment/User"
]
}
]
}`


