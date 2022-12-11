CREATE EXTERNAL TABLE cds_dq_scorecard_kpi(
KPI_RUL_GRP_ID bigint,
KPI_RUL_GRP_DESC string,
KPI_RUL_ID bigint,
KPI_RUL_DESC string,
TOLERANCE_PERCENT bigint,
PRIORITY_NUM bigint,
IS_ACTV_IND string,
CRTD_TS timestamp,
CRTD_BY_NM string)
COMMENT 'Data Quality Scorecard KPI'
PARTITIONED BY (load_date bigint)
STORED AS ORC
LOCATION 'hdfs://nameservicecrtocc/projects/ccdri/hive/cdsltacert/cds_dq_scorecard_kpi';

INSERT INTO cds_dq_scorecard_kpi partition(load_date=20221210) VALUES(100,'Data Completeness',1,'Completeness Check',0,1,'Y','2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_kpi partition(load_date=20221210) VALUES(200,'Data Standardization',2,'DataType Check',0,2,'Y','2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_kpi partition(load_date=20221210) VALUES(200,'Data Standardization',3,'Length Check',0,2,'Y','2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_kpi partition(load_date=20221210) VALUES(300,'Data Accuracy',4,'BLANK/NULL Check',60,2,'Y','2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_kpi partition(load_date=20221210) VALUES(300,'Data Accuracy',5,'ALPHANUM Check',60,2,'Y','2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_kpi partition(load_date=20221210) VALUES(300,'Data Accuracy',6,'NUMERIC Check',60,2,'Y','2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_kpi partition(load_date=20221210) VALUES(300,'Data Accuracy',7,'ALPHA Check',60,2,'Y','2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_kpi partition(load_date=20221210) VALUES(300,'Data Accuracy',8,'PAI COMPLIANCE Check',0,2,'Y','2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_kpi partition(load_date=20221210) VALUES(400,'Data Consistency',9,'Consistency Check',60,3,'Y','2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_kpi partition(load_date=20221210) VALUES(500,'Data Currency',10,'Timeliness Check',70,4,'Y','2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_kpi partition(load_date=20221210) VALUES(500,'Data Currency',11,'Current Delta Check',100,4,'Y','2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_kpi partition(load_date=20221210) VALUES(600,'Data Uniqueness',12,'Duplicate Check',100,5,'Y','2022-12-10 16:45:38','dqs');

+--------------+--------------------+----------+--------------------+-----------------+------------+-----------+-------------------+----------+---------+
|kpi_rul_grp_id|kpi_rul_grp_desc    |kpi_rul_id|kpi_rul_desc        |tolerance_percent|priority_num|is_actv_ind|crtd_ts            |crtd_by_nm|load_date|
+--------------+--------------------+----------+--------------------+-----------------+------------+-----------+-------------------+----------+---------+
|100           |Data Completeness   |1         |Completeness Check  |0                |1           |Y          |2022-12-10 16:45:38|dqs       |20221210 |
|200           |Data Standardization|2         |DataType Check      |0                |2           |Y          |2022-12-10 16:45:38|dqs       |20221210 |
|200           |Data Standardization|3         |Length Check        |0                |2           |Y          |2022-12-10 16:45:38|dqs       |20221210 |
|300           |Data Accuracy       |4         |BLANK/NULL Check    |60               |2           |Y          |2022-12-10 16:45:38|dqs       |20221210 |
|300           |Data Accuracy       |5         |ALPHANUM Check      |60               |2           |Y          |2022-12-10 16:45:38|dqs       |20221210 |
|300           |Data Accuracy       |6         |NUMERIC Check       |60               |2           |Y          |2022-12-10 16:45:38|dqs       |20221210 |
|300           |Data Accuracy       |7         |ALPHA Check         |60               |2           |Y          |2022-12-10 16:45:38|dqs       |20221210 |
|300           |Data Accuracy       |8         |PAI COMPLIANCE Check|0                |2           |Y          |2022-12-10 16:45:38|dqs       |20221210 |
|400           |Data Consistency    |9         |Consistency Check   |60               |3           |Y          |2022-12-10 16:45:38|dqs       |20221210 |
|500           |Data Currency       |10        |Timeliness Check    |70               |4           |Y          |2022-12-10 16:45:38|dqs       |20221210 |
|500           |Data Currency       |11        |Current Delta Check |100              |4           |Y          |2022-12-10 16:45:38|dqs       |20221210 |
|600           |Data Uniqueness     |12        |Duplicate Check     |100              |5           |Y          |2022-12-10 16:45:38|dqs       |20221210 |
+--------------+--------------------+----------+--------------------+-----------------+------------+-----------+-------------------+----------+---------+