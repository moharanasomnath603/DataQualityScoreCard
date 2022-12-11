CREATE EXTERNAL TABLE cds_dq_scorecard_enrichment(
TEMPL_ID bigint,
TEMPL_VER_NUM bigint,
ENRCHMT_RUL_ID bigint,
INPUT_FLD_NM string,
KPI_RUL_ID int,
ADDNL_PRMPTD_COL map<string,string>,
CRTD_TS timestamp,
CRTD_BY_NM string)
COMMENT 'Contains Column Metadata i.e. Positions read from the External Source/File(s) and Which Rule applies at Column level'
PARTITIONED BY (load_date bigint)
STORED AS ORC
LOCATION 'hdfs://nameservicecrtocc/projects/ccdri/hive/cdsltacert/cds_dq_scorecard_enrichment';
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,1,'user_id',1,map('ADDNL_PRMPTD_COL1',null,'ADDNL_PRMPTD_COL2',null),'2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,2,'transaction_id',1,map('ADDNL_PRMPTD_COL1',null,'ADDNL_PRMPTD_COL2',null),'2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,3,'no_of_items_purchased',1,map('ADDNL_PRMPTD_COL1',null,'ADDNL_PRMPTD_COL2',null),'2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,4,'account_num',1,map('ADDNL_PRMPTD_COL1',null,'ADDNL_PRMPTD_COL2',null),'2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,5,'date_trans',1,map('ADDNL_PRMPTD_COL1',null,'ADDNL_PRMPTD_COL2',null),'2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,6,'user_id',2,map('ADDNL_PRMPTD_COL1','integerType','ADDNL_PRMPTD_COL2',null),'2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,9,'item_code',2,map('ADDNL_PRMPTD_COL1','varcharType','ADDNL_PRMPTD_COL2',null),'2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,12,'cost_per_item',2,map('ADDNL_PRMPTD_COL1','decimalType','ADDNL_PRMPTD_COL2',null),'2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,15,'date_trans',2,map('ADDNL_PRMPTD_COL1','dateType','ADDNL_PRMPTD_COL2',null),'2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,21,'cost_per_item',3,map('ADDNL_PRMPTD_COL1','4','ADDNL_PRMPTD_COL2','2'),'2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,22,'user_id',4,map('ADDNL_PRMPTD_COL1',null,'ADDNL_PRMPTD_COL2',null),'2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,23,'chq_num',4,map('ADDNL_PRMPTD_COL1',null,'ADDNL_PRMPTD_COL2',null),'2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,24,'cost_per_item',9,map('ADDNL_PRMPTD_COL1','item_code','ADDNL_PRMPTD_COL2',null),'2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,25,'user_id',10,map('ADDNL_PRMPTD_COL1','date_trans','ADDNL_PRMPTD_COL2','15'),'2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,26,'cost_per_item',11,map('ADDNL_PRMPTD_COL1','date_trans','ADDNL_PRMPTD_COL2',null),'2022-12-10 16:45:38','dqs');
INSERT INTO cds_dq_scorecard_enrichment partition(load_date=20221210) VALUES(36610,1,27,'account_num',8,map('ADDNL_PRMPTD_COL1',null,'ADDNL_PRMPTD_COL2',null),'2022-12-10 16:45:38','dqs');
+--------+-------------+--------------+---------------------+----------+----------------------------------------------------------+-------------------+----------+---------+
|templ_id|templ_ver_num|enrchmt_rul_id|input_fld_nm         |kpi_rul_id|addnl_prmptd_col                                          |crtd_ts            |crtd_by_nm|load_date|
+--------+-------------+--------------+---------------------+----------+----------------------------------------------------------+-------------------+----------+---------+
|36610   |1            |1             |user_id              |1         |[ADDNL_PRMPTD_COL1 ->, ADDNL_PRMPTD_COL2 ->]              |2022-12-10 16:45:38|dqs       |20221210 |
|36610   |1            |2             |transaction_id       |1         |[ADDNL_PRMPTD_COL1 ->, ADDNL_PRMPTD_COL2 ->]              |2022-12-10 16:45:38|dqs       |20221210 |
|36610   |1            |3             |no_of_items_purchased|1         |[ADDNL_PRMPTD_COL1 ->, ADDNL_PRMPTD_COL2 ->]              |2022-12-10 16:45:38|dqs       |20221210 |
|36610   |1            |4             |account_num          |1         |[ADDNL_PRMPTD_COL1 ->, ADDNL_PRMPTD_COL2 ->]              |2022-12-10 16:45:38|dqs       |20221210 |
|36610   |1            |5             |date_trans           |1         |[ADDNL_PRMPTD_COL1 ->, ADDNL_PRMPTD_COL2 ->]              |2022-12-10 16:45:38|dqs       |20221210 |
|36610   |1            |6             |user_id              |2         |[ADDNL_PRMPTD_COL1 -> integerType, ADDNL_PRMPTD_COL2 ->]  |2022-12-10 16:45:38|dqs       |20221210 |
|36610   |1            |9             |item_code            |2         |[ADDNL_PRMPTD_COL1 -> varcharType, ADDNL_PRMPTD_COL2 ->]  |2022-12-10 16:45:38|dqs       |20221210 |
|36610   |1            |12            |cost_per_item        |2         |[ADDNL_PRMPTD_COL1 -> decimalType, ADDNL_PRMPTD_COL2 ->]  |2022-12-10 16:45:38|dqs       |20221210 |
|36610   |1            |15            |date_trans           |2         |[ADDNL_PRMPTD_COL1 -> dateType, ADDNL_PRMPTD_COL2 ->]     |2022-12-10 16:45:38|dqs       |20221210 |
|36610   |1            |21            |cost_per_item        |3         |[ADDNL_PRMPTD_COL1 -> 4, ADDNL_PRMPTD_COL2 -> 2]          |2022-12-10 16:45:38|dqs       |20221210 |
|36610   |1            |22            |user_id              |4         |[ADDNL_PRMPTD_COL1 ->, ADDNL_PRMPTD_COL2 ->]              |2022-12-10 16:45:38|dqs       |20221210 |
|36610   |1            |23            |chq_num              |4         |[ADDNL_PRMPTD_COL1 ->, ADDNL_PRMPTD_COL2 ->]              |2022-12-10 16:45:38|dqs       |20221210 |
|36610   |1            |24            |cost_per_item        |9         |[ADDNL_PRMPTD_COL1 -> item_code, ADDNL_PRMPTD_COL2 ->]    |2022-12-10 16:45:38|dqs       |20221210 |
|36610   |1            |25            |user_id              |10        |[ADDNL_PRMPTD_COL1 -> date_trans, ADDNL_PRMPTD_COL2 -> 15]|2022-12-10 16:45:38|dqs       |20221210 |
|36610   |1            |26            |cost_per_item        |11        |[ADDNL_PRMPTD_COL1 -> date_trans, ADDNL_PRMPTD_COL2 ->]   |2022-12-10 16:45:38|dqs       |20221210 |
|36610   |1            |27            |account_num          |8         |[ADDNL_PRMPTD_COL1 ->, ADDNL_PRMPTD_COL2 ->]              |2022-12-10 16:45:38|dqs       |20221210 |
+--------+-------------+--------------+---------------------+----------+----------------------------------------------------------+-------------------+----------+---------+