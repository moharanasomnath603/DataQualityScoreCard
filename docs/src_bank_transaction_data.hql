CREATE EXTERNAL TABLE src_bank_transaction_data(
user_id bigint,
transaction_id bigint,
transaction_time string,
item_code bigint,
item_description string,
no_of_items_purchased bigint,
cost_per_item decimal(19,2),
country string,
account_num string,
date_trans date,
transaction_details string,
chq_num bigint,
withdrawl_amount decimal(19,2),
deposit_amount decimal(19,2),
balance_amount decimal(19,2))
COMMENT 'Contains Bank Transaction data'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://nameservicecrtocc/projects/ccdri/hive/cdsltacert/src_bank_transaction_data'
tblproperties ("skip.header.line.count"="2");

+-------+--------------+----------------------------+---------+-----------------------------------+---------------------+-------------+--------------+------------+----------+--------------------------------+-------+----------------+--------------+--------------+
|user_id|transaction_id|transaction_time            |item_code|item_description                   |no_of_items_purchased|cost_per_item|country       |account_num |date_trans|transaction_details             |chq_num|withdrawl_amount|deposit_amount|balance_amount|
+-------+--------------+----------------------------+---------+-----------------------------------+---------------------+-------------+--------------+------------+----------+--------------------------------+-------+----------------+--------------+--------------+
|361620 |6258318       |Tue Dec 11 10:46:00 IST 2018|443583   |SET/10 IVORY POLKADOT PARTY CANDLES|3                    |1.73         |United Kingdom|409000611074|2021-02-10|BBPS SETTLEMENT FOR DT 10       |null   |null            |0.90          |null          |
|305235 |6275038       |Sat Dec 22 11:39:00 IST 2018|493668   |MINI PLAYING CARDS DOLLY GIRL      |9                    |0.58         |United Kingdom|409000611074|2021-04-11|WAIVE SWITCHING FEES 31.0       |null   |null            |665.49        |null          |
|326655 |5919716       |Tue Feb 20 12:53:00 IST 2018|458052   |GARLAND WITH HEARTS AND BELLS      |18                   |6.84         |United Kingdom|409000611074|2022-08-23|TRF TO  Indiaforensic SERVICES I|null   |null            |0.00          |null          |
|294399 |6171792       |Fri Oct 05 11:39:00 IST 2018|482265   |3 TRADITIONAl BISCUIT CUTTERS  SET |18                   |2.90         |United Kingdom|409000611074|2022-09-03|TRF TO  Indiaforensic SERVICES I|null   |null            |0.00          |null          |
|306726 |6264027       |Sun Dec 16 07:29:00 IST 2018|460824   |KITTENS DESIGN FLANNEL             |3                    |1.18         |United Kingdom|409000611074|2022-09-07|TRF TO  Indiaforensic SERVICES I|null   |null            |0.00          |null          |
|null   |6149605       |Sun Sep 16 13:09:00 IST 2018|474810   |PENS ASSORTED FUNNY FACE           |99                   |0.58         |United Kingdom|409000611074|2022-07-02|Indfor INCOME INDO REMI30061	  |null   |960.00          |null          |null          |
|382746 |6146250       |Fri Sep 14 06:47:00 IST 2018|435225   |LUNCH BAG RED RETROSPOT            |60                   |2.28         |United Kingdom|409000611074|2022-07-02|Indfor INCOME INDO REMI01071    |null   |120.00          |null          |null          |
|267708 |6359705       |Mon Feb 04 07:32:00 IST 2019|486297   |SET OF 6 NATIVITY MAGNETS          |3                    |2.88         |United Kingdom|409000611074|2022-07-16|Indfor INCOME INDO REMI15071    |null   |360.00          |null          |null          |
|322518 |5983791       |Sun Apr 29 04:18:00 IST 2018|468993   |RETROSPOT PARTY BAG + STICKER SET  |24                   |2.28         |United Kingdom|409000611074|2022-07-16|Indfor INCOME INDO REMI14071    |null   |840.00          |null          |null          |
|383145 |6383421       |Wed Feb 13 10:48:00 IST 2019|466452   |FOUR HOOK  WHITE LOVEBIRDS         |18                   |2.90         |United Kingdom|409000611074|2022-10-04|TRF TO  Indiaforensic SERVICES I|null   |null            |0.00          |null          |
|null   |6141509       |Sun Sep 09 07:53:00 IST 2018|476658   |PINK REGENCY TEACUP AND SAUCER     |3                    |8.00         |United Kingdom|409000611074|2022-10-15|TRF TO  Indiaforensic SERVICES I|null   |null            |0.00          |null          |
|266301 |6327123       |Mon Jan 21 05:34:00 IST 2019|1789872  |SET/3 DECOUPAGE STACKING TINS      |9                    |6.84         |France        |409000611074|2022-10-22|TRF TO  Indiaforensic SERVICES I|null   |null            |300.00        |null          |
|null   |6058140       |Mon Jul 02 07:33:00 IST 2018|1784769  |75 GREEN FAIRY CAKE CASES          |6                    |2.88         |United Kingdom|409000611074|2022-10-29|TRF TO  Indiaforensic SERVICES I|null   |null            |0.00          |null          |
|307440 |6061968       |Sun Jul 08 08:31:00 IST 2018|448140   |CLASSIC METAL BIRDCAGE PLANT HOLDER|6                    |17.60        |United Kingdom|409000611074|2022-11-06|TRF TO  Indiaforensic SERVICES I|null   |null            |0.00          |null          |
+-------+--------------+----------------------------+---------+-----------------------------------+---------------------+-------------+--------------+------------+----------+--------------------------------+-------+----------------+--------------+--------------+

