package dqs.ccdri.workflow.rules

import org.apache.hadoop.fs._
import java.util.Properties
import org.apache.log4j.PropertyConfigurator
import org.apache.log4j.Logger
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions.{current_timestamp, lit, substring}
import org.apache.spark.sql.functions._
import dqs.ccdri.workflow.{DQScoreDebugger, DQScoreLogger}
import java.sql.DriverManager
import dqs.ccdri.workflow.rules.DQProfileRules._
import dqs.ccdri.workflow.rules.DQProfileRuleParams
import dqs.ccdri.workflow.utils.DataSources
import dqs.ccdri.workflow.persists.{MutableReaderDao}


class DQProfileRulesDAO (rulesDF: DataFrame, srcFeedTblNM: String) extends ExecuteRulesTrait {
  
    val sparkSession = DataSources.spark
    val strt_Timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.S")
            
    var srcCount = -1
    var query = ""
        
    @transient
    private val log: Logger = Logger.getLogger(classOf[DQProfileRulesDAO])

  
    //ONE METHOD THAT PERFORMS ALL OF THE OPERATION AS DRIVEN IN SEQUENCE 
    override def executeRules() {
      DQScoreLogger.info("[STATS_PROFILE-ETL] START")

        DQProfileRuleParams.setDQParams(rulesDF,srcFeedTblNM)
        runValidationChecks()
        
      DQScoreLogger.info("[STATS_PROFILE-ETL] END")
    }
   
    
    //VALIDATION CHECKS TO BE PERFORMED
    def runValidationChecks()={
      DQScoreLogger.info("[STATS_PROFILE-ETL] : \""+ DataSources.TARGET_DQS_REPORT_PATH +"\"")
      
      query = """SELECT a.*, CONCAT( 
                      CONCAT(""" + buildNullEmptyChkColumnList(DQProfileRuleParams.DQNullEmptyChkColsList) + """),
                      CONCAT(""" + buildRangeChkColumnList(DQProfileRuleParams.DQRangeChkColsList) + """),
                      CONCAT(""" + buildSplCharChkColumnList(DQProfileRuleParams.DQSplCharChkColsList) + """),
                      CONCAT(""" + buildDataTypeChkColumnList(DQProfileRuleParams.DQTblNM,DQProfileRuleParams.DQDataTypeChkColsList) + """),
                      CONCAT(""" + buildLengthChkColumnList(DQProfileRuleParams.DQLengthChkColsList) + """),
                      CONCAT(""" + buildDQCompletenessChkColumnList(DQProfileRuleParams.DQCompletenessChkColsList) + """),
                      CONCAT(""" + buildConsistencyChkColumnList(DQProfileRuleParams.DQConsistencyChkColsList) + """),
                      CONCAT(""" + buildCurrencyTimelinessChkColumnList(DQProfileRuleParams.DQTimelinessChkColsList) + """),
                      CONCAT(""" + buildPAIComplianceChkColumnList(DQProfileRuleParams.DQPAIComplianceChkColsList) + """) 
                 """ + """) AS VALIDATION_MSG 
                 FROM """ + DQProfileRuleParams.DQTblNM + """ a""" 
      
      DQScoreLogger.info("---------- FINAL QUERY BUILD ----------")
      DQScoreLogger.info(query)
      DQScoreLogger.info("---------------------------------------")
      
      val finalDQScoredReportDfIml = sparkSession.sql(query).withColumn("IS_VALID", when((col("VALIDATION_MSG") === ""),lit(1)).otherwise(lit(0))).withColumn("LOAD_DT",lit(strt_Timestamp))
      val finalDQScoredReportDfStg = finalDQScoredReportDfIml.select("transaction_id" ,"transaction_time" ,"item_code" ,"item_description" ,"no_of_items_purchased" ,"cost_per_item" ,"country" ,"account_num" ,"date_trans" ,"transaction_details" ,"chq_num" ,"withdrawl_amount" ,"deposit_amount" ,"balance_amount","VALIDATION_MSG","LOAD_DT").withColumn("DQ_TBL_NM",lit(DQProfileRuleParams.DQTblNM))
      DQScoreLogger.debugFunction("RULE OUTPUT from Source feed :- " + finalDQScoredReportDfStg.show(false) )
      
      /**
       * Tolerance Scoring
       */
      val kpiDf = MutableReaderDao.cds_dq_scorecard_kpi.selectExpr("is_actv_ind", "CASE WHEN kpi_rul_desc='Completeness Check' THEN 'COMPLETENESS-0'     WHEN kpi_rul_desc='DataType Check' THEN 'DATATYPE-0'     WHEN kpi_rul_desc='Length Check' THEN 'LENGTH-0'     WHEN kpi_rul_desc='BLANK/NULL Check' THEN 'NULL-60'     WHEN kpi_rul_desc='ALPHANUM Check' THEN 'ALPHANUM-60'     WHEN kpi_rul_desc='NUMERIC Check' THEN 'NUMERIC-60'     WHEN kpi_rul_desc='ALPHA Check' THEN 'ALPHA-60'     WHEN kpi_rul_desc='PAI COMPLIANCE Check' THEN 'PAI-0'     WHEN kpi_rul_desc='Consistency Check' THEN 'CONSISTENCY-60'     WHEN kpi_rul_desc='Timeliness Check' THEN 'TIMELINESS-70'     WHEN kpi_rul_desc='Current Delta Check' THEN 'DELTA-100'     WHEN kpi_rul_desc='Duplicate Check' THEN 'DUPLICATE-100'  END AS KPI_TOLRNC" )
                                              .where(col("is_actv_ind") === lit("Y"))
                                              .select("KPI_TOLRNC")//.rdd.collect.mkString(",").replaceAll("\\[","").replaceAll("\\]","").split(",").toSet
      val finalDQScoredReportDf = finalDQScoredReportDfStg.as("STG").join(kpiDf.as("KPI"), finalDQScoredReportDfStg("VALIDATION_MSG").contains(kpiDf("KPI_TOLRNC")) ,"left_outer")
                                  .selectExpr("STG.transaction_id" ,"STG.transaction_time" ,"STG.item_code" ,"STG.item_description" ,"STG.no_of_items_purchased" ,"STG.cost_per_item" ,"STG.country" ,"STG.account_num" ,"STG.date_trans" ,"STG.transaction_details" ,"STG.chq_num" ,"STG.withdrawl_amount" ,"STG.deposit_amount" ,"STG.balance_amount","STG.VALIDATION_MSG","STG.LOAD_DT","STG.DQ_TBL_NM", 
                                  """CASE WHEN KPI.KPI_TOLRNC LIKE '%COMPLETENESS%' THEN 0%
                                          WHEN KPI.KPI_TOLRNC LIKE '%DATATYPE%' THEN 0%
                                          WHEN KPI.KPI_TOLRNC LIKE '%LENGTH%' THEN 0%
                                          WHEN KPI.KPI_TOLRNC LIKE 'ALPHANUM' THEN '60%'
                                          WHEN KPI.KPI_TOLRNC LIKE 'NUMERIC' THEN '60%'
                                          WHEN KPI.KPI_TOLRNC LIKE 'ALPHA' THEN '60%'
                                          WHEN KPI.KPI_TOLRNC LIKE 'PAI' THEN '0%'
                                          WHEN KPI.KPI_TOLRNC LIKE 'CONSISTENCY' THEN '60%'
                                          WHEN KPI.KPI_TOLRNC LIKE 'TIMELINESS' THEN '70%'
                                          WHEN KPI.KPI_TOLRNC LIKE 'DELTA' THEN '100%'
                                          WHEN KPI.KPI_TOLRNC LIKE 'DUPLICATE' THEN '100%'
                                   AS DQ_SCORE""")
      DQScoreLogger.debugFunction("FINAL SCORE OUTPUT from Source feed :- " + finalDQScoredReportDf.show(false) )
      
      try {
          finalDQScoredReportDf.write.option("header",true).csv(DataSources.TARGET_DQS_REPORT_PATH)
      }
      catch {
          case ex: Throwable =>
          log.error("Exception occurred: ", ex)
          throw ex
          sparkSession.stop()
      }
      
    }
      
}