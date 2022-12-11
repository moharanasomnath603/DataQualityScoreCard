package dqs.ccdri.workflow.rules

import java.util.Properties
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import java.sql.Timestamp
import dqs.ccdri.workflow.utils.DataSources

/**
 * @author Somanatha Moharana
 * @version 1.0
 *
 *  A generic Companion Object for setting up Parameters dynamically across all environments alongwith common JOB level getters implementations
 */
object DQProfileRuleParams {
  var DQTblNM = ""
  var DQNullEmptyChkColsList,DQLengthChkColsList,DQRangeChkColsList,DQSplCharChkColsList,DQDataTypeChkColsList,DQCompletenessChkColsList,DQConsistencyChkColsList,DQCurrentDeltaChkColsList,DQTimelinessChkColsList,DQPAIComplianceChkColsList = Set[String]()

  def setDQParams(rulesDF: DataFrame, srcFeedTblNM:String) {
    val activeRulesDF = rulesDF.selectExpr("kpi_rul_id","input_fld_nm","addnl_prmptd_col['ADDNL_PRMPTD_COL1'] AS ADDNL_PRMPTD_COL1","addnl_prmptd_col['ADDNL_PRMPTD_COL2'] AS ADDNL_PRMPTD_COL2")
    DQTblNM = srcFeedTblNM
        
    DQNullEmptyChkColsList = activeRulesDF.where(col("kpi_rul_id") === lit(4)).select("input_fld_nm").rdd.collect.mkString(",").replaceAll("\\[","").replaceAll("\\]","").split(",").toSet
    DQLengthChkColsList = activeRulesDF.where(col("kpi_rul_id") === lit(3)).select("input_fld_nm","ADDNL_PRMPTD_COL1").withColumn("source_column",expr("concat_ws('~',input_fld_nm,ADDNL_PRMPTD_COL1)")).select("source_column").rdd.collect.mkString(",").replaceAll("\\[","").replaceAll("\\]","").split(",").toSet
    DQCompletenessChkColsList = activeRulesDF.where(col("kpi_rul_id") === lit(1)).select("input_fld_nm").rdd.collect.mkString(",").replaceAll("\\[","").replaceAll("\\]","").split(",").toSet
    DQDataTypeChkColsList = activeRulesDF.where(col("kpi_rul_id") === lit(2)).select("input_fld_nm","ADDNL_PRMPTD_COL1").withColumn("source_column",expr("concat_ws('~',input_fld_nm,upper(ADDNL_PRMPTD_COL1))")).select("source_column").rdd.collect.mkString(",").replaceAll("\\[","").replaceAll("\\]","").split(",").toSet
    DQConsistencyChkColsList = activeRulesDF.where(col("kpi_rul_id") === lit(9)).select("input_fld_nm","ADDNL_PRMPTD_COL1").withColumn("source_column",expr("concat_ws('~',input_fld_nm,upper(ADDNL_PRMPTD_COL1))")).select("source_column").rdd.collect.mkString(",").replaceAll("\\[","").replaceAll("\\]","").split(",").toSet
    DQTimelinessChkColsList = activeRulesDF.where(col("kpi_rul_id") === lit(10)).select("input_fld_nm","ADDNL_PRMPTD_COL1","ADDNL_PRMPTD_COL2").withColumn("source_column",expr("concat_ws('~',input_fld_nm,upper(ADDNL_PRMPTD_COL1),upper(ADDNL_PRMPTD_COL2))")).select("source_column").rdd.collect.mkString(",").replaceAll("\\[","").replaceAll("\\]","").split(",").toSet
    DQPAIComplianceChkColsList = activeRulesDF.where(col("kpi_rul_id") === lit(8)).select("input_fld_nm").rdd.collect.mkString(",").replaceAll("\\[","").replaceAll("\\]","").split(",").toSet
  }

 
}
