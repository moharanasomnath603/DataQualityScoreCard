package dqs.ccdri.workflow

import java.io.{PrintWriter, Serializable}
import java.sql.{Connection, PreparedStatement, ResultSet, Timestamp}
import dqs.ccdri.workflow.utils.{Loggers, DateUtil} 
import dqs.ccdri.workflow.persists.{MutableReaderDao}
import dqs.ccdri.workflow.utils.DataSources
import java.sql.SQLException
import dqs.ccdri.workflow.{DQScoreDebugger, DQScoreLogger}
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import dqs.ccdri.workflow.rules.DQProfileRulesDAO

/**
 * Validates input arguments and triggers specific chain
 * Entry point for all workflow
 *
 * @author Somnath
 */



object Main extends Serializable {
  
  val accum = DataSources.sparkContext.accumulator(0, "First Trigger")
  var startTime: Timestamp = null
  
  def main(args: Array[String]): Unit = {

    DQScoreLogger.info("Start of worflow " + new java.text.SimpleDateFormat("HH.mm.ss").format(new java.util.Date()))
    val formatToSec = new java.text.SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.S")
    val formatToDay = new java.text.SimpleDateFormat("yyyy-MM-dd")
    
    /**
     * Fetch Metadata & Source data Feed
     */
    val kpiDf: DataFrame = MutableReaderDao.cds_dq_scorecard_kpi
    val rulesDf: DataFrame = MutableReaderDao.cds_dq_scorecard_enrichment
    val srcFeedDf: DataFrame = MutableReaderDao.src_bank_transaction_data
    var srcFeedTblNM = "src_bank_transaction_data"
    
    DQScoreLogger.debugFunction("All possible KPIs Defined :- " + kpiDf.show(false) )
    DQScoreLogger.debugFunction("All possible Rule(s) Defined for each Column of Source feed :- " + rulesDf.show(false) )
    
    /**
     * Rule Parser
     */
    (new DQProfileRulesDAO(rulesDf,srcFeedTblNM)).executeRules() 
        
    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    println("++++++++++++++++++++ End of worflow +++++++++++++++++++++++")
  }
}