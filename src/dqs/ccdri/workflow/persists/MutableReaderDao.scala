package dqs.ccdri.workflow.persists

import java.sql.Connection

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import dqs.ccdri.workflow.rules.Generic
import dqs.ccdri.workflow.utils.DataSources

import scala.collection.mutable.ListBuffer
import dqs.ccdri.workflow.{DQScoreDebugger, DQScoreLogger}
import java.sql.SQLException

/**
 * Reads from rdbms
 * Receives call from tasks
 *
 * @author Somnath
 */

object MutableReaderDao extends Serializable {

  def cds_dq_scorecard_kpi: DataFrame = {DataSources.spark.sql("SELECT * FROM " + DataSources.HIVE_DB_NAME + "cds_dq_scorecard_kpi")}
  
  def cds_dq_scorecard_enrichment: DataFrame = {DataSources.spark.sql("SELECT * FROM " + DataSources.HIVE_DB_NAME + "cds_dq_scorecard_enrichment")}
  
  def src_bank_transaction_data: DataFrame = {DataSources.spark.sql("SELECT * FROM " + DataSources.HIVE_DB_NAME + "src_bank_transaction_data")}
  
}