package dqs.ccdri.workflow.rules

import scala.reflect.runtime.universe

import org.apache.spark.sql.functions.udf
import dqs.ccdri.workflow.utils.DataSources

/**
 * Receives call from dataframes
 * TODO : Remove udf from functions to make them unit testable
 *
 * @author Somnath
 */

object Generic extends Serializable {
  
  def getIncrementedValue = udf((arg: Int) => Option(arg).getOrElse(0) + 1)

  def getLimit = (arg: Int) => if (arg == 0) 99999 else arg

  val coder1 = (groupFormats: String, HDP_TEMPL_ID: java.lang.Long, format_id: Int) =>
      { if (Option(HDP_TEMPL_ID).isDefined) "C" else if (groupFormats.split(",").toList.contains(format_id.toString)) "G" else "R" }
  def getExtractionType = udf(coder1)

  def monthId = (received_sent_dt: String) => received_sent_dt.substring(0, 4) + received_sent_dt.substring(5, 7)
  //def fileDate = (received_sent_dt: String) => received_sent_dt.substring(0, 4) + received_sent_dt.substring(5, 7)+ received_sent_dt.substring(8, 10)
  
  def fileDate = (received_sent_dt: String) => (received_sent_dt match {
    case null => null
    case _ => received_sent_dt.substring(0, 4) + received_sent_dt.substring(5, 7)+ received_sent_dt.substring(8, 10)
  }
  )
  def partition = udf(fileDate)
  
  /**
   * THIS REPLACES THE EXISTING HDP_TEMPL_ID WITH THE NEW SCRUBBED TEMPL_ID FROM MAP FOR THE COMMS PRESENT, ELSE REUSES THE EXISTING HD_TEMPL_ID 
   */
  def getScrubbedTemplateUdf(m: Map[String,String]) = udf((comm: String,hdpTemplId: String) => { 
      val res = m.getOrElse(comm,hdpTemplId)
      res
  })

  
  def fileDateLong = (received_sent_dt: String) => (received_sent_dt.substring(0, 4) + received_sent_dt.substring(5, 7)+ received_sent_dt.substring(8, 10)).toLong
  def partitionLong = udf(fileDateLong)
  
  //def fileTypeFormat = udf((formatId:String) => if (List("2","4","7","8","10","11","12","13","19").contains(formatId)) "VCFFLDL" else "VCFDL")
  def fileTypeFormat(flTemplates: List[String]) = { udf((formatId:String) => if (flTemplates.contains(formatId)) "VCFFLDL" else "VCFDL") }
}