package dqs.ccdri.workflow.rules

import java.text.SimpleDateFormat
import java.util.Date

import scala.reflect.runtime.universe

import org.apache.spark.sql.functions.udf

import dqs.ccdri.workflow.utils.DataSources

/**
 * Receives call from dataframes
 * TODO : Remove udf from functions to make them unit testable
 *
 * @author Somnath
 */

object DateTime extends Serializable {

  def dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS")
  
  def getCurrentTimeStampInFormat = dateFormat.format(new Date())
  
  def getCurrentTime = udf(() => { getCurrentTimeStampInFormat })

}