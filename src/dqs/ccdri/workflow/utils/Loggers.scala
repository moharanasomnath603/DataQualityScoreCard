package dqs.ccdri.workflow.utils

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import dqs.ccdri.workflow.rules.DateTime
import dqs.ccdri.workflow.{DQScoreDebugger, DQScoreLogger}

/**
 * Intercepts every call
 *
 * @author Somnath
 */

case class Execution(job_id: String, job_name: String, execution_step: String, count: String, run_ts: String)
case class Summary(job_id: String, run_ts: String, status: String)

object Loggers extends Serializable {

  var executionStep = ""
  var jobId = ""
  var executions = new ListBuffer[Execution]()
  var summary = new ListBuffer[Summary]()

  def workflowHandler(schemaName: String, exe: => Unit, JOB_ID: String) = {
    val startTimeOfWorkflow = System.currentTimeMillis
    var summeryStatus = "SUCCESS"
    try {
      exe
    } catch {
      case e: Throwable =>
        logException(JOB_ID, e.getMessage)
        summeryStatus = "FAILURE"
    } finally {
      /*ImmutableWriterDao.jobExecution(schemaName, executions)
      summary += Summary(JOB_ID, (System.currentTimeMillis - startTimeOfWorkflow).toString, summeryStatus)
      ImmutableWriterDao.jobSummary(schemaName, summary)
      DataSources.sparkContext.stop*/
    }
  } 

  def workflowHandler(schemaName: String, exe1: => Unit, exe2: => Unit, JOB_ID: String) = {
    jobId = JOB_ID
    try {
      exe1
      exe2
    } catch {
      case e: Throwable => logException(JOB_ID, e.getMessage)
    } finally {
      /*ImmutableWriterDao.jobExecution(schemaName, executions)
      DataSources.sparkContext.stop*/
    }
  }

  private def logException(JOB_ID: String, message: String) = {
    val msg = "ERROR :- " + message
    println(msg)
    executions //+= Execution(JOB_ID, executionStep, msg, "", DateTime.getCurrentTimeStampInFormat)
  }

  def logDataframe(JOB_ID: String, exe: => DataFrame, moduleName: String) = {
    def logDataframeSlow(JOB_ID: String, exe: => DataFrame, moduleName: String) = {
      //executionStep = moduleName
      //println(moduleName)
      //val startTime = System.currentTimeMillis
      val result = exe
      //val duration = System.currentTimeMillis - startTime
      //logMessage(JOB_ID, moduleName, result.count.toString, duration + " ms")
      //result.cache
      //result.show
      result
    }
    def logDataframeQuick(JOB_ID: String, exe: => DataFrame, moduleName: String) = {
      //executionStep = moduleName
      //println(moduleName)
      //val startTime = System.currentTimeMillis
      val result = exe
      //val duration = System.currentTimeMillis - startTime
      //logMessage(JOB_ID, moduleName, "\t", duration + " ms")
      result
    }
    logDataframeSlow(JOB_ID, exe, moduleName)
  }

  def logUnit(JOB_ID: String, df: DataFrame, exe: => Unit, moduleName: String) = {
    def logUnitSlow(JOB_ID: String, df: DataFrame, exe: => Unit, moduleName: String) = {
      executionStep = moduleName
      println(moduleName)
      DQScoreLogger.debugFunction( df.show )
      val startTime = System.currentTimeMillis
      exe
      val duration = System.currentTimeMillis - startTime
      logMessage(JOB_ID, moduleName, "\t", duration + " ms")
    }
    def logUnitQuick(JOB_ID: String, df: DataFrame, exe: => Unit, moduleName: String) = {
      executionStep = moduleName
      println(moduleName)
      val startTime = System.currentTimeMillis
      exe
      val duration = System.currentTimeMillis - startTime
      logMessage(JOB_ID, moduleName, "\t", duration + " ms")
    }
    logUnitSlow(JOB_ID, df, exe, moduleName)
  }

  def logSQL(JOB_ID: String, moduleName: String, SQL: String, count: Int) = {
    executionStep = moduleName
    logMessage(JOB_ID, moduleName, count.toString, SQL)
  }

  def logThisMessage(message: String) = {
    logMessage(jobId, executionStep, "", message)
  }

  def logDF(JOB_ID: String, moduleName: String, count: Int) = {
    executionStep = moduleName
    logMessage(JOB_ID, moduleName, count.toString, "")
  }

  private def logMessage(JOB_ID: String, moduleName: String, count: String, durationOrSql: String) = {
    println("====> " + moduleName + " " + count + " " + durationOrSql)
    executions //+= Execution(JOB_ID, moduleName, durationOrSql, count, DateTime.getCurrentTimeStampInFormat)
  }

  // TODO
  def logArguments(JOB_ID: String, moduleName: String, rdd: Array[(Any, Any)]) = {
    logMessage(JOB_ID, moduleName, rdd.map(x => x._1.toString + "," + x._2.toString).mkString("\t"), "")
  }

  // TODO
  private def logUDF(JOB_ID: String, exe: => Column, moduleName: String) = {
    executionStep = moduleName
    val startTime = System.currentTimeMillis
    val result = udf(() => exe)
    val duration = System.currentTimeMillis - startTime
    logMessage(JOB_ID, moduleName, "", duration + " ms")
    result
  }

  // TODO
  private def isNotEmpty(JOB_ID: String, df: DataFrame, moduleName: String) = {
    val startTime = System.currentTimeMillis
    val count = df.count
    val duration = System.currentTimeMillis - startTime
    logMessage(JOB_ID, moduleName, count.toString, duration + " ms")
    0 < count
  }

}