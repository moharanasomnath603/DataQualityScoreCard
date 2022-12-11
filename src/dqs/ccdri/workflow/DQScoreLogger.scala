package dqs.ccdri.workflow

import java.util.LinkedHashMap

import com.visa.cds.logging.Logger
import dqs.ccdri.workflow.utils.DataSources


object DQScoreDebugger{
  private val isDebugOn = DataSources.WF_ISDEBUG

  def debug(message: => Any): Unit = {
    if (isDebugOn != null && isDebugOn) {
      message
    }
  }
}

object DQScoreLogger {

  val mandatoryTags: LinkedHashMap[String, String] = new LinkedHashMap()
  mandatoryTags.put("app", "DQScoreCard")
  mandatoryTags.put("jobid", "NA")
  mandatoryTags.put("appid", "NA")
  mandatoryTags.put("oozieid", "ooizeId")
  mandatoryTags.put("module", getClass.getName)

  private val logger = Logger.getLogger(getClass.getName, mandatoryTags)
  private val debugger = DQScoreDebugger

  def initiallizeLogger(jobId:String, appid:String, oozieid:String):Unit={
    logger.updateMandatoryTag("jobid", jobId)
    logger.updateMandatoryTag("appid", appid)
    logger.updateMandatoryTag("oozieid", oozieid)
  }


  class jobDetailInfo(jobId: String, jobStepName: String, scheduleJobId: String, applicationJobId: String, applicationStatusCd: String,
                      logStatusCd: String, logStatusMessage: String) {
    val JOB_ID = jobId
    val JOB_MDLE_NM = jobStepName
    val SCHD_JOB_ID = scheduleJobId
    val APPLN_JOB_ID = applicationJobId
    val APPLN_STATUS_CD = applicationStatusCd
    val LOG_STATUS_CD = logStatusCd
    val LOG_STATUS_MSG = logStatusMessage
  }

  def info(msg:String): Unit ={
    logger.info(msg)
    debugger.debug(println("[INFO]"+msg))
  }

  def debug(msg:String): Unit ={
    debugger.debug(println("[DEBUG]"+msg))
  }

  def debugFunction(message: => Any): Unit ={
    debugger.debug(println("[DEBUG]"+message))
  }

  def detailedError(msg:String, err:Throwable,ifLog: Boolean=true): Unit ={
    if(ifLog){
      logger.error(msg,err)
    }
    debugger.debug(println("[ERROR]"+msg+"\n"+err.getMessage))
  }

  def error(msg:String,ifLog: Boolean=true): Unit ={
    if(ifLog){
      logger.error(msg)
    }
    debugger.debug(println("[ERROR]"+msg))
  }
}