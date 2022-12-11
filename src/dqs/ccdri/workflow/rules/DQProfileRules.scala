package dqs.ccdri.workflow.rules

import java.util.Properties
import org.apache.spark.sql.functions._
import java.security.MessageDigest
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import scala.util.matching.Regex
import dqs.ccdri.workflow.utils.DataSources
import dqs.ccdri.workflow.luhn
import dqs.ccdri.workflow.luhn.CardUtils

/**
 * @author Somanatha Moharana
 * Class extends Serializable to help calling SCALA UDFs over serialized singleton objects
 * This Placeholder defines below SCALA UDFs :
 * 		1. MD5 Checksum generation
 */

object DQProfileRules extends Serializable {
  
  val sparkSession = DataSources.spark
  
  //RULE-1 : UDF
  def registerNullEmptyChkUDF() = {
    sparkSession.udf.register("nullEmptyColChk", (colNM:String ,cols:String) =>  if((cols == null || cols == "") && colNM != "") colNM+"_NULL-EMPTY_CHK_FAIL;" else "" )
  }
  
  //RULE-1 : NULL/EMPTY CHECK
  def buildNullEmptyChkColumnList(myCols: Set[String]): String = {
    registerNullEmptyChkUDF
    if(myCols.last == "") {
      myCols.map(x => "nullEmptyColChk(\"" + x + "\", \"" + x + "\")").mkString(",")   
    }
    else {
      myCols.map(x => "nullEmptyColChk(\"" + x + "\", " + x + ")").mkString(",") 
    }
    
  }
  
  
  //RULE-2 : LENGTH CHECK
  def buildLengthChkColumnList(myCols: Set[String]): String = {
    if(myCols.last == "") {
      myCols.map(x => "CASE WHEN 0 > 1 THEN '" + x + "_LENGTH_CHK_FAIL;' ELSE '' END").mkString(",")   
    }
    else {
      val pipeRegExp: Regex = "\\|".r
      var finalRangestr = ""
      
      val ZeroPipeOcc = myCols.map(x => (x.split("~")(0),x.split("~")(1))).map(x => (x._1,x._2,pipeRegExp.findAllIn(x._2).length)).filter(x => x._3 == 0).map(x => "CASE WHEN LENGTH(" + x._1 + ") <> " + x._2 + " THEN '" + x._1 + "_LENGTH_CHK_FAIL;' ELSE '' END").mkString(",")
      val SinglePipeOcc = myCols.map(x => (x.split("~")(0),x.split("~")(1))).map(x => (x._1,x._2,pipeRegExp.findAllIn(x._2).length)).filter(x => x._3 == 1).map(x => (x._1,x._2,x._3,x._2.replace("|","~").split("~")(0),x._2.replace("|","~").split("~")(1))).map(x => "CASE WHEN LENGTH(" + x._1 + ") NOT BETWEEN " + x._2.replace("|","~").split("~")(0) + " AND " + x._2.replace("|","~").split("~")(1) + " THEN '" + x._1 + "_LENGTH_CHK_FAIL;' ELSE '' END").mkString(",")
      
      if(ZeroPipeOcc.isEmpty() == false && SinglePipeOcc.isEmpty() == false) {
        finalRangestr = ZeroPipeOcc + "," + SinglePipeOcc
      } 
      else if(ZeroPipeOcc.isEmpty() == true && SinglePipeOcc.isEmpty() == false) {
        finalRangestr = SinglePipeOcc
      }
      else if(ZeroPipeOcc.isEmpty() == false && SinglePipeOcc.isEmpty() == true) {
        finalRangestr = ZeroPipeOcc
      }
      else finalRangestr
      
      finalRangestr
      
      
    }
  }
  
  //RULE-3 : RANGE CHECK
  def buildRangeChkColumnList(myCols: Set[String]): String = {
    if(myCols.last == "") {
      myCols.map(x => "CASE WHEN 0 > 1 THEN '" + x + "_RANGE_CHK_FAIL;' ELSE '' END").mkString(",")   
    }
    else {
      val pipeRegExp: Regex = "\\|".r
      val numberRegExp: Regex = "[0-9]".r
      val alphaRegExp: Regex = "[a-zA-Z]".r
      var finalRangestr = ""
      
      val SinglePipeOcc = myCols.map(x => (x.split("~")(0),x.split("~")(1))).map(x => (x._1,x._2,pipeRegExp.findAllIn(x._2).length)).filter(x => x._3 == 1).map(x => (x._1,x._2,x._3,x._2.replace("|","~").split("~")(0),x._2.replace("|","~").split("~")(1))).map(x => (x._1,x._2,x._3,alphaRegExp.findFirstMatchIn(x._4), numberRegExp.findFirstMatchIn(x._4),alphaRegExp.findFirstMatchIn(x._5), numberRegExp.findFirstMatchIn(x._5) )).filter(x =>  (x._3 == 1) && (x._4 == None && x._5 != None)).filter(x => x._6 == None && x._7 != None).map(x => "CASE WHEN " + x._1 + " NOT BETWEEN " + x._2.replace("|","~").split("~")(0) + " AND " + x._2.replace("|","~").split("~")(1) + " THEN '" + x._1 + "_RANGE_CHK_FAIL;' ELSE '' END").mkString(",")
      val MultiplePipeOcc = myCols.map(x => (x.split("~")(0),x.split("~")(1))).map(x => (x._1,x._2,pipeRegExp.findAllIn(x._2).length)).map(x => (x._1,x._2,x._3,x._2.replace("|","~").split("~")(0))).map(x => (x._1,x._2,x._3,alphaRegExp.findFirstMatchIn(x._4), numberRegExp.findFirstMatchIn(x._4) )).filter(x =>  (x._3 != 1) || ((x._3 == 1) && (x._4 != None))).map(x => "CASE WHEN " + x._1 + " NOT IN ('" + x._2.replace(":","|").replace("|","','") + "') THEN '" + x._1 + "_RANGE_CHK_FAIL;' ELSE '' END").mkString(",")
      
      if(SinglePipeOcc.isEmpty() == false && MultiplePipeOcc.isEmpty() == false) {
        finalRangestr = SinglePipeOcc + "," + MultiplePipeOcc
      } 
      else if(SinglePipeOcc.isEmpty() == true && MultiplePipeOcc.isEmpty() == false) {
        finalRangestr = MultiplePipeOcc
      }
      else if(SinglePipeOcc.isEmpty() == false && MultiplePipeOcc.isEmpty() == true) {
        finalRangestr = SinglePipeOcc
      }
      else finalRangestr
      
      finalRangestr
    }
  }
  
  //RULE-4 : SPECIAL CHARACTER CHECK
  def buildSplCharChkColumnList(myCols: Set[String]): String = {
    val splCharRegExp: Regex = "[^a-zA-Z0-9]".r
    
    if(myCols.last == "") {
      myCols.map(x => "CASE WHEN 0 > 1 THEN '" + x + "_SPL_CHAR_CHK_FAIL;' ELSE '' END").mkString(",")   
    }
    else {
      myCols.map(x => "CASE WHEN " + x  + " RLIKE '" + splCharRegExp + "' THEN '" + x + "_SPL_CHAR_CHK_FAIL;' ELSE '' END").mkString(",")
    }
  }
  
  //RULE-5 : DATA TYPE CHECK
  def buildDataTypeChkColumnList(DQTblNM: String, myCols: Set[String]): String = {
    if(myCols.last == "") {
      myCols.map(x => "CASE WHEN 0 > 1 THEN '" + x + "_DATA_TYPE_CHK_FAIL;' ELSE '' END").mkString(",")   
    }
    else {
      val srcCols = myCols.map(x => (x.split("~")(0).trim(),x.split("~")(1).trim())).toSet
      
      val myColsList = myCols.map(x => (x.split("~")(0))).mkString(",")
      val flds = sparkSession.sql("SELECT " + myColsList + " FROM " + DataSources.HIVE_DB_NAME + DQTblNM).schema.fields
      val tgtCols = flds.map(x => (x.name,x.dataType)).map(x => (x._1.trim(),x._2.toString.trim().toUpperCase)).toSet
      
      if((srcCols.diff(tgtCols).mkString).isEmpty) {
        ""
      }
      else {
        "'" + srcCols.diff(tgtCols).map(x => x._1 + "_DATA_TYPE_CHK_FAIL;").mkString + "'"
      }
      
    }
  }
  
  //RULE-6 : DATA COMPLETENESS CHECK
  def buildDQCompletenessChkColumnList(myCols: Set[String]): String = {
    if(myCols.last == "") {
      myCols.map(x => "CASE WHEN 0 > 1 THEN '" + x + "_DQ_COMPLETENESS_CHK_FAIL;' ELSE '' END").mkString(",")
    }
    else if(myCols.size == 1) {
      myCols.map(x => "CASE WHEN " + x + " IS NULL THEN '" + x + "_DQ_COMPLETENESS_CHK_FAIL;' ELSE '' END").mkString(",")
    }
    else {
      var completeMandateColsStr = myCols.map(x => x + " IS NULL ").mkString(",").replaceAll(",","AND ")
      "CASE WHEN " + completeMandateColsStr + "THEN 'DQ_COMPLETENESS_CHK_FAIL;' ELSE '' END"
    }
    
  }
  
  //RULE-7 : DATA CONSISTENCY CHECK
  def buildConsistencyChkColumnList(myCols: Set[String]): String = {
    if(myCols.last == "") {
      myCols.map(x => "CASE WHEN 0 > 1 THEN '" + x + "_DATA_CONSISTENCY_CHK_FAIL;' ELSE '' END").mkString(",")   
    }
    else {
      myCols.map(x => "CASE WHEN ( (MAX("+x.split("~")(0)+") OVER(PARTITION BY "+x.split("~")(0)+" ORDER BY "+x.split("~")(1)+") ) != (MIN("+x.split("~")(0)+") OVER(PARTITION BY "+x.split("~")(0)+" ORDER BY "+x.split("~")(1)+") ) THEN '" +x.split("~")(0) + "_DATA_CONSISTENCY_CHK_FAIL;' ELSE '' END" ).mkString(",") 
    }
    
  }
  
  //RULE-8 : DATA CURRENCY TIMELINESS CHECK
  def buildCurrencyTimelinessChkColumnList(myCols: Set[String]): String = {
    if(myCols.last == "") {
      myCols.map(x => "CASE WHEN 0 > 1 THEN '" + x + "_DATA_CONSISTENCY_CHK_FAIL;' ELSE '' END").mkString(",")   
    }
    else {
      myCols.map(x => "CASE WHEN ( (MAX(abs(datediff("+x.split("~")(1)+",current_date))) OVER(PARTITION BY "+x.split("~")(0)+")) > "+x.split("~")(2)+" THEN '" +x.split("~")(0) + "_DATA_CURRENCY_TIMELINESS_CHK_FAIL;' ELSE '' END" ).mkString(",")
    }
  }
  
  
  //RULE-9 : PAI COMPLIANCE CHECK
  def buildPAIComplianceChkColumnList(myCols: Set[String]): String = {
    if(myCols.last == "") {
      myCols.map(x => "CASE WHEN 0 > 1 THEN '" + x + "_DATA_CONSISTENCY_CHK_FAIL;' ELSE '' END").mkString(",")   
    }
    else {
      myCols.map(x => "CASE WHEN " + CardUtils.cardValiditychk(x.toLong) + " = false THEN '" + x + "_PAI_COMPLIANCE_CHK_FAIL;' ELSE '' END" ).mkString(",")
    }
  }
  
  
}