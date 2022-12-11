package dqs.ccdri.workflow.utils

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import java.sql.Connection
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SparkSession

/**
 * A generic class for boot-strapping the SparkContext for Extract Transform Load implementations 
 * Holds connections and property file values
 * @author Somnath
 */

object DataSources extends Serializable {

  //spark
  val spark = SparkSession.builder()
              .config("hive.auto.convert.join", "true")
              .enableHiveSupport()
              .getOrCreate()
  val sparkConf = spark.conf
  val JOB_NAME = sparkConf.get("spark.app.name")
  //sparkConf.set("spark.app.name",JOB_NAME)    //Handled in Spark Submit

  val sparkContext = spark.sparkContext
  val sqlContext = spark.sqlContext
  val hiveContext = spark
  val appID = sparkContext.applicationId
  println("Application ID = " + appID)
  

  val whoami = System.getProperty("user.name")
  
  //DB2 TO HIVE PPQ SYNCHUP
  val db2TohivePPQSynchup= sparkConf.get("spark.wf.db2.hive.ppq.synchup.flg","false").toBoolean
  val hivePPQTable= sparkConf.get("spark.wf.hive.ppq.tbl","CDS_OB_TCOSUB_POSTPROCESS_QUEUE")
  
  // hive
  val HIVE_DRIVER = sparkConf.get("spark.hive.driver")
  val HIVE_DEV_URL = sparkConf.get("spark.hive.dev.url")
  val HIVE_ANA_URL = sparkConf.get("spark.hive.ana.url")


  // db2
  val DB2_DRIVER = sparkConf.get("spark.db2.driver")
  val DB2_URL = sparkConf.get("spark.db2.url")
  val DB2_USER = sparkConf.get("spark.db2.user")
  //val DB2_PASSWORD = EncryptionTool.decrypt(sparkConf.get("spark.db2.pass"))
  val DB2_PASSWORD = ""
  val DB2_SCHEMA = sparkConf.get("spark.db2.schema") + "."
  val DB2_GRANT_TARGET = sparkConf.get("spark.db2.grant.target.user")
  def db2Proprties = {
    val prop = new Properties
    prop.put("user", DB2_USER)
    prop.put("password", DB2_PASSWORD)
    prop
  }

  val HIVE_DB_NAME = sparkConf.get("spark.hive.db.name") + "."

  //DB2-DF
  val db2Context = sqlContext.read.format("jdbc").option("driver", DB2_DRIVER).option("url", DB2_URL).option("user", DB2_USER).option("password", DB2_PASSWORD)
  def dfFromdb2(str: String): DataFrame={db2Context.option("dbtable", s"($str) as test1").load()}


  def db2Connection = {
    Class.forName(DataSources.DB2_DRIVER)
    var connection:Connection = null
    try {
      connection = DriverManager.getConnection(DataSources.DB2_URL, DataSources.DB2_USER, DataSources.DB2_PASSWORD)
      connection.setAutoCommit(true)
    } catch {
      case e:Exception =>
    }
    connection
  }
  
  /**
   * Final CSV Report to be written into Target Path 
   */
  val TARGET_DQS_REPORT_PATH = sparkConf.get("spark.wf.target.rpt.path","")
  
  val WF_ISDEBUG = true
  
}
