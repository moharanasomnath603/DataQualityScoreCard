package dqs.ccdri.workflow.utils

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Calendar

//import akka.actor.Status.Success
import org.joda.time.{DateTime, Period}

import scala.util.Try

/**
 * Created by Somnath on 05/28/2019.
 */
object DateUtil {

  /** add days to a given date
    *
    * @param date a date in format of "YYYY-MM-DD"
    * @param days number of days
    * @return a String of new date
    */
  def addDaysToGivenDate(date:String, days:Int): String ={
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val dt = format.parse(date)
    val cal = Calendar.getInstance()
    cal.setTime(dt)
    cal.add(Calendar.DATE, days)
    val start_date = cal.getTime
    val result = format.format(start_date)
    result
  }

  def addDaysToGivenDateInTimestampFormat(date:String, days:Int): String ={
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = format.parse(date)
    val cal = Calendar.getInstance()
    cal.setTime(dt)
    cal.add(Calendar.DATE, days)
    val start_date = cal.getTime
    val result = format.format(start_date)
    result
  }

  /** add days to a given date
    *
    * @param date a date in format of "YYYY-MM-DD"
    * @param months number of months
    * @return a String of new date in format of "YYYY-MM-DD"
    */
  def addMonthsToGivenDate(date:String, months: Int):String =  {
    val cal = Calendar.getInstance()
    cal.setTime(Date.valueOf(date))
    cal.add(Calendar.MONTH, months); //minus number would decrement the days
    val start_date = cal.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd")
    val history = timeFormat.format(start_date)
    history.toString
  }


  def addDaysToCurrentDateInHourFormat(days:Int): String ={
    val cal = Calendar.getInstance()
    cal.setTime(Calendar.getInstance.getTime)
    cal.add(Calendar.DATE, days)
    val start_date = cal.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val history = timeFormat.format(start_date)
    history.toString
  }

  /** add days to today
    *
    * @param days number of days
    * @return a String of new date in format of "YYYY-MM-DD"
    */
  def addDaysToCurrentDate(days:Int): String ={
    val cal = Calendar.getInstance()
    cal.setTime(Calendar.getInstance.getTime)
    cal.add(Calendar.DATE, days)
    val start_date = cal.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd")
    val history = timeFormat.format(start_date)
    history.toString
  }

  /** add months to today
    *
    * @param months number of months
    * @return a String of new date in format of "YYYY-MM-DD"
    */
  def addMonthsToCurrentDate(months: Int):String =  {
    val cal = Calendar.getInstance()
    cal.setTime(Calendar.getInstance.getTime)
    cal.add(Calendar.MONTH, months); //minus number would decrement the days
    val start_date = cal.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd")
    val history = timeFormat.format(start_date)
    history.toString
  }


  /** add months to today
    *
    * @param months number of months
    * @return a String of new date in format of "YYYYMMDD"
    */
  def addMonthsToCurrentDateFileDate(months: Int):String = {
    val history_dt = addMonthsToCurrentDate(months)
    val dt = new SimpleDateFormat("yyyy-MM-dd").parse(history_dt)
    val history_filedate = new SimpleDateFormat("yyyyMMdd").format(dt)
    history_filedate
  }

  /** Get todays date
    *
    * @return a String of today's date in format of "yyyy-MM-dd HH:mm:ss"
    */
  def getDate: String = {
    val today = Calendar.getInstance.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now = timeFormat.format(today)
    now.toString
  }

  /** Get todays date
    *
    * @return a String of today's date in format of "yyyy-MM-dd HH:mm:ss.SSSSSS"
    */
  def getDate_timeStampType: Timestamp = {
    val today: java.util.Date = Calendar.getInstance.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS")
    val now: String = timeFormat.format(today)
    //now
    val re = java.sql.Timestamp.valueOf(now)
    re
  }

  def getCurrentDate: Timestamp = {
    val today: java.util.Date = Calendar.getInstance.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd")
    val now: String = timeFormat.format(today)
    //now
    val re = java.sql.Timestamp.valueOf(now)
    re
  }

  /** Get todays date
    *
    * @return a String of today's date in format of "yyyy-MM-dd"
    */
  def getDate_dateType: java.sql.Date = {
    val sqlDate = new java.sql.Date(Calendar.getInstance().getTime().getTime())
    sqlDate
  }

  /** get today's date in given format
    *
    * @param fmt format
    * @return a date in new format
    */
  def getCurrentDateInFormat(fmt: String): String = {
    val format = fmt.replace("C", "Y").replace("D", "d").replace("Y", "y").replace("m", "M").replace("ddd", "DDD")
    val today = Calendar.getInstance.getTime
    val timeFormat = new SimpleDateFormat(format)
    val now = timeFormat.format(today)
    now.toString
  }

  /** transform a given date to a given format
    *
    * @param date a date in format of "YYYY-MM-DD"
    * @param fmt new format
    * @return a date in new format
    */
  def getDateInFormat(date:String, fmt: String): String = {
    val format = fmt.replace("C", "Y").replace("D", "d").replace("Y", "y").replace("m", "M").replace("ddd", "DDD")
    val timeFmt1 = new SimpleDateFormat("yyyy-MM-dd")
    val timeFmt2 = new SimpleDateFormat("MMddyyyy")
    val givenDate =
      Try(timeFmt1.parse(date)) match {
        case util.Success(a) => a
        case util.Failure(_) =>
          Try(timeFmt2.parse(date)) match {
            case util.Success(b) => b
            case util.Failure(_) => timeFmt1.parse("9999-01-01")
          }
      }

    val newtimeFormat = new SimpleDateFormat(format)
    newtimeFormat.format(givenDate)
  }

  def getDateNow: String = {
    val today = Calendar.getInstance.getTime
    val timeFormat = new SimpleDateFormat("yyMM")
    val now = timeFormat.format(today)
    now.toString
  }



  /** get List of date range from today
    *
    * @param minusMonths
    * @return
    */
  def getDateRangeFromToday(minusMonths: Int)={
    def dateRangeGenerator(from: DateTime, to: DateTime, step: Period): Iterator[DateTime] =Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))
    val endOfDay = new DateTime(Calendar.getInstance().getTime())
    val startOfDay = endOfDay.plusMonths(minusMonths)
    val dateRange = dateRangeGenerator(startOfDay, endOfDay, Period.days(1)).toList
    val now = dateRange.map(_.toString("YYYYMMdd"))
    now
  }

  /** get List of date range
    *
    * @param start_dt
    * @param end_dt
    * @return
    */
  def getDateRangeBtwnStartEndDt(start_dt: String, end_dt: String)={
    def dateRangeGenerator(from: DateTime, to: DateTime, step: Period): Iterator[DateTime] =Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))
    val originalFmt = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val start_dt_in_datetime = new DateTime(originalFmt.parse(start_dt))
    val end_dt_in_datetime = new DateTime(originalFmt.parse(end_dt))
    val dateRange = dateRangeGenerator(start_dt_in_datetime, end_dt_in_datetime, Period.days(1)).toList
    val now = dateRange.map(_.toString("YYYYMMdd"))
    now
  }

}
