package com.spark.assignment1

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Assignment1 {

  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("M/d/yyyy H:mm")

  /**
    * Helper function to print out the contents of an RDD
    * @param label Label for easy searching in logs
    * @param theRdd The RDD to be printed
    * @param limit Number of elements to print
    */
  private def printRdd[_](label: String, theRdd: RDD[_], limit: Integer = 20) = {
    val limitedSizeRdd = theRdd.take(limit)
    println(s"""$label ${limitedSizeRdd.toList.mkString(",")}""")
  }
  /**
   * We have to prove to the governor that our ride service is being used.
   * Find the total ride duration across all trips.
   */
  def problem1(tripData: RDD[Trip]): Long = {
    val maxDuration = tripData.map(d => d.duration).max()
    return maxDuration
  }
  /**
   * Find all trips starting at the 'San Antonio Shopping Center' station.
   */
  def problem2(trips: RDD[Trip]): Long = {
    val sascTrips = trips.filter(t => t.start_station == "San Antonio Shopping Center")
    return sascTrips.count()
  }
  /**
   * List out all the subscriber types from the 'trip' dataset.
   */
  def problem3(trips: RDD[Trip]): Seq[String] = {
    val subs = trips.map(t => t.subscriber_type)
    val subgroup = subs.distinct()
    return subgroup.collect()
  }
  /**
   * Find the zip code with the most rides taken.
   */
  def problem4(trips: RDD[Trip]): String = {
    val tt = trips.map(t => t.zip_code).countByValue().maxBy(t => t._2)
    return tt._1
  }
  /**
   * Some people keep their bikes for a long time. How many people keep their bikes overnight?
   */
  def problem5(trips: RDD[Trip]): Long = {
    val tripDates = trips.filter(t => parseTimestamp(t.start_date).getDayOfYear != parseTimestamp(t.end_date).getDayOfYear)
    return tripDates.count()
  }
  /**
   * What is the total number of records in the trips dataset?
   */
  def problem6(trips: RDD[Trip]): Long = {
    trips.count()
  }
  /**
   * What percentage of people keep their bikes overnight at least on night?
   */
  def problem7(trips: RDD[Trip]): Double = {
    val tripDates = trips.filter(t => parseTimestamp(t.start_date).getDayOfYear != parseTimestamp(t.end_date).getDayOfYear)
    return tripDates.count().toDouble / trips.count().toDouble
  }
  /**
   * Ope! The docks were miscalibrated and only counted half of a trip duration. Double the duration of each trip so
   * we can have an accurate measurement.
   */
  def problem8(trips: RDD[Trip]): Double = {
    val tripDuration = trips.map(t => t.duration*2)
    return tripDuration.sum()
  }
  /**
   * Find the coordinates (latitude and longitude) of the trip with the id 913401.
   */
  def problem9(trips: RDD[Trip], stations: RDD[Station]): (Double, Double) = {
    val tt = trips.filter(t=> t.trip_id == "913401")
    val ttStation = tt.map(t => t.start_station)
    val ttStationString = ttStation.first()
    val ss = stations.filter(s => s.name == ttStationString)
    val ssLat = ss.map(s => s.lat)
    val ssLon = ss.map(s => s.lon)
    val lat = ssLat.first()
    val lon = ssLon.first()
    return (lat,lon)  }
  /**
   * Find the duration of all trips by starting at each station.
   * To complete this you will need to join the Station and Trip RDDs.
   *
   * The result must be a Array of pairs Array[(String, Long)] where the String is the station name
   * and the Long is the summation.
   */
  def problem10(trips: RDD[Trip], stations: RDD[Station]): Array[(String, Long)] = {
    /*val tt = trips.map(t => (t.start_terminal,t.duration)).reduceByKey(_+_)
    val ss = stations.map(s => (s.station_id,s.name))
    val joinedSS = ss.join(tt).map(x => x._2)
    printRdd("joined",joinedSS)
    return joinedSS.collect()*/
    val tt = trips.map(t => (t.start_station,t.duration)).reduceByKey(_+_)
    return tt.collect()

    /* both methods above give 70 elements rather than the expected 68.
    This is because of the misspellings in the data Post at Kearny vs	Post at Kearney and
    Washington at Kearny	vs Washington at Kearney. I can't quite figure an easy way
    to filter out these spelling errors, but the other two assertions are correct with
    both methods of performing this query.
     */
  }

  /*
   Dataframes
   */

  /**
   * Select the 'trip_id' column
   */
  def dfProblem11(trips: DataFrame): DataFrame = {
    val tCol = trips.select("trip_id")
    return tCol
  }
  /**
   * Count all the trips starting at 'Harry Bridges Plaza (Ferry Building)'
   */
  def dfProblem12(trips: DataFrame): DataFrame = {
    val tHarry = trips.filter(trips("start_station") === "Harry Bridges Plaza (Ferry Building)")
    return tHarry
  }
  /**
   * Sum the duration of all trips
   */
  def dfProblem13(trips: DataFrame): Long = {
    val durSum = trips.agg(sum("duration")).first()
    return durSum.getLong(0)
  }

  // Helper function to parse the timestamp format used in the trip dataset.
  private def parseTimestamp(timestamp: String) = LocalDateTime.from(timestampFormat.parse(timestamp))
}
