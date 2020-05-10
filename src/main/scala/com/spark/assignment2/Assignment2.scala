package com.spark.assignment2

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Assignment2 {

  /**
   * Which department received the most 311 calls?
   */
  def problem1(services: DataFrame): String = {
    /*group by the agency name and count by group*/
    val departmentCount = services.groupBy("agency_name").count()
    /*sort in descending order*/
    val departmentSort = departmentCount.sort(desc("count"))
    /*get just the agency name with the max count*/
    departmentSort.select("agency_name").first().getString(0)
  }

  /**
   * What was the most common complaint type?
   */
  def problem2(services: DataFrame): String = {
    /*group by the agency name and count by group*/
    val complaintCount = services.groupBy("complaint_type").count()
    /*sort in descending order*/
    val complaintSort = complaintCount.sort(desc("count"))
    /*get just the complaint_type with the max count*/
    complaintSort.select("complaint_type").first().getString(0)
  }

  /**
   * Who (which address) complained the most?
   */
  def problem3(services: DataFrame): String = {
    /*group by the incident_address and count by group*/
    val complaintCount = services.groupBy("incident_address").count()
    /*filter out the null values*/
    val complaintFilter = complaintCount.filter(complaintCount("incident_address").isNotNull)
    /*sort in descending order*/
    val complaintSort = complaintFilter.sort(desc("count"))
    /*get just the incident_address with the max complaints*/
    complaintSort.select("incident_address").first().getString(0)
  }

  /**
   * What is the average number complaints received by zipcode?
   */
  def problem4(services: DataFrame): Double = {
    /*group by the incident_zip and count by group*/
    val complaintCount = services.groupBy("incident_zip").count()
    /*select the average of the 'count' column and return the double value*/
    complaintCount.select(avg("count")).first().getDouble(0)
  }

  /**
   * What percentage of tree data was collected by volunteers?
   **/
  def problem5(trees: DataFrame): Double = {
    /*group by the user_type and count by group*/
    val treeCollectors = trees.groupBy("user_type").count()
    /*filter the dataframe for just the volunteers*/
    val tVolunteer = treeCollectors.filter(treeCollectors("user_type") === "Volunteer")
    /*count the total number of volunteers*/
    val tVolunteerCount = tVolunteer.select("count").first().getLong(0)
    /*count the total number of people collecting data*/
    val collectorSum = treeCollectors.agg(sum("count")).first().getLong(0)
    /*return the percent of data from volunteers*/
    tVolunteerCount.toDouble/collectorSum.toDouble
  }

  /**
   * How many trees are there in the zipcode with the most trees?
   */
  def problem6(trees: DataFrame): Long = {
    /*group by the agency name and count by group*/
    val treeCount = trees.groupBy("postcode").count()
    /*sort in descending order*/
    val treeSort = treeCount.sort(desc("count"))
    /*get just the number of trees with the max count*/
    treeSort.select("count").first().getLong(0)
  }

  /**
   * How many trees are there in the zipcode with the most complaints?
   */
  def problem7(trees: DataFrame,services: DataFrame): Long = {
    /*group by the incident_zip and count by group then rename the 'count'*/
    val complaintCount = services.groupBy("incident_zip").count().withColumnRenamed("count","complaintCount")
    /*group by the postcode and count by group then rename the 'count'*/
    val treeCount = trees.groupBy("postcode").count().withColumnRenamed("count","treeCount")
    /*join the two counts together by their zipcode*/
    val joinedCount = complaintCount.join(treeCount,col("incident_zip") === col("postcode"))
    /*sort in descending order*/
    val sortedJoin = joinedCount.sort(desc("complaintCount"))
    /*get just the tree count at the top of the sorted dataframe*/
    sortedJoin.select("treeCount").first().getLong(0)
  }

}
