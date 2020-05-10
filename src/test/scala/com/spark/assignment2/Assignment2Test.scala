package com.spark.assignment2

import com.spark.assignment2.Assignment2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import org.scalatest.BeforeAndAfterEach

import scala.concurrent.duration._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class Assignment2Test extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  /**
   * Set this value to 'true' to halt after execution so you can view the Spark UI at localhost:4040.
   * NOTE: If you use this, you must terminate your test manually.
   * OTHER NOTE: You should only use this if you run a test individually.
   */
  val BLOCK_ON_COMPLETION = false;

  /**
   * Set this value to 'true' if the parquet files need to be written
   */
  val GENERATE_PARQUET = false;

  // Paths to dour data.
  val SERVICE_REQUEST_DATA_CSV_PATH = "data/311_Service_Requests_from_2010_to_Present.csv"
  val TREE_DATA_CSV_PATH = "data/2015_Street_Tree_Census_-_Tree_Data.csv"
  val SERVICE_REQUEST_DATA_PARQUET_PATH = "data/parquet/service"
  val TREE_DATA_PARQUET_PATH = "data/parquet/tree"
  /**
   * Create a SparkSession that runs locally on our laptop.
   */
  val spark =
    SparkSession
      .builder()
      .appName("Assignment 1")
      .master("local[*]") // Spark runs in 'local' mode using all cores
      .getOrCreate()

  if (GENERATE_PARQUET) {
    /**
     * Encoders to assist converting a csv records into Case Classes.
     * They are 'implicit', meaning they will be picked up by implicit arguments,
     * which are hidden from view but automatically applied.
     */
    implicit val treeEncoder: Encoder[Tree] = Encoders.product[Tree]
    implicit val serviceEncoder: Encoder[service_request] = Encoders.product[service_request]

    /**
     * Let Spark infer the data types. Tell Spark this CSV has a header line.
     */
    val csvReadOptions =
      Map("inferSchema" -> true.toString, "header" -> true.toString)

    /**
     * Create Trip Spark collections
     */
    def treeDataDS: Dataset[Tree] = spark.read.options(csvReadOptions).csv(TREE_DATA_CSV_PATH).as[Tree]

    def treeDataDF: DataFrame = treeDataDS.toDF()

    treeDataDF.write.parquet(TREE_DATA_PARQUET_PATH)

    /**
     * Create service request Spark collections from CSV then write the parquet files
     */
    def serviceDataDS: Dataset[service_request] = spark.read.options(csvReadOptions).csv(SERVICE_REQUEST_DATA_CSV_PATH).as[service_request]

    def serviceDataDF: DataFrame = serviceDataDS.toDF()

    serviceDataDF.write.parquet(SERVICE_REQUEST_DATA_PARQUET_PATH)
  }

  /**
   * Read data from parquet into dataframes
   */

  def treeDF = spark.read.parquet(TREE_DATA_PARQUET_PATH)

  def serviceDF = spark.read.parquet(SERVICE_REQUEST_DATA_PARQUET_PATH)

  /**
   * Keep the Spark Context running so the Spark UI can be viewed after the test has completed.
   * This is enabled by setting `BLOCK_ON_COMPLETION = true` above.
   */
  override def afterEach: Unit = {
    if (BLOCK_ON_COMPLETION) {
      // open SparkUI at http://localhost:4040
      Thread.sleep(5.minutes.toMillis)
    }
  }

  /**
   * Which department received the most 311 calls in the past year?
   */
  test("Department with the most calls") {
    Assignment2.problem1(serviceDF) must equal("New York City Police Department")
  }

  /**
   * What was the most common complaint type?
   */
  test("Most common complaint") {
    Assignment2.problem2(serviceDF) must equal("DOF Property - Reduction Issue")
  }

  /**
   * Who (which address) complained the most?
   * Interesting note: most of the complaints are for heat/hot water
   * at the apartment complex, 2176 TIEBOUT AVENUE
   */
  test("Biggest whiner") {
    Assignment2.problem3(serviceDF) must equal("2176 TIEBOUT AVENUE")
  }

  /**
   * What is the average number complaints received by zipcode?
   */
  test("Average by zipcode") {
    Assignment2.problem4(serviceDF) must be(2509.0 +- .5)
  }

  /**
   * What percentage of tree data was collected by volunteers?
   **/
  test("Percent of tree data from volunteers") {
    Assignment2.problem5(treeDF) must be(0.318 +- .0005)
  }

  /**
   * How many trees are there in the zipcode with the most trees?
   */
  test("Trees per zipcode") {
    Assignment2.problem6(treeDF) must equal(22186)
  }

  /**
   * How many trees are there in the zipcode with the most complaints?
   */
  test("Number of trees in the zipcode with most complaints") {
    Assignment2.problem7(treeDF,serviceDF) must equal(3875)
  }
}