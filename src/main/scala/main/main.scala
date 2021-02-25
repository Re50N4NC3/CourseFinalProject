package main

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, LongType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Main class */
object main {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Final1")
      .master("local")
      .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /** Main function */
  def main(args: Array[String]): Unit = {
    val positions: DataFrame = read("/Users/dchadala/IdeaProjects/CourseFinalProject1/positions.csv")
    val positionsC: DataFrame = positionsCast(positions)

    val amounts: DataFrame = read("/Users/dchadala/IdeaProjects/CourseFinalProject1/amounts.csv")
    val amountsC: DataFrame = amountsCast(amounts)

    val curAmount: DataFrame = currentAmountForEachPosition(positionsC, amountsC)
    val productAmount: DataFrame = warehouseProductsStats(positionsC, amountsC)

    curAmount.show()
    curAmount.printSchema()

    productAmount.show()
    productAmount.printSchema()
  }

  /**
   * @param path Path to csv file with headers
   * @return Read Data Frame with column headers
   */
  def read(path: String): DataFrame = {
    spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(path)
  }

  /**
   * @param positions Data Frame of warehouse positions
   * @return Data Frame of warehouse positions with required casted types
   */
  def positionsCast(positions: DataFrame): DataFrame = {
    positions
      .withColumn("positionId", col("positionId").cast(LongType))
      .withColumn("warehouse", col("warehouse").cast(StringType))
      .withColumn("product", col("product").cast(StringType))
      .withColumn("eventTime", col("eventTime").cast(TimestampType))
  }

  /**
   * @param amounts Data Frame of product amounts
   * @return Data Frame of product amounts with required casted types
   */
  def amountsCast(amounts: DataFrame): DataFrame = {
    amounts
      .withColumn("positionId", col("positionId").cast(LongType))
      .withColumn("amount", col("amount").cast(FloatType))
      .withColumn("eventTime", col("eventTime").cast(TimestampType))
  }

  /**
   * @param positions Data Frame of warehouse positions
   * @param amounts   Data Frame of product amounts
   * @return Data Frame with amount of products on each position
   */
  def currentAmountForEachPosition(positions: DataFrame, amounts: DataFrame): DataFrame = {
    val pos = positions.select("positionId", "warehouse", "product")
    val am = amounts
      .groupBy("positionId")
      .agg(max("eventTime") as "eventTime", first("amount") as "amount")
      .select("positionId", "amount")

    pos.join(am, Seq("positionId")).sort("positionId")
  }

  /**
   * @param positions Data Frame of warehouse positions
   * @param amounts   Data Frame of product amounts
   * @return Data Frame with max, min and avg amounts of products in each warehouse in each position
   */
  def warehouseProductsStats(positions: DataFrame, amounts: DataFrame): DataFrame = {
    val stats = positions
      .join(amounts, Seq("positionId"))
      .select("positionId", "warehouse", "product", "amount")
      .groupBy("positionId")
      .agg(
        max("amount").as("max"),
        min("amount").as("min"),
        round(avg("amount"), 2).as("avg")
      )
      .orderBy("positionId")

    val warehouse = positions.drop("eventTime")

    warehouse.join(stats, Seq("positionId")).drop("positionId")
  }
}
