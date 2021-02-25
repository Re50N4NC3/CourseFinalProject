package main

import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions._

/** Main class */
object twitter {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Final2")
      .master("local")
      .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {
    val retweets: DataFrame = spark.read.format("avro").load("RETWEET.avro")
    val messages: DataFrame = spark.read.format("avro").load("MESSAGE.avro")
    val userDir: DataFrame = spark.read.format("avro").load("USER_DIR.avro")
    val messageDir: DataFrame = spark.read.format("avro").load("MESSAGE_DIR.avro")

    val userMessage: List[Int] = messages.select("USER_ID").collect().map(_ (0).toString.toInt).toList
    val messageId: List[Int] = messages.select("MESSAGE_ID").collect().map(_(0).toString.toInt).toList

    // there must be a better way
    val retweetRows = (userMessage zip messageId)
      .map { case (u, m) => findRetweets(retweets, m, u, userDir, messageDir) }

    val topUsers = retweetRows
      .toDF("USER_ID", "FIRST_NAME", "LAST_NAME", "MESSAGE_ID", "MESSAGE_TEXT", "RETWEET_AMOUNT")
      .orderBy(desc("RETWEET_AMOUNT"))
      .limit(10)

    topUsers.show()
    topUsers.write.format("avro").save("topUsers.avro")
  }

  /**
   *
   * @param retweets Dataframe with retweets
   * @param messId Id of message to check
   * @param userId Id of user to check
   * @param userDir Data frame of first and last names with Id's
   * @param messDir Data frame of tweets with Id's
   * @return Organized tuple of final row data
   */
  def findRetweets(retweets: DataFrame, messId: Int, userId: Int, userDir: DataFrame, messDir: DataFrame): (Int, String, String, Int, String, Long) = {
    // first and second wave retweets
    val firstRetweets = findFirstRetweets(retweets, messId, userId)
    val subscriberList: List[Int] = getSubscribers(firstRetweets)
    val secondRetweets: DataFrame = findSecondRetweets(retweets, subscriberList, messId)

    val retweetAmount: Long = firstRetweets.count() + secondRetweets.count()

    // rest of the needed data columns
    val firstName = dfToString(userDir, "USER_ID", userId, "FIRST_NAME")
    val lastName = dfToString(userDir, "USER_ID", userId, "LAST_NAME")
    val messText = dfToString(messDir, "MESSAGE_ID", messId, "TEXT")

    val finalRows = (userId, firstName, lastName, messId, messText, retweetAmount)

    finalRows
  }

  /**
   *
   * @param retweets Data frame containing retweets
   * @param messageId Currently checked message Id
   * @param userId Currently checked user Id
   * @return First time retweets rows
   */
  def findFirstRetweets(retweets: DataFrame, messageId: Int, userId: Int): DataFrame = {
    retweets
      .filter(retweets.col("MESSAGE_ID").equalTo(messageId))
      .filter(retweets.col("USER_ID").equalTo(userId))
  }

  /**
   *
   * @param retweets Data frame containing retweets
   * @param subscribers List of subscribers of user
   * @param messageId currently checked message Id
   * @return Second time retweets rows
   */
  def findSecondRetweets(retweets: DataFrame, subscribers: List[Int], messageId: Int): DataFrame = {
    val checkedMessages = retweets
      .filter(retweets.col("MESSAGE_ID").equalTo(messageId))

    checkedMessages.filter(col("USER_ID").isin(subscribers :_*))
  }

  /**
   *
   * @param retweetsDf Data frame of users retweets
   * @return List of subscribers in the data frame
   */
  def getSubscribers(retweetsDf: DataFrame): List[Int] = {
    retweetsDf.select("SUBSCRIBER_ID").collect().map(_(0).toString.toInt).toList
  }

  /**
   *
   * @param dir Table to pick from
   * @param colNameId Name of column to pick from
   * @param id Id of value to pick
   * @param colSelect Column to pick from
   * @return String from cell in data frame
   */
  def dfToString(dir: DataFrame, colNameId: String, id: Int, colSelect: String): String ={
    dir
      .filter(dir.col(colNameId).equalTo(id))
      .select(colSelect)
      .head()
      .getString(0)
  }
}
