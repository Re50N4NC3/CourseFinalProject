import main.twitter
import org.apache.spark.sql.DataFrame
import org.junit._

class twitterSuite{
  def initializeTwitter(): Boolean =
    try {
      twitter
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  import twitter._
  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  val retweets: DataFrame = spark.read.format("avro").load("RETWEET.avro")
  val messages: DataFrame = spark.read.format("avro").load("MESSAGE.avro")
  val userDir: DataFrame = spark.read.format("avro").load("USER_DIR.avro")
  val messageDir: DataFrame = spark.read.format("avro").load("MESSAGE_DIR.avro")

  @Test def`'findRetweets' should return specific user retweets for a specific message`(): Unit ={
    assert(findRetweets(retweets, 11, 1, userDir, messageDir) == (1, "Robert", "Smith", 11, "text11", 4))
    assert(findRetweets(retweets, 12, 2, userDir, messageDir) == (2, "John", "Johnson", 12, "text12", 1))
    assert(findRetweets(retweets, 13, 3, userDir, messageDir) == (3, "Alex", "Jones", 13, "text13", 1))
  }

  @Test def`'findFirstRetweets' should return rows of first retweets for a specific message of a user`(): Unit ={
    val re = findFirstRetweets(retweets, 11, 1)
    val eq = Seq((1, 2 ,11), (1, 3, 11)).toDF("USER_ID", "SUBSCRIBER_ID", "MESSAGE_ID")

    assert(re.except(eq).count() == 0)
  }

  @Test def`'findSecondRetweets' should return rows of second retweets for a list of specified subscribers`(): Unit ={
    val re = findSecondRetweets(retweets, List(2, 3), 1)
    val eq = Seq((2, 5 ,11), (3, 7, 11)).toDF("USER_ID", "SUBSCRIBER_ID", "MESSAGE_ID")

    assert(re.except(eq).count() == 0)
  }

  @Test def`'getSubscribers' should return list of subscribers from data frame of retweets`(): Unit ={
    val re = getSubscribers(
      Seq((2, 5 ,11), (3, 7, 11))
        .toDF("USER_ID", "SUBSCRIBER_ID", "MESSAGE_ID")
    )

    val eq = List(5, 7)

    assert(re == eq)
  }

  @Test def`'dfToString' should return string from specified cell in data frame`(): Unit ={
    val df = Seq((1, "Robert", "Smith"), (2, "John", "Johnson"), (3, "Alex", "Jones"))
      .toDF("USER_ID", "FIRST_NAME", "LAST_NAME")

    val re = dfToString(df, "USER_ID", 3, "FIRST_NAME")

    val eq = "Alex"

    assert(re == eq)
  }
}
