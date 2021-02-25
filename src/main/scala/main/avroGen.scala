package main

import com.sksamuel.avro4s.AvroOutputStream
import org.apache.spark.sql.SparkSession

import java.io.File

/** Main class */
object avroGen {

  /** Main function */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    case class USER_DIR(USER_ID: Int, FIRST_NAME: String, LAST_NAME: String)
    case class MESSAGE_DIR(MESSAGE_ID: Int, TEXT: String)
    case class MESSAGE(USER_ID: Int, MESSAGE_ID: Int)
    case class RETWEET(USER_ID: Int, SUBSCRIBER_ID: Int, MESSAGE_ID: Int)

    val usrDir = Seq(
      USER_DIR(1, "Robert", "Smith"),
      USER_DIR(2, "John", "Johnson"),
      USER_DIR(3, "Alex", "Jones")
    )

    val messDir = Seq(
      MESSAGE_DIR(11, "text11"),
      MESSAGE_DIR(12, "text12"),
      MESSAGE_DIR(13, "text13")
    )

    val mess = Seq(
      MESSAGE(1, 11),
      MESSAGE(2, 12),
      MESSAGE(3, 13)
    )

    val ret = Seq(
      RETWEET(1, 2, 11),
      RETWEET(1, 3, 11),
      RETWEET(2, 5, 11),
      RETWEET(3, 7, 11),
      RETWEET(7, 14, 11),
      RETWEET(5, 33, 11),
      RETWEET(2, 4, 12),
      RETWEET(3, 8, 13)
    )

    // is this preferred maybe?
    ///*
    val df = spark.createDataFrame(
      Seq(
        (1, "Robert", "Smith"),
        (2, "John", "Johnson"),
        (3, "Alex", "Jones")
      )
    ).toDF("USER_ID", "FIRST_NAME", "LAST_NAME")

    df.toDF.write.format("avro").partitionBy("USER_ID", "LAST_NAME").save("/tmp/exOut")
     //*////////////////////////////

    val osUsr = AvroOutputStream.data[USER_DIR].to(new File("USER_DIR.avro")).build()
    osUsr.write(usrDir)
    osUsr.flush()
    osUsr.close()

    val osMessDir = AvroOutputStream.data[MESSAGE_DIR].to(new File("MESSAGE_DIR.avro")).build()
    osMessDir.write(messDir)
    osMessDir.flush()
    osMessDir.close()

    val osMess = AvroOutputStream.data[MESSAGE].to(new File("MESSAGE.avro")).build()
    osMess.write(mess)
    osMess.flush()
    osMess.close()

    val osRet = AvroOutputStream.data[RETWEET].to(new File("RETWEET.avro")).build()
    osRet.write(ret)
    osRet.flush()
    osRet.close()

    //saveAvro(AvroOutputStream.data[USER_DIR], usrDir, "USER_DIR")

  }

  /*
  def saveAvro(builder: AvroOutputStreamBuilder[_], dataSeq: Seq[_], saveName: String): Unit = {
    val os = builder.to(new File(saveName + ".avro")).build()
    os.write(dataSeq)
    os.flush()
    os.close()
  }
  */
}
