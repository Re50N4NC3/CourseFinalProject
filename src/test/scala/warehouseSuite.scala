import main.main
import org.junit._


class warehouseSuite {
  def initializeWarehouse(): Boolean =
    try {
      _root_.main.main
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  import main._
  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  // TODO using @Before to generate the data frames used many times?
  /*trait TestUtils extends FunSuite with BeforeAndAfterAll with Matchers  {
    val sparkTestSession = SparkSession.builder().getOrCreate()
  }

class SparkAppTest extends TestUtils {

    test("read JSON") {
        val path = "/my/test.json"
        val expectedOutput = List(...)
        SparkExampleJob.getRecords(path, sparkTestSession).collect.toList should equal expectedOutput
    }
}
*/

  @Test def`'currentAmountForEachPosition' should return data frame with current amounts of products on each position`(): Unit ={
    val po = Seq(
      (1, "W-1", "P-1", 1528463098),
      (2, "W-1", "P-2", 1528463100))
      .toDF("positionId", "warehouse", "product", "eventTime")

    val am = Seq(
      (1, 10.00, 1528463098),
      (1, 10.20, 1528463008))
      .toDF("positionId", "amount", "eventTime")

    val re = currentAmountForEachPosition(po, am)
    val eq = Seq((1, "W-1", "P-1" ,10.00)).toDF("positionId", "warehouse", "product", "amount")

    assert(re.except(eq).count() == 0)
  }

  @Test def`'warehouseProductsStats' should return data frame with max, min and avg amounts of each product`(): Unit ={
    val po = Seq(
      (1, "W-1", "P-1", 1528463098),
      (2, "W-1", "P-2", 1528463100),
      (3, "W-2", "P-3", 1528463110),
      (4, "W-2", "P-4", 1528463111))
      .toDF("positionId", "warehouse", "product", "eventTime")

    val am = Seq(
      (1, 10.00, 1528463098),
      (1, 10.20, 1528463008),
      (2, 5.00, 1528463100),
      (3, 4.90, 1528463100),
      (3, 5.50, 1528463111),
      (3, 5.00, 1528463105),
      (4, 99.99, 1528463111),
      (4, 99.57, 1528463112))
      .toDF("positionId", "amount", "eventTime")

    val re = warehouseProductsStats(po, am)
    val eq = Seq(
      ("W-1", "P-1" ,10.2, 10.0, 10.1),
      ("W-1", "P-2" ,5.0, 5.0, 5.0),
      ("W-2", "P-3" ,5.5, 4.9, 5.13),
      ("W-2", "P-4" ,99.99, 99.57, 99.78)
    )
      .toDF("warehouse", "product", "max", "min", "avg")

    assert(re.except(eq).count() == 0)

  }

}
