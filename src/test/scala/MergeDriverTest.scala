package sparktest

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}


class MergeDriverTest extends FunSuite with BeforeAndAfterAll with Matchers {

  val record1 = Record("John", "Edwards", 2001, 100L)
  val record2 = Record("Bill", "Smith", 2001, 100L)
  val record2Update = Record("Bill", "Smith", 2001, 101L)
  val originals = Seq(record1, record2)
  val updates = Seq(record2Update)

  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .appName("spark-merge")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def afterAll = {
    spark.stop
  }

  // The entire pipeline can be tested
  ignore("SQL merge") {

    val result = MergeDriver.mergeSql(
      spark.sqlContext,
      spark.createDataFrame(originals),
      spark.createDataFrame(updates))
      .collect()

    result
      .should(contain)
      .only(record1, record2Update)
  }

  // The entire pipeline can be tested
  ignore("merge using rdd groupBy") {

    val originalRdd = spark.createDataset(originals).rdd
    val updatesRdd = spark.createDataset(updates).rdd
    // Spark does a 'union all' by default
    val allRecords = originalRdd.union(updatesRdd)

    val primaryKey = "field1"
    val timestampColumn = "lastUpdated"
    val result = MergeDriver.mergeRddGroupBy(
      primaryKey,
      timestampColumn,
      allRecords)
      .collect()

    result
      .should(contain)
      .only(record1, record2Update)
  }

  ignore("merge using dataframe groupedData") {

    val primaryKey = Seq("field1", "field2")
    val timestampColumn = "lastUpdated"

    val originalsDataframe = spark.createDataset(originals)
    val updatesDataframe = spark.createDataset(updates)

    val records = originalsDataframe.union(updatesDataframe)

    val result = MergeDriver
      .merge(primaryKey, timestampColumn, records)
      .as[Record].collect()

    result
      .should(contain)
      .only(record1, record2Update)
  }

  test("merge by upating partitions") {
    val primaryKey = Seq("field1", "field2")
    val timestampColumn = "lastUpdated"
    val tableName = "tname"
    val partitionColumn = "year"

    MergeDriver.createDataPartitions(tableName, partitionColumn, spark.createDataFrame(originals))

    val originalRecords = spark.read.table(tableName)

    MergeDriver.updatePartitions(spark, tableName, primaryKey, partitionColumn, timestampColumn, originalRecords.as[Record], spark.createDataFrame(updates).as[Record])

    val result = spark.read.table(tableName).as[Record].collect()

    result
      .should(contain)
      .only(record1, record2Update)
  }

}

