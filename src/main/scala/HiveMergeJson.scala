package sparktest

import org.rogach.scallop._
import org.apache.spark.sql.{SparkSession, SaveMode}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val tableName = opt[String](required = true)
  val updateFilePath = opt[String](required = true)
  val uniqueKey = opt[String](required = true)
  val orderColumn = opt[String](required = true)
  verify()
}

object HiveMergeJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("spark-json-merge")
      .master("yarn")
      .getOrCreate()

    val conf = new Conf(args)

    val mergeTableName = conf.tableName() + "_merge"

    val currentTable = spark.sql("SELECT * FROM " + conf.tableName())

    val jsonUpdate = spark.read.json(conf.updateFilePath())
    val jsonUpdateLower = jsonUpdate.toDF(jsonUpdate.columns map(_.toLowerCase): _*)

    val uniqueKeys = conf.uniqueKey().split(",").map(_.trim).mkString(", ")
    val primaryKey = Seq(s"$uniqueKeys")
    val timestampColumn = conf.orderColumn()

    val selectColumns = currentTable.columns
    val jsonUpdateLowerOrdered = jsonUpdateLower.select(selectColumns.head, selectColumns.tail: _*)

    val records = currentTable.union(jsonUpdateLowerOrdered)

    MergeDriver.merge(primaryKey, timestampColumn, records)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"$mergeTableName")

    spark.stop()
  }
}
