package sparktest

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class Record(field1: String, field2: String, year: Int, lastUpdated: Long)


object MergeDriver {

  def mergeSql[A](sqlContext: SQLContext,
                  originalRecords: Dataset[A],
                  updates: Dataset[A]): DataFrame = {

    originalRecords.createOrReplaceTempView("originals")
    updates.createOrReplaceTempView("updates")

    originalRecords.show()
    updates.show()

    sqlContext.sql(
      """
        |SELECT full.* FROM
        |
        |(SELECT * FROM originals
        |
        |    UNION ALL
        |
        |    SELECT * FROM updates) full
        |
        |JOIN
        |
        |    (SELECT field1, field2, max(lastUpdated) max_modified FROM
        |
        |        (SELECT * FROM originals
        |
        |        UNION ALL
        |
        |        SELECT * FROM updates) t2
        |
        |    GROUP BY field1, field2) grouped
        |
        |ON full.field1 = grouped.field1 AND full.field2 = grouped.field2 AND full.modified_date = grouped.max_modified
      """.stripMargin)
  }

  // can group by multiple keys by appending them
  def mergeRddGroupBy(keys: String, timestampColumn: String, records: RDD[Record]) = {
    val grouped = records.groupBy(x => x.field1)

    val maxRow = grouped.map {
      case (key, rows) =>
        rows.reduce { (l, r) =>
          if (l.lastUpdated > r.lastUpdated) l else r
        }
    }

    maxRow
  }

  def createDataPartitions(tableName: String, partitionColumn: String, records: DataFrame) = {
    records.createGlobalTempView(tableName)
    records.write.partitionBy(partitionColumn).mode(SaveMode.Overwrite).saveAsTable(tableName)
  }

  def updatePartitions(
                        sparkSession: SparkSession,
                        tableName: String,
                        keys: Seq[String],
                        partitionColumn: String,
                        timestampColumn: String,
                        originalRecords: Dataset[_],
                        updates: Dataset[_]) = {

    val tmpPath = s"/tmp/${UUID.randomUUID()}"

    val latest = merge(keys, timestampColumn, updates)

    val keysColumns = keys.map(key => latest(key))

    val partitions = latest.groupBy(partitionColumn)
      .agg(latest(partitionColumn)).as(partitionColumn)
      .collect()
      .map(_.getInt(0))

    // get only the partitions that have updates from the original dataset
    val parititionedOriginalRecords =
      originalRecords
        .filter(originalRecords(partitionColumn).isin(partitions: _*))
        .write.save(tmpPath)

    partitions.foreach { partition =>
      sparkSession.sql(s"""ALTER TABLE $tableName DROP PARTITION (year=$partition)""")
    }

    val originalRecordsCache = sparkSession.read.parquet(tmpPath)

    val unioned = originalRecordsCache.union(latest)
    println("unioned")

    val mergedRecords = merge(keys, timestampColumn, unioned).cache()
    mergedRecords.write.partitionBy(partitionColumn).mode(SaveMode.Append).saveAsTable(tableName)
  }

  def merge(keys: Seq[String], timestampColumn: String, records: Dataset[_]) = {
    val keysColumns = keys.map(key => records(key))

    val groupedMax = records.groupBy(keysColumns: _*)
      .agg(
        max(timestampColumn).as(timestampColumn)
      )

    groupedMax.join(records, keys :+ timestampColumn)
  }
}
