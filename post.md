# Intro

If you do a search for doing incremental merges in Hadoop you probably come across links that promote a strategy in hive where each row is grouped by it's private key and the greatest timestamp is selected [https://hortonworks.com/blog/four-step-strategy-incremental-updates-hive/]

There are other strategies https://www.phdata.io/4-strategies-for-updating-hive-tables/


# Sql approach
Works for simple cases
Hard to wrap with error handling
The strategy looks like this:

```sql
CREATE VIEW reconcile_view AS

SELECT t1.* FROM

(SELECT * FROM base_table

    UNION ALL

    SELECT * FROM incremental_table) t1

JOIN

    (SELECT id, max(modified_date) max_modified FROM

        (SELECT * FROM base_table

        UNION ALL

        SELECT * FROM incremental_table) t2

    GROUP BY id) s

ON t1.id = s.id AND t1.modified_date = s.max_modified;

``` cite!

In short, 
- Union the existing records with the new records
- Group all rows by the private key
- Select the max date of all duplicate private keys
- join that key back to a union of the new records and existing records


There are a few downsides to the SQL approach. First, SQL isn't generic and you can't re-use it. For each table you want to merge, you would need to write the exact same query. Hive and Impala do support variable substitution, but the flexibility breaks down when you want have compound keys in your table.

## Using Spark
- You get to use the Catalyst query optimizer
- You get full error handling capabilities
- You get to perform side effects
- You get good control over logging and exception handling
- You can build useful abstractions

Here we will build a function with the interface:

```scala

def merge(keys: Seq[String], timestampColumn: String, records: Dataset[_]) 

```


This function takes three arguments
- A sequence of keys that will make up a primary key
- A timestamp that will be used to pick the latest record for a given primary key
- A single dataset. It's assumed that the old and new records have already been unioned before this function is called

The actual function will be implemented like this:

```scala

  def merge(keys: Seq[String], timestampColumn: String, records: Dataset[_]) = {
    val keysColumns = keys.map(key => records(key))

    val groupedMax = records.groupBy(keysColumns: _*).agg(max(timestampColumn).as(timestampColumn))

    groupedMax.join(records, keys :+ timestampColumn)
  }
  
```

First, the keys are mapped into Columns so they can be used with the Dataset API. 

Next the keys are grouped by the primary key. Each grouping could then have multiple records with different timestamps, so the `agg` function is used to select only the greatest timestamp. When doing the `groupBy` we've already selected the primary keys, so we have everything we need to join back to the full table and select all the records that match both the primary keys and latest timestamp. The result of this join is our final merged dataset.

# RDD approach
Groupbykey

# DataFrames
Get the power of SQL optimizations along with the abstractions of a programming language

- Unit tests

What about [Sqoop Merge](https://sqoop.apache.org/docs/1.4.0-incubating/SqoopUserGuide.html#id1770457)? Sqoop merge will do the same thing, but the limit is it will not handle compound keys. The performance of the Spark merge implementation will also likely be better since Sqoop merge is based on mapreduce and can't take advantage of columnar formats like Apache Parquet or other SQL optimizations that the Spark Catalyst Optimizer can.

window functions!	