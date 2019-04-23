package com.mpojeda84.mapr.scala

import com.mapr.db.spark.streaming.MapRDBSourceConfig
import com.mpojeda84.mapr.scala.config.Configuration
import org.apache.spark.sql.SparkSession
import com.mpojeda84.mapr.scala.helper.Helper

object Application {

  def main (args: Array[String]): Unit = {

    // reads parameters
    val argsConfiguration = Configuration.parse(args)

    // create application
    val spark = SparkSession.builder.appName("Car Data stream to table transformed").getOrCreate
    import spark.implicits._

    // create the structured stream
    val stream =
      spark
        .readStream
        .format("kafka")
        .option("failOnDataLoss", false)
        .option("kafka.bootstrap.servers", "none")
        .option("subscribe", argsConfiguration.topic)
        .option("startingOffsets", "earliest")
        .load()

    // transforms from csv to json with _id
    val documents = stream.select("value").as[String].map(Helper.toJsonWithId)

    // create a temp view to use SQL
    documents.createOrReplaceTempView("raw_data")

    // register UDF
    spark.udf.register("valueIfInLastXDays", Helper.valueIfInLastXDays)

    saveTransformed(spark,argsConfiguration)

  }

  // saves into a Mapr-DB table, grouped by VIN, and updates as new data comes in
  private def saveTransformed(spark: SparkSession, argsConfiguration: Configuration): Unit = {

    val processed = spark.sql(
      "SELECT VIN AS `vin`, " +
      "first(make) AS `make`, " +
      "first(`year`) AS `year`, " +
      "max(valueIfInLastXDays(speed, hrTimestamp, 1)) as `maxSpeedToday`, " +
      "max(valueIfInLastXDays(speed, hrTimestamp, 7)) as `maxSpeedLast7Days`, " +
      "avg(cast(`speed` AS Double)) AS `avgSpeed`, " +
      "max(cast(`instantFuelEconomy` AS Double)) AS `bestFuelEconomy`, " +
      "avg(cast(`instantFuelEconomy` AS Double)) AS `totalFuelEconomy`, " +
      "cast(count(vin) as Int) as `dataPointCount`, " +
      "sum(speed) as `speedSum`, " +
      "max(odometer) as `odometer`, " +
      "min(cast(valueIfInLastXDays(odometer, hrTimestamp, 1) as Double)) as `minOdometerToday`, " +
      "max(cast(valueIfInLastXDays(odometer, hrTimestamp, 1) as Double)) as `maxOdometerToday`, " +
      "min(cast(valueIfInLastXDays(odometer, hrTimestamp, 7) as Double)) as `minOdometerThisWeek`, " +
      "max(cast(valueIfInLastXDays(odometer, hrTimestamp, 7) AS Double)) as `maxOdometerThisWeek` " +

      "FROM raw_data " +
      "GROUP BY vin")


    // saves to MapR-DB
    val query =
      processed
        .writeStream
        .format(MapRDBSourceConfig.Format)
        .option(MapRDBSourceConfig.TablePathOption, argsConfiguration.transformed) // table path
        .option(MapRDBSourceConfig.CreateTableOption,false) // do not create table
        .option(MapRDBSourceConfig.IdFieldPathOption, "vin") // vin is the id
        .option("checkpointLocation", argsConfiguration.checkpoint) // checkpoint location
        .outputMode("complete")
        .start()

    query.awaitTermination()
  }


}
