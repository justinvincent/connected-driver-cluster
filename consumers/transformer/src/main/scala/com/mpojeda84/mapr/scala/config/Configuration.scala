package com.mpojeda84.mapr.scala.config

case class Configuration(checkpoint: String, topic: String, transformed: String, dateOffset: String)

object Configuration {

  def parse(args: Seq[String]): Configuration = parser.parse(args, Configuration.default).get

  def default: Configuration = DefaultConfiguration

  object DefaultConfiguration extends Configuration(
    "path/to/json", // to achieve exactly-once semantic (each element in the stream is read one time exactly)
    "/path/to/stream:topic", // topic to read from
    "/obd/car-data-transformed", // table to write to
    "2019-02-13 0:55:08" // fix the date for the demo rather than reading from the system date
  )

  private val parser = new scopt.OptionParser[Configuration]("App Name") {
    head("App Name")

    opt[String]('h', "checkpoint")
      .action((t, config) => config.copy(checkpoint = t))
      .maxOccurs(1)
      .text("Checkpoint Location")

    opt[String]('r', "transformed")
      .action((t, config) => config.copy(transformed = t))
      .maxOccurs(1)
      .text("MapR-DB table name to write results to")

    opt[String]('n', "topic")
      .action((s, config) => config.copy(topic = s))
      .text("Topic where Kafka Producer is writing to")

    opt[String]('o', "dateOffset")
      .optional()
      .action((s, config) => config.copy(dateOffset = s))
      .text("Offset Date, default: '2019-01-28 0:17:08'")

  }
}