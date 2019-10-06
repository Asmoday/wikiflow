package com.renarde.wikiflow.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object AnalyticsConsumer extends App with LazyLogging {

  val appName: String = "analytics-consumer-example"

  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .config("spark.driver.memory", "5g")
    .master("local[2]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  logger.info("Initializing Structured consumer")
  import spark.implicits._


  val inputStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "wikiflow-topic")
    .option("startingOffsets", "earliest")
    .load()

  // please edit the code below


  val expectedSchema = new StructType()
    .add(StructField("bot", BooleanType))
    .add(StructField("comment", StringType))
    .add(StructField("id", LongType))
    .add("length", new StructType()
      .add(StructField("new", LongType))
      .add(StructField("old", LongType))
    )
    .add("meta", new StructType()
      .add(StructField("domain", StringType))
      .add(StructField("dt", StringType))
      .add(StructField("id", StringType))
      .add(StructField("offset", LongType))
      .add(StructField("partition", LongType))
      .add(StructField("request_id", StringType))
      .add(StructField("stream", StringType))
      .add(StructField("topic", StringType))
      .add(StructField("uri", StringType))
    )
    .add("minor", BooleanType)
    .add("namespace", LongType)
    .add("parsedcomment", StringType)
    .add("patrolled", BooleanType)
    .add("revision", new StructType()
      .add("new", LongType)
      .add("old", LongType)
    )
    .add("server_name", StringType)
    .add("server_script_path", StringType)
    .add("server_url", StringType)
    .add("timestamp", LongType)
    .add("title", StringType)
    .add("type", StringType)
    .add("user", StringType)
    .add("wiki", StringType)


  val transformedStream: DataFrame = inputStream
    .selectExpr("cast(key as string)","cast(value as string)").as[(String,String)]
    .filter($"value".isNotNull)
    .select(from_json($"value",expectedSchema).as("data"))
    .select("data.*")
    .filter($"bot" !== true)
    .selectExpr("type","cast(timestamp as timestamp) as timestamp")
    .withWatermark("timestamp", "10 minutes")
    .groupBy(window($"timestamp", "10 minutes", "5 minutes"), $"type")
    .count()
    .selectExpr("*","CURRENT_TIMESTAMP() as CURRENT_DTTM")


  transformedStream.writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", "/storage/analytics-consumer/checkpoints")
    .start("/storage/analytics-consumer/output")

  spark.streams.awaitAnyTermination()
}
