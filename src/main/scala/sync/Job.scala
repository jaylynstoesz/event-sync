package sync

import java.net.URI
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.{FileIO, Sink}
import akka.stream.{ActorAttributes, Materializer, Supervision}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, ReturnValue, UpdateItemRequest}

import scala.collection.JavaConverters._

class Job {
  import Job._

  def execute(filepath: String): Unit = {
    implicit val actorSystem: ActorSystem = ActorSystem.create()
    implicit val materializer: Materializer = Materializer.matFromSystem

    implicit val client: DynamoDbClient =
      DynamoDbClient
        .builder()
        .endpointOverride(URI.create("http://localhost:4569"))
        .region(Region.US_EAST_1)
        .build()

    FileIO
      .fromPath(Paths.get(filepath))
      .via(CsvParsing.lineScanner())
      .via(CsvToMap.toMapAsStrings())
      .map(buildUpdateRequest)
      .withAttributes(ActorAttributes.supervisionStrategy {
        ex: Throwable =>
          println(s"Error parsing row event: $ex")
          Supervision.Resume
      })
      .map(save)
      .withAttributes(ActorAttributes.supervisionStrategy {
        ex: Throwable =>
          println(s"Error saving row event: ${ex.getMessage}")
          Supervision.Resume
      })
      .to(Sink.ignore)
      .run()
  }

  def save(updateItemRequest: UpdateItemRequest)(implicit client: DynamoDbClient): Unit = {
    val campaignId = updateItemRequest.key().get(CAMPAIGN_ID).s()
    val timestamp = updateItemRequest.expressionAttributeValues().get(s":$TIMESTAMP").n().toLong
    println(s"$campaignId | $timestamp")
    client.updateItem(updateItemRequest)
  }

  /** Build request to upsert record in lifetime BudgetMetrics table (LifetimeBudgetMetrics) */
  def buildUpdateRequest(line: Map[String, String]): UpdateItemRequest = {
    val contractId = line("contract_id")
    val campaignId = line("campaign_id")

    val lifetimeKey = Map(
      CAMPAIGN_ID -> AttributeValue.builder().s(campaignId).build(),
      CONTRACT_ID -> AttributeValue.builder().s(contractId).build()
    ).asJava

    val updateExpressions: String =
      "SET " +
        List(
          s"$TIMESTAMP = :$TIMESTAMP",
          s"$FIRST_ACTIVITY = :$FIRST_ACTIVITY",
          s"#$TTL = :$TTL",
          s"$ON_PLATFORM_REDEMPTIONS = :$ON_PLATFORM_REDEMPTIONS",
          s"$ON_PLATFORM_MICROS = :$ON_PLATFORM_MICROS",
          s"$ON_PLATFORM_UNLOCKS = :$ON_PLATFORM_UNLOCKS",
          s"$OFF_PLATFORM_REDEMPTIONS = :$OFF_PLATFORM_REDEMPTIONS",
          s"$OFF_PLATFORM_MICROS = :$OFF_PLATFORM_MICROS",
          s"$OFF_PLATFORM_UNLOCKS = :$OFF_PLATFORM_UNLOCKS",
          s"$UNTRACKED_REDEMPTIONS = :$UNTRACKED_REDEMPTIONS",
          s"$UNTRACKED_MICROS = :$UNTRACKED_MICROS",
          s"$UNTRACKED_UNLOCKS = :$UNTRACKED_UNLOCKS"
        ).mkString(", ")

    val expressionValues =
      Map(
        s":$TIMESTAMP" -> s"${line("ts")}000",
        s":$TTL" -> line("ttl"),
        s":$FIRST_ACTIVITY" -> s"${line("first_activity")}000",
        s":$ON_PLATFORM_MICROS" -> line("micros"),
        s":$ON_PLATFORM_REDEMPTIONS" -> line("redemptions"),
        s":$ON_PLATFORM_UNLOCKS" -> "0",
        s":$OFF_PLATFORM_MICROS" -> "0",
        s":$OFF_PLATFORM_REDEMPTIONS" -> "0",
        s":$OFF_PLATFORM_UNLOCKS" -> "0",
        s":$UNTRACKED_MICROS" -> line("micros_untracked"),
        s":$UNTRACKED_REDEMPTIONS" -> line("redemptions_untracked"),
        s":$UNTRACKED_UNLOCKS" -> "0",
      ).map { v =>
        val attribute = AttributeValue.builder().n(v._2).build()
        v._1 -> attribute
      }.asJava

    val expressionNames = Map(
      s"#$TTL" -> TTL
    ).asJava

    UpdateItemRequest
      .builder()
      .tableName(LIFETIME_BUDGET_METRICS)
      .key(lifetimeKey)
      .updateExpression(updateExpressions)
      .expressionAttributeValues(expressionValues)
      .expressionAttributeNames(expressionNames)
      .returnValues(ReturnValue.NONE)
      .build()
  }
}

object Job {
  val CONTRACT_ID = "cid"
  val CAMPAIGN_ID = "pid"
  val ON_PLATFORM_REDEMPTIONS = "opr"
  val ON_PLATFORM_MICROS = "opm"
  val ON_PLATFORM_UNLOCKS = "opu"
  val OFF_PLATFORM_REDEMPTIONS = "fpr"
  val OFF_PLATFORM_MICROS = "fpm"
  val OFF_PLATFORM_UNLOCKS = "fpu"
  val UNTRACKED_REDEMPTIONS = "xr"
  val UNTRACKED_MICROS = "xm"
  val UNTRACKED_UNLOCKS = "xu"
  val FIRST_ACTIVITY = "fst"
  val TIMESTAMP = "ts"
  val TTL = "ttl"
  val LIFETIME_BUDGET_METRICS: String = "realtime-expiration-service-LifetimeBudgetMetrics"
}