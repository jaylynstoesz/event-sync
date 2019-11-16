import java.util

import akka.actor.ActorSystem
import akka.stream.{ActorAttributes, ActorMaterializer, Materializer, Supervision}
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.util.ByteString
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeAction, AttributeValue, AttributeValueUpdate, ReturnValue, UpdateItemRequest, UpdateItemResponse}

import scala.collection.JavaConverters._
import java.net.URI

import software.amazon.awssdk.regions.Region

object Main {
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

  import akka.stream.scaladsl._
  import java.nio.file.Paths

  def main(args: Array[String]): Unit = {
    val filepath = args.headOption.getOrElse(throw new Exception("Missing source filepath"))

    implicit val actorSystem: ActorSystem = ActorSystem.create()
    implicit val materializer: Materializer = Materializer.matFromSystem

    val client: DynamoDbClient =
      DynamoDbClient
          .builder()
          .endpointOverride(URI.create("http://localhost:4569"))
          .region(Region.US_EAST_1)
          .build()

    FileIO
      .fromPath(Paths.get(filepath))
      .via(CsvParsing.lineScanner())
      .via(CsvToMap.toMapAsStrings())
      .map(updateRequest)
      .withAttributes(ActorAttributes.supervisionStrategy {
        case ex: Throwable =>
          println("Error parsing row event: {}", ex)
          Supervision.Resume
      })
      .map(client.updateItem)
      .withAttributes(ActorAttributes.supervisionStrategy {
        case ex: Throwable =>
          println("Error saving row event: {}", ex)
          Supervision.Resume
      })
      .runWith(Sink.ignore)

    println("Done. ")
  }

  /** Build request to upsert record in lifetime BudgetMetrics table (LifetimeBudgetMetrics) */
  def updateRequest(line: Map[String, String]): UpdateItemRequest = {
    val contractId = line("contract_id")
    val campaignId = line("campaign_id")


    val lifetimeKey = Map(
      CAMPAIGN_ID -> AttributeValue.builder().s(campaignId).build(),
      CONTRACT_ID -> AttributeValue.builder().s(contractId).build()
    ).asJava

    val updateExpressions: String =
      "SET " +
        List(
          s"$TIMESTAMP = :$TIMESTAMP, ",
          s"$FIRST_ACTIVITY = :$FIRST_ACTIVITY, ",
          s"#$TTL = :$TTL, ",
          s"$ON_PLATFORM_REDEMPTIONS = :$ON_PLATFORM_REDEMPTIONS, ",
          s"$ON_PLATFORM_MICROS = :$ON_PLATFORM_MICROS, ",
          s"$ON_PLATFORM_UNLOCKS = :$ON_PLATFORM_UNLOCKS, ",
          s"$OFF_PLATFORM_REDEMPTIONS = :$OFF_PLATFORM_REDEMPTIONS, ",
          s"$OFF_PLATFORM_MICROS = :$OFF_PLATFORM_MICROS, ",
          s"$OFF_PLATFORM_UNLOCKS = :$OFF_PLATFORM_UNLOCKS, ",
          s"$UNTRACKED_REDEMPTIONS = :$UNTRACKED_REDEMPTIONS, ",
          s"$UNTRACKED_MICROS = :$UNTRACKED_MICROS, ",
          s"$UNTRACKED_UNLOCKS = :$UNTRACKED_UNLOCKS"
        ).mkString("")

    val expressionValues =
      Map(
        s":$TIMESTAMP" -> line("ts"),
        s":$TTL" -> line("ttl"),
        s":$FIRST_ACTIVITY" -> line("first_activity"),
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

    println(campaignId)
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