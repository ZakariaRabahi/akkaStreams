import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.Future
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import scala.concurrent.ExecutionContextExecutor

import akka.NotUsed

import java.nio.file.{Paths, StandardOpenOption}

// Case class to represent team statistics (team name and count)
case class TeamStats(team: String, count: Int)

object BasketballStatsProcessorBalanced extends App {

  // Set up the actor system and execution context
  implicit val system: ActorSystem = ActorSystem("StatsProcessorBalancedSystem")
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  // Path to the basketball CSV file
  val csvFilePath = Paths.get("src/main/scala/basketball.csv")

  // Case class to represent a single game record
  case class GameRecord(
                         season: Int, // Season year
                         round: Int, // Round number of the game
                         gameDate: String, // Date of the game
                         winningTeam: String, // Name of the winning team
                         losingTeam: String, // Name of the losing team
                         pointsDifference: Int, // Difference in points between winning and losing teams
                         dayOfWeek: String // Day of the week the game occurred
                       )

  // Flow to parse each row of the CSV into a GameRecord object
  val csvFlow = Flow[List[String]].map { columns =>
    GameRecord(
      season = columns(0).toInt,
      round = columns(1).toInt,
      gameDate = columns(3),
      winningTeam = columns(8),
      losingTeam = columns(18),
      pointsDifference = columns(14).toInt - columns(24).toInt,
      dayOfWeek = columns(4)
    )
  }

  // Custom flow to process GameRecords with a balancing mechanism
  def customFlowShapeWithBalance(filter: GameRecord => Boolean, useLosingTeam: Boolean = false): Flow[GameRecord, TeamStats, NotUsed] = {
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // Balance stage: distribute work evenly between two pipelines
      val balance = builder.add(Balancerge = builder.add(Merge )

      // F counts stats for teams based on the filter
      val countA = Flow[GameRecord]
        .filter(filter) // Apply the filter condition
        .fold(Map.empty[String, Int]) { (acc, game) =>
          val team = if (useLosingTeam) game.losingTeam else game.winningTeam
          acc.updated(team, acc.getOrElse(team, 0) + 1)
        }
        .mapConcat(_.toList.map { case (team, count) => TeamStats(team, count) }) // Convert to TeamStats list

      // Second pipeline: similar processing logic as the first
      val countB = Flow[GameRecord]
        .filter(filter)
        .fold(Map.empty[String, Int]) { (acc, game) =>
          val team = if (useLosingTeam) game.losingTeam else game.winningTeam
          acc.updated(team, acc.getOrElse(team, 0) + 1)
        }
        .mapConcat(_.toList.map { case (team, count) => TeamStats(team, count) })

      // Connect pipelines to balance and merge stages
      balance.out(0) ~> countA.buffer(20, OverflowStrategy.backpressure) ~> merge.in(0)
      balance.out(1) ~> countB.buffer(20, OverflowStrategy.backpressure) ~> merge.in(1)

      FlowShape(balance.in, merge.out) // Define the flow shape
    })
  }

  // Define filters for specific questions
  val sundayVictoriesFilter: GameRecord => Boolean = _.dayOfWeek.toLowerCase == "sunday"
  val closeVictoriesFilter: GameRecord => Boolean = _.pointsDifference <= 5
  val quarterFinalFilter: GameRecord => Boolean = _.round <= 4
  val lossesIn1980sFilter: GameRecord => Boolean = game => game.season >= 1980 && game.season <= 1990

  // Process games where victories happened on Sunday
  val sundayVictoriesStream = FileIO.fromPath(csvFilePath)
    .via(CsvParsing.lineScanner())
    .map(_.map(_.utf8String)) // Convert each line to a list of strings
    .drop(1) // Skip the header row
    .via(csvFlow) // Parse rows into GameRecord objects
    .via(customFlowShapeWithBalance(sundayVictoriesFilter)) // Apply custom flow with Sunday filter
    .runWith(Sink.fold(List.empty[TeamStats])((acc, stats) => acc :+ stats))

  // Process games with close victories (point difference <= 5)
  val closeVictoriesStream = FileIO.fromPath(csvFilePath)
    .via(CsvParsing.lineScanner())
    .map(_.map(_.utf8String))
    .drop(1)
    .via(csvFlow)
    .via(customFlowShapeWithBalance(closeVictoriesFilter))
    .runWith(Sink.fold(List.empty[TeamStats])((acc, stats) => acc :+ stats))

  // Process games from the quarter-finals
  val quarterFinalStream = FileIO.fromPath(csvFilePath)
    .via(CsvParsing.lineScanner())
    .map(_.map(_.utf8String))
    .drop(1)
    .via(csvFlow)
    .via(customFlowShapeWithBalance(quarterFinalFilter))
    .runWith(Sink.fold(List.empty[TeamStats])((acc, stats) => acc :+ stats))

  // Process games from the 1980s where teams lost
  val lossesIn1980sStream = FileIO.fromPath(csvFilePath)
    .via(CsvParsing.lineScanner())
    .map(_.map(_.utf8String))
    .drop(1)
    .via(csvFlow)
    .via(customFlowShapeWithBalance(lossesIn1980sFilter, useLosingTeam = true))
    .runWith(Sink.fold(List.empty[TeamStats])((acc, stats) => acc :+ stats))

  // Combine results from all streams and write them to separate files
  val allResults = for {
    sundayVictories <- sundayVictoriesStream
    closeVictories <- closeVictoriesStream
    quarterFinals <- quarterFinalStream
    losses1980s <- lossesIn1980sStream
  } yield (sundayVictories, closeVictories, quarterFinals, losses1980s)

  allResults.onComplete {
    case scala.util.Success((sundayVictories, closeVictories, quarterFinals, losses1980s)) =>
      def writeResultsToFile(flowName: String, statsList: List[TeamStats], metric: String, fileName: String): Future[IOResult] = {
        val content = statsList.groupBy(_.team)
          .map { case (team, stats) =>
            val totalCount = stats.map(_.count).sum
            s"name: $team --> $metric: $totalCount"
          }
          .mkString("\n")
        val filePath = Paths.get(fileName)
        Source.single(ByteString(content + "\n"))
          .runWith(FileIO.toPath(filePath, Set(StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)))
      }

      // Write results for each analysis to separate files
      val sundayVictoriesFile = writeResultsToFile("Sunday Victories", sundayVictories, "number of wins on Sunday", "sunday_victories.txt")
      val closeVictoriesFile = writeResultsToFile("Close Victories", closeVictories, "number of close victories", "close_victories.txt")
      val quarterFinalFile = writeResultsToFile("Quarter-Finals", quarterFinals, "number of quarter-finals reached", "quarter_finals.txt")
      val losses1980sFile = writeResultsToFile("Losses in the 1980s", losses1980s, "losses in the 1980s", "losses_1980s.txt")

      Future.sequence(Seq(sundayVictoriesFile, closeVictoriesFile, quarterFinalFile, losses1980sFile)).onComplete {
        case scala.util.Success(_) =>
          println("All files have been written successfully.")
          system.terminate()
        case scala.util.Failure(ex) =>
          println(s"File writing failed: $ex")
          system.terminate()
      }

    case scala.util.Failure(ex) =>
      println(s"Stream processing failed: $ex")
      system.terminate()
  }
}
