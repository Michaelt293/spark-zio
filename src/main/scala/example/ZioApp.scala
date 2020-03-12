package example

import org.apache.spark.sql.Encoders
import zio._
import zio.console._

import example.read._
import example.spark._
import example.write._

object ZioApp extends App {

  type AppEnv =
    ReadParquet
      with ReadCsv
      with WriteParquet
      with WriteCsv
      with AppSparkSession

  val appEnv: ZLayer[Any, Throwable, AppEnv] =
    ReadParquet.live ++
      ReadCsv.live ++
      WriteParquet.live ++
      WriteCsv.live ++
      (SparkSessionBuilder.live >>> sparkSessionZLayer)

  case class Person(name: String, age: Int, job: String) {
    def toPersonSummary = PersonSummary(name, age)
  }

  case class PersonSummary(name: String, age: Int)

  val program: ZIO[AppEnv with Console, Throwable, Unit] =
    for {
      _ <- putStrLn("Testing......")
      spark <- sparkSession
      _ <- putStrLn(s"Creating Dataset......")
      dataset = spark.createDataset(
        List(Person("Michael", 18, "Student"), Person("Peter", 38, "Chef"))
      )(Encoders.product[Person])
      parquetPath = "/tmp/zio-test.parquet"
      _ <- putStrLn(s"Writing parquet to $parquetPath......")
      _ <- writeParquet(spark, parquetPath, dataset)
      _ <- putStrLn(s"Reading from parquet from $parquetPath......")
      parquetData <- readParquet[Person](spark, parquetPath)
      summaryPath = "/tmp/zio-test_summary.csv"
      _ <- putStrLn(s"Writing summary to $summaryPath......")
      summary = parquetData.map(_.toPersonSummary)(
        Encoders.product[PersonSummary])
      _ <- writeCsv(spark, summaryPath, summary)
      summaryData <- readCsv[PersonSummary](spark, summaryPath)
      _ <- putStrLn("Printing summary to console......")
      _ <- ZIO.foreach_(summaryData.take(2))(p => putStrLn(p.toString))
    } yield ()

  def run(args: List[String]): ZIO[ZEnv, Nothing, Int] =
    program
      .provideCustomLayer(appEnv)
      .foldM(
        _ => putStrLn(s"Job failed!") *> ZIO.succeed(1),
        _ => putStrLn("Job completed!") *> ZIO.succeed(0)
      )
}
