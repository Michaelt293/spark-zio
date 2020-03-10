package example

import org.apache.spark.sql.Encoders
import zio._
import zio.console._
import example.read._
import example.spark._
import example.write._

object ZioApp extends App {

  type AppEnv = ReadParquet with ReadCsv with WriteParquet with AppSparkSession

  val appEnv: ZLayer[Any, Throwable, AppEnv] =
    ReadParquet.live ++
      ReadCsv.live ++
      WriteParquet.live ++
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
      )(Encoders.kryo[Person])
      parquetPath = "/tmp/zio-test.parquet"
      _ <- putStrLn(s"Writing parquet to $parquetPath......")
      _ <- writeParquet(spark, parquetPath, dataset)
      _ <- putStrLn(s"Reading from parquet from $parquetPath......")
      parquetData <- readParquet[Person](spark, parquetPath)
      summaryPath = "/tmp/zio-test_summary.parquet"
      _ <- putStrLn(s"Writing summary to $summaryPath......")
      summary = parquetData.map(_.toPersonSummary)(Encoders.kryo[PersonSummary])
      _ <- writeParquet(spark, summaryPath, summary)
      summaryData <- readParquet[PersonSummary](spark, summaryPath)
    } yield ()

  def run(args: List[String]): ZIO[ZEnv, Nothing, Int] =
    program
      .provideCustomLayer(appEnv)
      .foldM(
        _ => putStrLn("Job failed!") *> ZIO.succeed(1),
        _ => putStrLn("Job completed!") *> ZIO.succeed(0)
      )
}
