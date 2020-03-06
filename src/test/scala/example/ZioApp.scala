package example

import org.apache.spark.sql.{Encoders, SparkSession}
import zio._
import zio.console._

import example.read._
import example.write._

object ZioApp extends App {

  type AppEnv =
    Console with ReadParquet with ReadCsv with WriteParquet with AppSparkSession

  val appEnv: AppEnv =
    new Console with ReadParquet with ReadCsv with WriteParquet
    with AppSparkSession {
      val console: Console.Service[Any] = Console.Live.console

      val readParquet: ReadParquet.Service[Any] =
        ReadParquet.Live.readParquet

      val readCsv: ReadCsv.Service[Any] =
        ReadCsv.Live.readCsv

      val writeParquet: WriteParquet.Service[Any] =
        WriteParquet.Live.writeParquet

      val appSparkSession: AppSparkSession.Service[Any] =
        AppSparkSession.Live.appSparkSession
    }

  case class Person(name: String, age: Int, job: String) {
    def toPersonSummary = PersonSummary(name, age)
  }

  case class PersonSummary(name: String, age: Int)

  val program: ZIO[AppEnv, Throwable, Unit] =
    for {
      _ <- putStrLn("Testing......")
      spark <- AppSparkSession.sparkSession
      _ <- putStrLn(s"Creating Dataset......")
      dataset = spark.createDataset(
        List(Person("Michael", 18, "Student"), Person("Peter", 38, "Chef"))
      )(Encoders.kryo[Person])
      parquetPath = "/tmp/zio-test.parquet"
      _ <- putStrLn(s"Writing parquet to $parquetPath......")
      _ <- WriteParquet.writeParquet(spark, parquetPath, dataset)
      _ <- putStrLn(s"Reading from parquet from $parquetPath......")
      parquetData <- ReadParquet.readParquet[Person](spark, parquetPath)
      summaryPath = "/tmp/zio-test_summary.parquet"
      _ <- putStrLn(s"Writing summary to $summaryPath......")
      summary = parquetData.map(_.toPersonSummary)(Encoders.kryo[PersonSummary])
      _ <- WriteParquet.writeParquet(spark, summaryPath, summary)
      summaryData <- ReadParquet.readParquet[PersonSummary](spark, summaryPath)
      _ = spark.stop()
    } yield ()

  def run(args: List[String]) =
    program
      .provide(appEnv)
      .foldM(
        _ => putStrLn("Job failed!") *> ZIO.succeed(1),
        _ => putStrLn("Job completed!") *> ZIO.succeed(0)
      )
}
