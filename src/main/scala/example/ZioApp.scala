package example

import org.apache.spark.sql.{Encoders, SparkSession}
import zio._
import zio.console._

object ZioApp extends App {

  type AppEnv =
    Console with ReadFile with WriteFile with AppSparkSession

  val appEnv: AppEnv =
    new Console with ReadFile with WriteFile with AppSparkSession {
      val readFile: ReadFile.Service[Any] =
        ReadFile.Live.readFile

      val console: Console.Service[Any] = Console.Live.console

      val writeFile: WriteFile.Service[Any] =
        WriteFile.Live.writeFile

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
      _ <- WriteFile.writeParquet(spark, parquetPath, dataset)
      _ <- putStrLn(s"Reading from parquet from $parquetPath......")
      parquetData <- ReadFile.readParquet[Person](spark, parquetPath)
      summaryPath = "/tmp/zio-test_summary.parquet"
      _ <- putStrLn(s"Writing summary to $summaryPath......")
      summary = parquetData.map(_.toPersonSummary)(Encoders.kryo[PersonSummary])
      _ <- WriteFile.writeParquet(spark, summaryPath, summary)
      summaryData <- ReadFile.readParquet[PersonSummary](spark, summaryPath)
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
