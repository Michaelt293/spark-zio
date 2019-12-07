package example

import frameless.cats.implicits._
import org.apache.spark.sql.SparkSession
import zio._
import zio.console._
import zio.interop.catz._

object ZioApp extends App {

  def run(args: List[String]) =
    program
      .provide(appEnv)
      .foldM(
        _ => putStrLn("Job failed!") *> ZIO.succeed(1),
        _ => putStrLn("Job completed!") *> ZIO.succeed(0)
      )

  type AppEnv =
    Console with ReadFile with WriteFile with AppSparkSession

  val appEnv: AppEnv =
    new Console with ReadFile with WriteFile with AppSparkSession {
      val readFile: ReadFile.Service[Any] =
        ReadFile.Live.readFile

      val console: Console.Service[Any] = Console.Live.console

      val appSparkSession: AppSparkSession.Service[Any] =
        AppSparkSession.Live.appSparkSession

      val writeFile: WriteFile.Service[Any] =
        WriteFile.Live.writeFile
    }

  case class Person(name: String, age: Int, job: String)

  case class PersonSummary(name: String, age: Int)

  val program =
    for {
      _ <- putStrLn("Testing......")
      implicit0(sparkSession: SparkSession) <- AppSparkSession.sparkSession
      csvPath = "/tmp/zio-test.csv"
      _ <- putStrLn(s"Reading from csv from$csvPath......")
      csvData <- ReadFile.readCsv[Person](csvPath)
      parquetPath = "/tmp/zio-test.parquet"
      _ <- putStrLn(s"Writing parquet to $parquetPath......")
      _ <- WriteFile.writeParquet(parquetPath, csvData)
      _ <- putStrLn(s"Reading from parquet from$parquetPath......")
      parquetData <- ReadFile.readParquet[Person](parquetPath)
      summaryPath = "/tmp/zio-test_summary.parquet"
      _ <- putStrLn(s"Writing summary to $summaryPath......")
      summary = parquetData.project[PersonSummary]
      _ <- WriteFile.writeParquet(summaryPath, summary)
      summaryData <- ReadFile.readParquet[PersonSummary](summaryPath)
      // sample <- summaryData.take[Task](5) * causes error below
      // _ <- ZIO.foreach(sample)(d => putStrLn(d.toString))
      _ = sparkSession.stop()
    } yield ()
}

// ERROR CodeGenerator: failed to compile: org.codehaus.commons.compiler.CompileException: File 'generated.java', Line 78, Column 11: No applicable constructor/method found for actual parameters "java.lang.String, org.apache.spark.unsafe.types.UTF8String"; candidates are: "example.ZioApp$PersonSummary(java.lang.String, long)"
