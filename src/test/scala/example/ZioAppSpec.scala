package example

import zio._
import zio.duration._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test.environment._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import example.read._
import example.spark._
import example.write._
import example.ZioApp.{Person, PersonSummary}
import example.spark.SparkSessionBuilder
import example.spark.SparkSessionBuilder.Service

object ZioAppSpec extends DefaultRunnableSpec {

  val testSessionBuilder: ZLayer.NoDeps[Nothing, SparkSessionBuilder] =
    ZLayer.succeed(
      new Service {
        val sparkSessionBuilder: SparkSession.Builder = {
          val conf =
            new SparkConf()
              .set("spark.ui.enabled", "false")
              .set("spark.driver.host", "localhost")

          SparkSession.builder
            .config(conf)
            .master("local")
            .appName("ZioAppTest")
        }
      }
    )

  def appEnv(
      ref: Ref[FileSystemState]
  ): ZLayer[Any, Throwable, ZioApp.AppEnv] =
    TestWriteParquet(ref) ++
      TestReadCsv(ref) ++
      TestReadParquet(ref) ++
      (testSessionBuilder >>> sparkSessionZLayer)

  val expectOutput = Map(
    "/tmp/zio-test.parquet" -> File(
      List(Person("Michael", 18, "Student"), Person("Peter", 38, "Chef")),
      FileType.Parquet
    ),
    "/tmp/zio-test_summary.parquet" -> File(
      List(PersonSummary("Michael", 18), PersonSummary("Peter", 38)),
      FileType.Parquet
    )
  )

  val expectConsoleOutput = Vector(
    s"Testing......${System.lineSeparator}",
    s"Creating Dataset......${System.lineSeparator}",
    s"Writing parquet to /tmp/zio-test.parquet......${System.lineSeparator}",
    s"Reading from parquet from /tmp/zio-test.parquet......${System.lineSeparator}",
    s"Writing summary to /tmp/zio-test_summary.parquet......${System.lineSeparator}"
  )

  def spec = suite("ZioAppSpec")(
    testM("The ZIO program should read and write from/to the test filesystem") {
      for {
        fs <- FileSystemState.create(
          Some(FileSystem.Posix),
          Set("/tmp"),
          Set("/tmp"),
          Map.empty
        )
        ref <- Ref.make(fs)
        _ <- ZioApp.program.provideCustomLayer(appEnv(ref))
        state <- ref.get
        consoleOutput <- TestConsole.output
      } yield
        assert(state.state)(equalTo(expectOutput)) &&
          assert(consoleOutput)(equalTo(expectConsoleOutput))
    } @@ timeout(10.seconds)
  )
}
