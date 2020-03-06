package example

import zio._
import zio.console._
import zio.duration._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test.environment._

import example.read._
import example.write._
import example.ZioApp.{Person, PersonSummary}

object ZioAppSpec extends DefaultRunnableSpec {

  def appEnv(
      ref: Ref[FileSystemState]
  ): ZLayer[Any, Nothing, ZioApp.AppEnv with Console] =
    TestWriteParquet(ref) ++ TestReadCsv(ref) ++ TestReadParquet(ref) ++ AppSparkSession.test ++ Console.live

  def runProgram(data: Map[String, File]): Task[FileSystemState] =
    for {
      fs <- FileSystemState.create(
        Some(FileSystem.Posix),
        Set("/tmp"),
        Set("/tmp"),
        data
      )
      ref <- Ref.make(fs)
      _ <- ZioApp.program.provideLayer(appEnv(ref))
      state <- ref.get
    } yield state

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

  def spec = suite("ZioAppSpec")(
    testM("The ZIO program should read and write from/to the test filesystem") {
      for {
        state <- runProgram(Map.empty)
      } yield assert(state.state)(equalTo(expectOutput))
    } @@ timeout(10.seconds)
  )
}
