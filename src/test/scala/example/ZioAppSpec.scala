package example

import org.scalatest._
import zio._
import zio.console._
import example.ZioApp.{Person, PersonSummary}

class ZioAppSpec extends FlatSpec with Matchers {

  "The ZIO program" should "read and write from/to the test filesystem" in {
    def appEnv(ref: Ref[FileSystem]): ZioApp.AppEnv =
      new Console with ReadFile with WriteFile with AppSparkSession {
        val readFile: ReadFile.Service[Any] =
          ReadFile.TestReadFile(ref).readFile

        val console: Console.Service[Any] = Console.Live.console

        val writeFile: WriteFile.Service[Any] =
          WriteFile.TestWriteFile(ref).writeFile

        val appSparkSession: AppSparkSession.Service[Any] =
          AppSparkSession.Test.appSparkSession
      }

    def runProgram(data: Map[String, File]): Task[FileSystem] =
      for {
        ref <- Ref.make(FileSystem(data))
        _ <- ZioApp.program
          .provide(appEnv(ref))
        state <- ref.get
      } yield state

    val state: FileSystem =
      new DefaultRuntime {}.unsafeRun {
        runProgram(Map.empty)
      }

    state.state shouldEqual
      Map(
        "/tmp/zio-test.parquet" -> File(
          List(Person("Michael", 18, "Student"), Person("Peter", 38, "Chef")),
          FileType.Parquet
        ),
        "/tmp/zio-test_summary.parquet" -> File(
          List(PersonSummary("Michael", 18), PersonSummary("Peter", 38)),
          FileType.Parquet
        )
      )
  }
}
