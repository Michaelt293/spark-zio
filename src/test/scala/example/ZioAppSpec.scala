package example

import org.scalatest._
import zio._
import zio.console._
import example.ZioApp.{Person, PersonSummary}

class ZioAppSpec extends FlatSpec with Matchers {
  "The ZIO program" should "read and write from/to the test filesystem" in {
    def appEnv(ref: Ref[FileSystemState]): ZioApp.AppEnv =
      new Console with ReadFile with WriteFile with AppSparkSession {
        val readFile: ReadFile.Service[Any] =
          ReadFile.TestReadFile(ref).readFile

        val console: Console.Service[Any] = Console.Live.console

        val appSparkSession: AppSparkSession.Service[Any] =
          AppSparkSession.Test.appSparkSession

        val writeFile: WriteFile.Service[Any] = {
          WriteFile.TestWriteFile(ref).writeFile
        }
      }

    def runProgram(data: Map[String, File]): Task[FileSystemState] =
      for {
        fs <- FileSystemState.create(
          Some(FileSystem.Posix),
          Set("/tmp"),
          Set("/tmp"),
          data
        )
        ref <- Ref.make(fs)
        _ <- ZioApp.program
          .provide(appEnv(ref))
        state <- ref.get
      } yield state

    val state: FileSystemState =
      new DefaultRuntime {}.unsafeRun {
        runProgram(
          Map(
            "/tmp/zio-test.csv" ->
              File(
                List(
                  Person("Michael", 18, "Student"),
                  Person("Peter", 38, "Chef")
                ),
                FileType.Csv
              )
          )
        )

      }

    state.state shouldEqual
      Map(
        "/tmp/zio-test.csv" -> File(
          List(Person("Michael", 18, "Student"), Person("Peter", 38, "Chef")),
          FileType.Csv
        ),
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