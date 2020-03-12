package example.write

import org.apache.spark.sql.{Dataset, SparkSession}
import zio._

import example.FileSystemState

final case class TestWriteCsvService(ref: Ref[FileSystemState])
    extends WriteCsv.Service {
  def writeCsv[A](
      spark: SparkSession,
      path: String,
      dataset: Dataset[A]
  ): Task[Unit] =
    for {
      state <- ref.get
      data <- state.writeCsv(spark, path, dataset)
      _ <- ref.set(data)
    } yield ()
}

object TestWriteCsv {
  def apply(
      ref: Ref[FileSystemState]
  ): ZLayer.NoDeps[Nothing, WriteCsv] =
    ZLayer.succeed(TestWriteCsvService(ref))
}
