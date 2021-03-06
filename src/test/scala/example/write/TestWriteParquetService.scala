package example.write

import org.apache.spark.sql.{Dataset, SparkSession}
import zio.{Layer, Ref, Task, ZLayer}

import example.FileSystemState

final case class TestWriteParquetService(ref: Ref[FileSystemState])
    extends WriteParquet.Service {
  def writeParquet[A](
      spark: SparkSession,
      path: String,
      dataset: Dataset[A]
  ): Task[Unit] =
    for {
      state <- ref.get
      data <- state.writeParquet(spark, path, dataset)
      _ <- ref.set(data)
    } yield ()
}

object TestWriteParquet {
  def apply(
      ref: Ref[FileSystemState]
  ): Layer[Nothing, WriteParquet] =
    ZLayer.succeed(TestWriteParquetService(ref))
}
