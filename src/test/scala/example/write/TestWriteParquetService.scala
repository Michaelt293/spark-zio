package example.write

import scala.reflect.ClassTag

import org.apache.spark.sql.{Dataset, SparkSession}
import zio._

import example.FileSystemState

final case class TestWriteParquetService[R](ref: Ref[FileSystemState])
    extends WriteParquet.Service[R] {
  override def writeParquet[A](
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
  def apply[A](ref: Ref[FileSystemState]): WriteParquet =
    new WriteParquet {
      def writeParquet: WriteParquet.Service[Any] =
        TestWriteParquetService[Any](ref)
    }
}
