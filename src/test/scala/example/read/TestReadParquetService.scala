package example.read

import scala.reflect.ClassTag

import org.apache.spark.sql.{Dataset, SparkSession}
import zio._

import example.FileSystemState

final case class TestReadParquetService(ref: Ref[FileSystemState])
    extends ReadParquet.Service {
  def readParquet[A](spark: SparkSession, path: String)(
      implicit
      classTag: ClassTag[A]
  ): Task[Dataset[A]] =
    ref.get.flatMap(_.readParquet(spark, path))
}

object TestReadParquet {
  def apply[A](
      ref: Ref[FileSystemState]
  ): ZLayer.NoDeps[Nothing, ReadParquet.ReadParquet] =
    ZLayer.succeed(TestReadParquetService(ref))
}
