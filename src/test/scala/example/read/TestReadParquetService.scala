package example.read

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.{Dataset, SparkSession}
import zio.{Layer, Ref, Task, ZLayer}

import example.FileSystemState

final case class TestReadParquetService(ref: Ref[FileSystemState])
    extends ReadParquet.Service {
  def readParquet[A <: Product](spark: SparkSession, path: String)(
      implicit
      typeTag: TypeTag[A]
  ): Task[Dataset[A]] =
    ref.get.flatMap(_.readParquet[A](spark, path))
}

object TestReadParquet {
  def apply(
      ref: Ref[FileSystemState]
  ): Layer[Nothing, ReadParquet] =
    ZLayer.succeed(TestReadParquetService(ref))
}
