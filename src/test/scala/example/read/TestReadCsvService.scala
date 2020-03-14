package example.read

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.{Dataset, SparkSession}
import zio.{Layer, Ref, Task, ZLayer}

import example.FileSystemState

final case class TestReadCsvService(ref: Ref[FileSystemState])
    extends ReadCsv.Service {
  def readCsv[A <: Product](spark: SparkSession, path: String)(
      implicit
      typeTag: TypeTag[A]
  ): Task[Dataset[A]] =
    ref.get.flatMap(_.readCsv[A](spark, path))
}

object TestReadCsv {
  def apply(
      ref: Ref[FileSystemState]
  ): Layer[Nothing, ReadCsv] =
    ZLayer.succeed(TestReadCsvService(ref))
}
