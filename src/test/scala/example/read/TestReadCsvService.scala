package example.read

import scala.reflect.ClassTag

import org.apache.spark.sql.{Dataset, SparkSession}
import zio._

import example.FileSystemState

final case class TestReadCsvService(ref: Ref[FileSystemState])
    extends ReadCsv.Service {
  def readCsv[A](spark: SparkSession, path: String)(
      implicit
      classTag: ClassTag[A]
  ): Task[Dataset[A]] =
    ref.get.flatMap(_.readCsv(spark, path))
}

object TestReadCsv {
  def apply[A](
      ref: Ref[FileSystemState]
  ): ZLayer.NoDeps[Nothing, ReadCsv.ReadCsv] =
    ZLayer.succeed(TestReadCsvService(ref))
}
