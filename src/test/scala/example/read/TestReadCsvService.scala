package example.read

import scala.reflect.ClassTag

import org.apache.spark.sql.{Dataset, SparkSession}
import zio._

import example.FileSystemState

final case class TestReadCsvService[R](ref: Ref[FileSystemState])
    extends ReadCsv.Service[R] {
  override def readCsv[A](spark: SparkSession, path: String)(
      implicit
      classTag: ClassTag[A]
  ): Task[Dataset[A]] =
    ref.get.flatMap(_.readCsv(spark, path))
}

object TestReadCsv {
  def apply[A](ref: Ref[FileSystemState]): ReadCsv =
    new ReadCsv {
      def readCsv: ReadCsv.Service[Any] =
        TestReadCsvService[Any](ref)
    }
}
