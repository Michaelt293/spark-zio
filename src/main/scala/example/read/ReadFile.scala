package example

import scala.reflect.ClassTag

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import zio._

trait ReadFile extends Serializable {
  def readFile: ReadFile.Service[Any]
}

object ReadFile {
  trait Service[R] {
    def readParquet[A](spark: SparkSession, path: String)(
        implicit
        classTag: ClassTag[A]
    ): RIO[R, Dataset[A]]

    def readCsv[A](spark: SparkSession, path: String)(
        implicit
        classTag: ClassTag[A]
    ): RIO[R, Dataset[A]]
  }

  def readParquet[A](spark: SparkSession, path: String)(
      implicit
      classTag: ClassTag[A]
  ): RIO[ReadFile, Dataset[A]] =
    RIO.accessM(_.readFile.readParquet(spark, path))

  def readCsv[A](spark: SparkSession, path: String)(
      implicit
      classTag: ClassTag[A]
  ): RIO[ReadFile, Dataset[A]] =
    RIO.accessM(_.readFile.readCsv(spark, path))

  trait Live extends ReadFile {
    override def readFile: ReadFile.Service[Any] =
      new ReadFile.Service[Any] {
        override def readParquet[A](spark: SparkSession, path: String)(
            implicit
            classTag: ClassTag[A]
        ): RIO[Any, Dataset[A]] =
          ZIO.effect(
            spark.read
              .parquet(path)
              .as[A](Encoders.kryo[A])
          )

        override def readCsv[A](spark: SparkSession, path: String)(
            implicit
            classTag: ClassTag[A]
        ): RIO[Any, Dataset[A]] =
          ZIO.effect(
            spark.read.csv(path).as[A](Encoders.kryo[A])
          )
      }
  }

  object Live extends Live

  final case class TestReadFileService[R](ref: Ref[FileSystemState])
      extends ReadFile.Service[R] {
    override def readParquet[A](spark: SparkSession, path: String)(
        implicit
        classTag: ClassTag[A]
    ): Task[Dataset[A]] =
      ref.get.flatMap(_.readParquet(spark, path))

    override def readCsv[A](spark: SparkSession, path: String)(
        implicit
        classTag: ClassTag[A]
    ): Task[Dataset[A]] =
      ref.get.flatMap(_.readCsv(spark, path))
  }

  object TestReadFile {
    def apply[A](ref: Ref[FileSystemState]): ReadFile =
      new ReadFile {
        def readFile: ReadFile.Service[Any] =
          TestReadFileService[Any](ref)
      }
  }
}
