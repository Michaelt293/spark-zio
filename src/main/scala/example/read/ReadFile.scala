package example

import frameless.{TypedEncoder, TypedDataset}
import frameless.cats.implicits._
import org.apache.spark.sql.SparkSession
import zio._
import zio.interop.catz._

trait ReadFile extends Serializable {
  def readFile: ReadFile.Service[Any]
}

object ReadFile {
  trait Service[R] {
    def readParquet[A](path: String)(
        implicit
        spark: SparkSession,
        encoder: TypedEncoder[A]
    ): RIO[R, TypedDataset[A]]

    def readCsv[A](path: String)(
        implicit
        spark: SparkSession,
        encoder: TypedEncoder[A]
    ): RIO[R, TypedDataset[A]]
  }

  def readParquet[A](path: String)(
      implicit
      spark: SparkSession,
      encoder: TypedEncoder[A]
  ): RIO[ReadFile, TypedDataset[A]] =
    RIO.accessM(_.readFile.readParquet(path))

  def readCsv[A](path: String)(
      implicit
      spark: SparkSession,
      encoder: TypedEncoder[A]
  ): RIO[ReadFile, TypedDataset[A]] =
    RIO.accessM(_.readFile.readCsv(path))

  trait Live extends ReadFile {
    override def readFile: ReadFile.Service[Any] =
      new ReadFile.Service[Any] {
        override def readParquet[A](path: String)(
            implicit
            spark: SparkSession,
            encoder: TypedEncoder[A]
        ): RIO[Any, TypedDataset[A]] =
          ZIO.effect(
            spark.read.parquet(path).unsafeTyped
          )

        override def readCsv[A](path: String)(
            implicit
            spark: SparkSession,
            encoder: TypedEncoder[A]
        ): RIO[Any, TypedDataset[A]] =
          ZIO.effect(
            spark.read.csv(path).unsafeTyped
          )
      }
  }

  object Live extends Live

  final case class TestReadFileService[R](ref: Ref[FileSystem])
      extends ReadFile.Service[R] {
    override def readParquet[A](path: String)(
        implicit
        spark: SparkSession,
        encoder: TypedEncoder[A]
    ): Task[TypedDataset[A]] = ref.get.flatMap(_.readParquet(path))

    override def readCsv[A](path: String)(
        implicit
        spark: SparkSession,
        encoder: TypedEncoder[A]
    ): Task[TypedDataset[A]] = ref.get.flatMap(_.readCsv(path))
  }

  object TestReadFile {
    def apply[A](ref: Ref[FileSystem]): ReadFile =
      new ReadFile {
        def readFile: ReadFile.Service[Any] =
          TestReadFileService[Any](ref)
      }
  }
}
