package example

import java.io.IOException

import frameless.{TypedEncoder, TypedDataset}
import frameless.cats.implicits._
import org.apache.spark.sql.SparkSession
import zio._
import zio.interop.catz._

final case class FileSystem(
    state: Map[String, File]
) {
  def writeParquet[A](path: String, data: TypedDataset[A])(
      implicit
      spark: SparkSession
  ): Task[FileSystem] =
    data
      .collect[Task]()
      .map(
        d =>
          FileSystem(
            state + (path -> File(d.toList, FileType.Parquet))
          )
      )

  private def read[A](path: String, fileType: FileType)(
      implicit
      spark: SparkSession,
      typedEncoder: TypedEncoder[A]
  ): Task[TypedDataset[A]] =
    ZIO.effect(TypedDataset.create {
      val f = state(path)

      if (f.fileType == fileType) f.data.map(_.asInstanceOf[A])
      else
        throw new IOException(
          s"Wrong file type: Found ${f.fileType}, expected $fileType"
        )
    })

  def readParquet[A](path: String)(
      implicit
      spark: SparkSession,
      typedEncoder: TypedEncoder[A]
  ): Task[TypedDataset[A]] =
    read(path, FileType.Parquet)

  def readCsv[A](path: String)(
      implicit
      spark: SparkSession,
      typedEncoder: TypedEncoder[A]
  ): Task[TypedDataset[A]] =
    read(path, FileType.Csv)
}

sealed trait FileType

object FileType {
  final case object Csv extends FileType
  final case object Parquet extends FileType
}

final case class File(data: List[Any], fileType: FileType)
