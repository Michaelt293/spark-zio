package example

import java.io.IOException

import scala.reflect.ClassTag

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import zio._

final case class FileSystem(
    state: Map[String, File]
) {
  def writeParquet[A](
      spark: SparkSession,
      path: String,
      data: Dataset[A]
  ): Task[FileSystem] =
    Task
      .effect(data.collect())
      .map(
        d =>
          FileSystem(
            state + (path -> File(d.toList, FileType.Parquet))
          )
      )

  private def read[A](spark: SparkSession, path: String, fileType: FileType)(
      implicit
      classTag: ClassTag[A]
  ): Task[Dataset[A]] = {
    val f = state(path)

    if (f.fileType == fileType)
      Task.succeed(
        spark.createDataset(f.data.map(_.asInstanceOf[A]))(Encoders.kryo[A])
      )
    else
      Task.fail(
        new IOException(
          s"Wrong file type: Found ${f.fileType}, expected $fileType"
        )
      )
  }

  def readParquet[A](spark: SparkSession, path: String)(
      implicit
      classTag: ClassTag[A]
  ): Task[Dataset[A]] =
    read(spark, path, FileType.Parquet)

  def readCsv[A](spark: SparkSession, path: String)(
      implicit
      classTag: ClassTag[A]
  ): Task[Dataset[A]] =
    read(spark, path, FileType.Csv)
}

sealed trait FileType

object FileType {
  final case object Csv extends FileType
  final case object Parquet extends FileType
}

final case class File(data: List[Any], fileType: FileType)
