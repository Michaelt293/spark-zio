package example

import java.io.IOException

import scala.reflect.ClassTag

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import zio._

sealed abstract case class FileSystemState(
    fileSystem: Option[FileSystem],
    withWritePermissions: Set[String],
    withReadPermissions: Set[String],
    state: Map[String, File]
) {
  private def validatePath(path: String): IO[IllegalArgumentException, Unit] =
    FileSystemState.validatePath(fileSystem)(path)

  def writeParquet[A](
      spark: SparkSession,
      path: String,
      data: Dataset[A]
  ): Task[FileSystemState] = {
    val validatePermissions =
      if (withWritePermissions.exists(p => !path.startsWith(p))) // TODO: improve logic
        Task.fail(
          new IOException(
            s"Write permissions do not exist for $path"
          )
        )
      else
        Task.succeed(())

    for {
      _ <- validatePath(path)
      _ <- validatePermissions
      d <- Task.effect(data.collect())
      state <- FileSystemState.create(
        fileSystem,
        withWritePermissions: Set[String],
        withReadPermissions: Set[String],
        state + (path -> File(d.toList, FileType.Parquet))
      )
    } yield state
  }

  private def read[A: ClassTag](
      spark: SparkSession,
      path: String,
      fileType: FileType
  ): Task[Dataset[A]] = {
    val file = ZIO.fromOption(state.get(path)).mapError { _ =>
      new IOException(s"Path does not exist $path")
    }

    val validatePermissions =
      if (withReadPermissions.exists(p => !path.startsWith(p))) // TODO: improve logic
        Task.fail(
          new IOException(
            s"Read permissions do not exist for $path"
          )
        )
      else
        Task.succeed(())

    val readFile = file.flatMap { f =>
      if (f.fileType == fileType)
        Task.effect(
          spark.createDataset(f.data.map(_.asInstanceOf[A]))(Encoders.kryo[A])
        )
      else
        Task.fail(
          new IOException(
            s"Wrong file type: Found ${f.fileType}, expected $fileType"
          )
        )
    }

    validatePath(path) *> validatePermissions *> readFile
  }

  def readParquet[A: ClassTag](
      spark: SparkSession,
      path: String
  ): Task[Dataset[A]] =
    read(spark, path, FileType.Parquet)

  def readCsv[A: ClassTag](
      spark: SparkSession,
      path: String
  ): Task[Dataset[A]] =
    read(spark, path, FileType.Csv)
}

object FileSystemState {
  def create(
      fileSystem: Option[FileSystem],
      withWritePermissions: Set[String],
      withReadPermissions: Set[String],
      state: Map[String, File]
  ): IO[IllegalArgumentException, FileSystemState] = {
    def validate(path: String): IO[IllegalArgumentException, Unit] =
      validatePath(fileSystem)(path)

    for {
      _ <- ZIO.foreach_(withWritePermissions)(validate)
      _ <- ZIO.foreach_(withReadPermissions)(validate)
    } yield
      new FileSystemState(
        fileSystem,
        withWritePermissions,
        withReadPermissions,
        state
      ) {}
  }

  def validatePath(fileSystem: Option[FileSystem])(
      path: String
  ): IO[IllegalArgumentException, Unit] =
    fileSystem match {
      case None     => ZIO.succeed(())
      case Some(fs) => fs.validatePath(path)
    }
}

sealed trait FileType

object FileType {
  final case object Csv extends FileType
  final case object Parquet extends FileType
}

final case class File(data: List[Any], fileType: FileType)
