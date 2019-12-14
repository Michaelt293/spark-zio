package example

import java.io.IOException

import frameless.{TypedEncoder, TypedDataset}
import frameless.cats.implicits._
import org.apache.spark.sql.SparkSession
import zio._
import zio.interop.catz._

sealed abstract case class FileSystemState(
    fileSystem: Option[FileSystem],
    withWritePermissions: Set[String],
    withReadPermissions: Set[String],
    state: Map[String, File]
) {
  private def validatePath(path: String): IO[IllegalArgumentException, Unit] =
    FileSystemState.validatePath(fileSystem)(path)

  def writeParquet[A](path: String, data: TypedDataset[A])(
      implicit
      spark: SparkSession
  ): Task[FileSystemState] = {
    val validatePermissions =
      if (withWritePermissions.exists(p => !path.startsWith(p)))
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
      d <- data.collect[Task]()
      state <- FileSystemState.create(
        fileSystem,
        withWritePermissions: Set[String],
        withReadPermissions: Set[String],
        state + (path -> File(d.toList, FileType.Parquet))
      )
    } yield state
  }

  private def read[A](path: String, fileType: FileType)(
      implicit
      spark: SparkSession,
      typedEncoder: TypedEncoder[A]
  ): Task[TypedDataset[A]] = {
    val file = ZIO.fromOption(state.get(path)).mapError { _ =>
      new IOException(s"Path does not exist $path")
    }

    val validatePermissions =
      if (withReadPermissions.exists(p => !path.startsWith(p)))
        Task.fail(
          new IOException(
            s"Read permissions do not exist for $path"
          )
        )
      else
        Task.succeed(())

    val readFile = file.flatMap { f =>
      if (f.fileType == fileType)
        Task.effect(TypedDataset.create(f.data.map(_.asInstanceOf[A])))
      else
        Task.fail(
          new IOException(
            s"Wrong file type: Found ${f.fileType}, expected $fileType"
          )
        )
    }

    validatePath(path) *> validatePermissions *> readFile
  }

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

object FileSystemState {
  def create(
      fileSystem: Option[FileSystem],
      withWritePermissions: Set[String],
      withReadPermissions: Set[String],
      state: Map[String, File]
  ) = {
    def validate(path: String) =
      validatePath(fileSystem)(path)
    for {
      _ <- ZIO.traverse(withWritePermissions)(validate(_))
      _ <- ZIO.traverse(withReadPermissions)(validate(_))
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
