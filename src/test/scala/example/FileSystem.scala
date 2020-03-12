package example

import zio.IO

sealed abstract class FileSystem {
  def validatePath(path: String): IO[IllegalArgumentException, Unit]
}

object FileSystem {
  final case object GS extends FileSystem {
    override def validatePath(
        path: String
    ): IO[IllegalArgumentException, Unit] =
      if (path.startsWith("gs://")) IO.succeed(())
      else
        IO.fail(
          new IllegalArgumentException("Path does not start with \"gs://\"")
        )
  }
  final case object S3 extends FileSystem {
    override def validatePath(
        path: String
    ): IO[IllegalArgumentException, Unit] =
      if (path.startsWith("s3://") || path.startsWith("s3a://")) IO.succeed(())
      else
        IO.fail(
          new IllegalArgumentException(
            "Path does not start with \"s3://\" or \"s3a://\""
          )
        )
  }
  final case object Hdfs extends FileSystem {
    override def validatePath(
        path: String
    ): IO[IllegalArgumentException, Unit] =
      if (path.startsWith("hdfs://")) IO.succeed(())
      else
        IO.fail(
          new IllegalArgumentException("Path does not start with \"hdfs://\"")
        )
  }

  final case object Posix extends FileSystem {
    override def validatePath(
        path: String
    ): IO[IllegalArgumentException, Unit] =
      if (path.startsWith("/")) IO.succeed(())
      else
        IO.fail(
          new IllegalArgumentException("Path does not start with \"/\"")
        )
  }
}
