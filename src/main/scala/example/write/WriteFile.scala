package example

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import zio._

trait WriteFile extends Serializable {
  def writeFile: WriteFile.Service[Any]
}

object WriteFile {
  trait Service[R] {
    def writeParquet[A](
        spark: SparkSession,
        path: String,
        dataset: Dataset[A]
    ): RIO[R, Unit]
  }

  def writeParquet[A](
      spark: SparkSession,
      path: String,
      dataset: Dataset[A]
  ): RIO[WriteFile, Unit] =
    RIO.accessM(_.writeFile.writeParquet(spark, path, dataset))

  trait Live extends WriteFile {
    override def writeFile: WriteFile.Service[Any] =
      new WriteFile.Service[Any] {
        override def writeParquet[A](
            spark: SparkSession,
            path: String,
            dataset: Dataset[A]
        ): Task[Unit] =
          ZIO.effect(dataset.write.mode(SaveMode.Overwrite).parquet(path))
      }
  }

  object Live extends Live

  final case class TestWriteFileService[R](ref: Ref[FileSystemState])
      extends WriteFile.Service[R] {
    override def writeParquet[A](
        spark: SparkSession,
        path: String,
        dataset: Dataset[A]
    ): Task[Unit] =
      for {
        state <- ref.get
        data <- state.writeParquet(spark, path, dataset)
        _ <- ref.set(data)
      } yield ()
  }

  object TestWriteFile {
    def apply[A](ref: Ref[FileSystemState]): WriteFile =
      new WriteFile {
        def writeFile: WriteFile.Service[Any] =
          TestWriteFileService[Any](ref)
      }
  }
}
