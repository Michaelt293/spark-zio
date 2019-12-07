package example

import frameless.TypedDataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import zio._

trait WriteFile extends Serializable {
  def writeFile: WriteFile.Service[Any]
}

object WriteFile {
  trait Service[R] {
    def writeParquet[A](path: String, typedDataset: TypedDataset[A])(
        implicit
        spark: SparkSession
    ): RIO[R, Unit]
  }

  def writeParquet[A](path: String, typedDataset: TypedDataset[A])(
      implicit
      spark: SparkSession
  ): RIO[WriteFile, Unit] =
    RIO.accessM(_.writeFile.writeParquet(path, typedDataset))

  trait Live extends WriteFile {
    override def writeFile: WriteFile.Service[Any] =
      new WriteFile.Service[Any] {
        override def writeParquet[A](
            path: String,
            typedDataset: TypedDataset[A]
        )(
            implicit
            spark: SparkSession
        ): Task[Unit] =
          ZIO.effect(typedDataset.write.mode(SaveMode.Overwrite).parquet(path))
      }
  }

  object Live extends Live

  final case class TestWriteFileService[R](ref: Ref[FileSystem])
      extends WriteFile.Service[R] {
    override def writeParquet[A](path: String, typedDataset: TypedDataset[A])(
        implicit
        spark: SparkSession
    ): Task[Unit] =
      for {
        state <- ref.get
        data <- state.writeParquet(path, typedDataset)
        _ <- ref.set(data)
      } yield ()
  }

  object TestWriteFile {
    def apply[A](ref: Ref[FileSystem]): WriteFile =
      new WriteFile {
        def writeFile: WriteFile.Service[Any] =
          TestWriteFileService[Any](ref)
      }
  }
}
