package example.write

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import zio._

trait WriteParquet extends Serializable {
  def writeParquet: WriteParquet.Service[Any]
}

object WriteParquet {
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
  ): RIO[WriteParquet, Unit] =
    RIO.accessM(_.writeParquet.writeParquet(spark, path, dataset))

  trait Live extends WriteParquet {
    override def writeParquet: WriteParquet.Service[Any] =
      new WriteParquet.Service[Any] {
        override def writeParquet[A](
            spark: SparkSession,
            path: String,
            dataset: Dataset[A]
        ): Task[Unit] =
          ZIO.effect(dataset.write.mode(SaveMode.Overwrite).parquet(path))
      }
  }

  object Live extends Live
}
