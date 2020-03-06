package example.write

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import zio._

trait WriteCsv extends Serializable {
  def writeCsv: WriteCsv.Service[Any]
}

object WriteCsv {
  trait Service[R] {
    def writeCsv[A](
        spark: SparkSession,
        path: String,
        dataset: Dataset[A]
    ): RIO[R, Unit]
  }

  def writeCsv[A](
      spark: SparkSession,
      path: String,
      dataset: Dataset[A]
  ): RIO[WriteCsv, Unit] =
    RIO.accessM(_.writeCsv.writeCsv(spark, path, dataset))

  trait Live extends WriteCsv {
    override def writeCsv: WriteCsv.Service[Any] =
      new WriteCsv.Service[Any] {
        override def writeCsv[A](
            spark: SparkSession,
            path: String,
            dataset: Dataset[A]
        ): Task[Unit] =
          ZIO.effect(dataset.write.mode(SaveMode.Overwrite).csv(path))
      }
  }

  object Live extends Live
}
