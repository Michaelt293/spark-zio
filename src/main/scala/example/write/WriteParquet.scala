package example.write

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import zio._

object WriteParquet {
  type WriteParquet = Has[Service]

  trait Service {
    def writeParquet[A](
        spark: SparkSession,
        path: String,
        dataset: Dataset[A]
    ): Task[Unit]
  }

  def writeParquet[A](
      spark: SparkSession,
      path: String,
      dataset: Dataset[A]
  ): RIO[WriteParquet, Unit] =
    RIO.accessM(_.get.writeParquet(spark, path, dataset))

  val live: ZLayer.NoDeps[Nothing, WriteParquet] = ZLayer.succeed(
    new Service {
      def writeParquet[A](
          spark: SparkSession,
          path: String,
          dataset: Dataset[A]
      ): Task[Unit] =
        ZIO.effect(dataset.write.mode(SaveMode.Overwrite).parquet(path))
    }
  )
}
