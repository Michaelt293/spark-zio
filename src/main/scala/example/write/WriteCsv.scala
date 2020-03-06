package example.write

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import zio._

object WriteCsv {
  type WriteCsv = Has[Service]

  trait Service {
    def writeCsv[A](
        spark: SparkSession,
        path: String,
        dataset: Dataset[A]
    ): Task[Unit]
  }

  def writeCsv[A](
      spark: SparkSession,
      path: String,
      dataset: Dataset[A]
  ): RIO[WriteCsv, Unit] =
    RIO.accessM(_.get.writeCsv(spark, path, dataset))

  val live: ZLayer.NoDeps[Nothing, WriteCsv] = ZLayer.succeed(
    new Service {
      def writeCsv[A](
          spark: SparkSession,
          path: String,
          dataset: Dataset[A]
      ): Task[Unit] =
        ZIO.effect(dataset.write.mode(SaveMode.Overwrite).csv(path))
    }
  )
}
