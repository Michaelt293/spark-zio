package example.write

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import zio.{Layer, Task, ZLayer, ZIO}

object WriteCsv {

  trait Service {
    def writeCsv[A](
        spark: SparkSession,
        path: String,
        dataset: Dataset[A]
    ): Task[Unit]
  }

  val live: Layer[Nothing, WriteCsv] = ZLayer.succeed(
    new Service {
      def writeCsv[A](
          spark: SparkSession,
          path: String,
          dataset: Dataset[A]
      ): Task[Unit] =
        ZIO.effect(
          dataset.write
            .mode(SaveMode.Overwrite)
            .option("header", "true")
            .csv(path))
    }
  )
}
