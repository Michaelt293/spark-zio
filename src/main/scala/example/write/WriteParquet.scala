package example.write

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import zio.{Layer, Task, ZLayer, ZIO}

object WriteParquet {

  trait Service {
    def writeParquet[A](
        spark: SparkSession,
        path: String,
        dataset: Dataset[A]
    ): Task[Unit]
  }

  val live: Layer[Nothing, WriteParquet] = ZLayer.succeed(
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
