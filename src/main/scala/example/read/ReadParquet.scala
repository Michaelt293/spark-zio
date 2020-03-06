package example.read

import scala.reflect.ClassTag

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import zio._

object ReadParquet {
  type ReadParquet = Has[Service]

  trait Service {
    def readParquet[A](spark: SparkSession, path: String)(
        implicit
        classTag: ClassTag[A]
    ): Task[Dataset[A]]
  }

  def readParquet[A](spark: SparkSession, path: String)( // TODO: move
      implicit
      classTag: ClassTag[A]
  ): RIO[ReadParquet, Dataset[A]] =
    RIO.accessM(_.get.readParquet(spark, path))

  val live: ZLayer.NoDeps[Nothing, ReadParquet] = ZLayer.succeed(
    new Service {
      def readParquet[A](spark: SparkSession, path: String)(
          implicit
          classTag: ClassTag[A]
      ): RIO[Any, Dataset[A]] =
        ZIO.effect(
          spark.read
            .parquet(path)
            .as[A](Encoders.kryo[A])
        )
    }
  )
}
