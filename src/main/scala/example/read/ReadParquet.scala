package example.read

import scala.reflect.ClassTag

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import zio._

object ReadParquet {

  trait Service {
    def readParquet[A](spark: SparkSession, path: String)(
        implicit
        classTag: ClassTag[A]
    ): Task[Dataset[A]]
  }

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
