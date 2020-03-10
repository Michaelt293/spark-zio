package example.read

import scala.reflect.ClassTag

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import zio._

object ReadCsv {

  trait Service {
    def readCsv[A](spark: SparkSession, path: String)(
        implicit
        classTag: ClassTag[A]
    ): Task[Dataset[A]]
  }

  val live: ZLayer.NoDeps[Nothing, ReadCsv] = ZLayer.succeed(
    new Service {
      def readCsv[A](spark: SparkSession, path: String)(
          implicit
          classTag: ClassTag[A]
      ): RIO[Any, Dataset[A]] =
        ZIO.effect(
          spark.read.csv(path).as[A](Encoders.kryo[A])
        )
    }
  )
}
