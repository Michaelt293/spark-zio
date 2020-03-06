package example.read

import scala.reflect.ClassTag

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import zio._

trait ReadParquet extends Serializable {
  def readParquet: ReadParquet.Service[Any]
}

object ReadParquet {
  trait Service[R] {
    def readParquet[A](spark: SparkSession, path: String)(
        implicit
        classTag: ClassTag[A]
    ): RIO[R, Dataset[A]]
  }

  def readParquet[A](spark: SparkSession, path: String)( // TODO: move
      implicit
      classTag: ClassTag[A]
  ): RIO[ReadParquet, Dataset[A]] =
    RIO.accessM(_.readParquet.readParquet(spark, path))

  trait Live extends ReadParquet {
    override def readParquet: ReadParquet.Service[Any] =
      new ReadParquet.Service[Any] {
        override def readParquet[A](spark: SparkSession, path: String)(
            implicit
            classTag: ClassTag[A]
        ): RIO[Any, Dataset[A]] =
          ZIO.effect(
            spark.read
              .parquet(path)
              .as[A](Encoders.kryo[A])
          )
      }
  }

  object Live extends Live
}
