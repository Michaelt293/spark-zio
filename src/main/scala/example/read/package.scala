package example

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.{Dataset, SparkSession}
import zio.{Has, RIO}

package object read {

  type ReadCsv = Has[ReadCsv.Service]
  type ReadParquet = Has[ReadParquet.Service]

  def readCsv[A <: Product](spark: SparkSession, path: String)(
      implicit
      typeTag: TypeTag[A]
  ): RIO[ReadCsv, Dataset[A]] =
    RIO.accessM(_.get.readCsv(spark, path))

  def readParquet[A <: Product](spark: SparkSession, path: String)(
      implicit
      typeTag: TypeTag[A]
  ): RIO[ReadParquet, Dataset[A]] =
    RIO.accessM(_.get.readParquet(spark, path))
}
