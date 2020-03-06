// package example.write

// import scala.reflect.ClassTag

// import org.apache.spark.sql.{Dataset, SparkSession}
// import zio._

// import example.FileSystemState

// final case class TestWriteCsvService[R](ref: Ref[FileSystemState])
//     extends WriteCsv.Service[R] {
//   override def writeCsv[A](
//       spark: SparkSession,
//       path: String,
//       dataset: Dataset[A]
//   ): Task[Unit] =
//     for {
//       state <- ref.get
//       data <- state.writeCsv(spark, path, dataset)
//       _ <- ref.set(data)
//     } yield ()
// }

// object TestWriteCsv {
//   def apply[A](
//       ref: Ref[FileSystemState]
//   ): ZLayer.NoDeps[Nothing, WriteCsv.WriteCsv] = ZLayer.succeed(
//     new WriteCsv.Service {
//       def writeCsv: WriteCsv.Service =
//         TestWriteCsvService(ref)
//     }
//   )
// }
