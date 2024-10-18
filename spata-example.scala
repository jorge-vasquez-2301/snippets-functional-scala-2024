//> using jvm graalvm-java23:23.0.0
//> using scala 3.5.1
//> using dep co.fs2::fs2-core:3.11.0
//> using dep dev.zio::zio:2.1.11
//> using dep dev.zio::zio-streams:2.1.11
//> using dep dev.zio::zio-interop-cats:23.1.0.3
//> using dep info.fingo::spata:3.2.1
//> using dep dev.zio::zio-schema:1.5.0
//> using dep dev.zio::zio-schema-derivation:1.5.0

import java.nio.file.Paths
import zio.*
import zio.interop.catz.*
import zio.stream.interop.fs2z.*
import info.fingo.spata.{ CSVParser, CSVRenderer, Record }
import info.fingo.spata.io.{ Reader, Writer }
import zio.schema.*

object SpataExample extends ZIOAppDefault:
  final case class Element(element: String, symbol: String, meltingTemp: Double, boilingTemp: Double):
    self =>

    def updateTemps(f: Double => Double) =
      self.copy(meltingTemp = f(self.meltingTemp), boilingTemp = f(self.boilingTemp))

  object Element:
    given Schema.Record[Element] = DeriveSchema.gen[Element]

  extension [A](a: A)
    def toRecord(using schema: Schema.Record[A]) =
      Record.fromPairs(schema.fields.map(field => field.fieldName -> field.get(a).toString)*)

  val csvParserWithDefaults: fs2.Pipe[Task, Char, Record] = CSVParser[Task].parse

  val csvParser: fs2.Pipe[Task, Char, Record] =
    CSVParser.config
      .mapHeader(Map("melting temperature [F]" -> "meltingTemp", "boiling temperature [F]" -> "boilingTemp"))
      .parser[Task]
      .parse

  val csvRendererWithDefaults: fs2.Pipe[Task, Record, Char] = CSVRenderer[Task].render

  val csvRenderer: fs2.Pipe[Task, Record, Char] =
    CSVRenderer.config
      .fieldDelimiter(';')
      .mapHeader(Map("meltingTemp" -> "melting temperature [C]", "boilingTemp" -> "boiling temperature [C]"))
      .renderer[Task]
      .render

  val processor: fs2.Stream[Task, Unit] =
    def fahrenheitToCelsius(f: Double): Double = (f - 32.0) * (5.0 / 9.0)

    Reader[Task]
      .read(Paths.get("testdata/elements-fahrenheit.csv"))
      .through(csvParser)
      .toZStream()
      .mapZIO(record => ZIO.fromEither(record.to[Element]))
      .map(_.updateTemps(fahrenheitToCelsius).toRecord)
      .toFs2Stream
      .through(csvRenderer)
      .through(Writer[Task].write(Paths.get("testdata/elements-celsius.csv")))

  val run =
    Console.printLine("Processing input CSV") *> processor.compile.drain
