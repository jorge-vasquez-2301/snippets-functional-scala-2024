//> using jvm graalvm-java23:23.0.0
//> using scala 3.5.2
//> using dep info.fingo::spata:3.2.1
//> using dep dev.zio::zio-interop-cats:23.1.0.3
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

  // Default CSVParser, according to RFC 4180
  // Delimiter: Comma (,)
  // Quote character: Double quote (")
  // The first line of the file is considered the header row
  // Headers will be mapped to case class fields
  val csvParserWithDefaults: CSVParser[Task] = CSVParser[Task]

  val csvParser: CSVParser[Task] =
    CSVParser.config
      .mapHeader(Map("melting temperature [F]" -> "meltingTemp", "boiling temperature [F]" -> "boilingTemp"))
      .parser[Task]

  val csvParserPipe: fs2.Pipe[Task, Char, Record] = csvParser.parse

  // Default CSVRenderer, according to RFC 4180
  // Delimiter: Comma (,)
  // Quote character: Double quote (")
  // The rendered CSV will contain a header row
  // Case class fields will be mapped to headers
  val csvRendererWithDefaults: CSVRenderer[Task] = CSVRenderer[Task]

  val csvRenderer: CSVRenderer[Task] =
    CSVRenderer.config
      .fieldDelimiter(';')
      .mapHeader(Map("meltingTemp" -> "melting temperature [C]", "boilingTemp" -> "boiling temperature [C]"))
      .renderer[Task]

  val csvRendererPipe: fs2.Pipe[Task, Record, Char] = csvRenderer.render

  val fahrenheitCSV = "testdata/elements-fahrenheit.csv"
  val celsiusCSV    = "testdata/elements-celsius.csv"

  val processor: fs2.Stream[Task, Unit] =
    def fahrenheitToCelsius(f: Double): Double = (f - 32.0) * (5.0 / 9.0)

    Reader[Task]
      .read(Paths.get(fahrenheitCSV))
      .through(csvParserPipe)
      .toZStream()
      .mapZIO(record => ZIO.fromEither(record.to[Element]))
      .map(_.updateTemps(fahrenheitToCelsius))
      .map(_.toRecord) // toRecord is actually an extension method
      .toFs2Stream
      .through(csvRendererPipe)
      .through(Writer[Task].write(Paths.get(celsiusCSV)))

  val run =
    ZIO.log(s"Processing $fahrenheitCSV")
      *> processor.compile.drain
