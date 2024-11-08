//> using jvm graalvm-java11:22.3.3
//> using scala 3.5.2
//> using dep me.mnedokushev::zio-apache-parquet-core:0.1.4
//> using dep info.fingo::spata:3.2.1
//> using dep dev.zio::zio-interop-cats:23.1.0.3
//> using dep dev.zio::zio-schema:1.5.0
//> using dep dev.zio::zio-schema-derivation:1.5.0

import zio.schema.*
import me.mnedokushev.zio.apache.parquet.core.codec.*
import me.mnedokushev.zio.apache.parquet.core.hadoop.{ ParquetReader, ParquetWriter, Path }
import org.apache.parquet.hadoop.*
import zio.*
import zio.interop.catz.*
import zio.stream.interop.fs2z.*

import java.nio.file.Files
import java.nio.file.Paths
import info.fingo.spata.io.Reader
import info.fingo.spata.{ CSVParser, Record }
import zio.stream.*
import info.fingo.spata.CSVRenderer
import info.fingo.spata.io.Writer
import me.mnedokushev.zio.apache.parquet.core.filter.syntax.*
import me.mnedokushev.zio.apache.parquet.core.filter.*

object ZIOApacheParquetExample extends ZIOAppDefault:
  type Eff[A] = RIO[Scope, A]

  final case class Element(element: String, symbol: String, meltingTemp: Double, boilingTemp: Double):
    self =>

    def updateTemps(f: Double => Double) =
      self.copy(meltingTemp = f(self.meltingTemp), boilingTemp = f(self.boilingTemp))

  object Element:
    given Schema.CaseClass4.WithFields[
      "element",
      "symbol",
      "meltingTemp",
      "boilingTemp",
      String,
      String,
      Double,
      Double,
      Element
    ] = DeriveSchema.gen[Element]

    // SchemaEncoder is used to generate the corresponding Parquet Schema when writing files
    given SchemaEncoder[Element] = Derive.derive[SchemaEncoder, Element](SchemaEncoderDeriver.default)

    // Element => Value
    given ValueEncoder[Element] = Derive.derive[ValueEncoder, Element](ValueEncoderDeriver.default)

    // Value => Element
    given ValueDecoder[Element] = Derive.derive[ValueDecoder, Element](ValueDecoderDeriver.default)

    given TypeTag[Element]                          = Derive.derive[TypeTag, Element](TypeTagDeriver.default)
    val (element, symbol, meltingTemp, boilingTemp) = Filter[Element].columns

  extension [A](a: A)
    def toRecord(using schema: Schema.Record[A]) =
      Record.fromPairs(schema.fields.map(field => field.fieldName -> field.get(a).toString)*)

  val csvParser: fs2.Pipe[Eff, Char, Record] =
    CSVParser.config
      .mapHeader(Map("melting temperature [F]" -> "meltingTemp", "boiling temperature [F]" -> "boilingTemp"))
      .parser[Eff]
      .parse

  val csvRenderer: fs2.Pipe[Eff, Record, Char] =
    CSVRenderer.config
      .fieldDelimiter(';')
      .mapHeader(Map("meltingTemp" -> "melting temperature [C]", "boilingTemp" -> "boiling temperature [C]"))
      .renderer[Eff]
      .render

  val elementsFahrenheitCSVFile      = Paths.get("testdata/elements-fahrenheit.csv")
  val elementsCelsiusParquetFile     = Path(Paths.get("testdata/elements-celsius.parquet"))
  val elementsCelsiusFilteredCSVFile = Paths.get("testdata/elements-celsius-filtered.csv")

  val processor: RIO[ParquetWriter[Element] & ParquetReader[Element], Unit] =
    def fahrenheitToCelsius(f: Double): Double = (f - 32.0) * (5.0 / 9.0)

    def writeToCsv(stream: ZStream[Scope, Throwable, Element], path: java.nio.file.Path) =
      stream
        .map(_.toRecord)
        .toFs2Stream
        .through(csvRenderer)
        .through(Writer[Eff].write(path))
        .compile
        .drain

    val elementsStream: ZStream[Scope, Throwable, Element] =
      Reader[Eff]
        .read(elementsFahrenheitCSVFile)
        .through(csvParser)
        .toZStream()
        .mapZIO(record => ZIO.fromEither(record.to[Element]))
        .map(_.updateTemps(fahrenheitToCelsius))

    ZIO.scoped {
      for
        _                      <- ZIO.log(s"Writing all elements to $elementsCelsiusParquetFile, records' schema will be:")
        _                      <- ZIO.log {
                                    summon[SchemaEncoder[Element]].encode(summon[Schema[Element]], "element", optional = false).toString
                                  }
        _                      <- ZIO.serviceWithZIO[ParquetWriter[Element]](_.writeStream(elementsCelsiusParquetFile, elementsStream))
        _                      <- ZIO.log(s"Reading all elements from $elementsCelsiusParquetFile")
        allElementsStream      <- ZIO.serviceWith[ParquetReader[Element]](_.readStream(elementsCelsiusParquetFile))
        _                      <- ZIO.log(s"Filtering elements from $elementsCelsiusParquetFile")
        filteredElementsStream <-
          ZIO.serviceWith[ParquetReader[Element]] {
            _.readStreamFiltered(elementsCelsiusParquetFile, filter(Element.element =!= "hydrogen"))
          }
        _                      <- ZIO.log(s"Writing filtered elements to $elementsCelsiusFilteredCSVFile")
        _                      <- writeToCsv(filteredElementsStream, elementsCelsiusFilteredCSVFile)
      yield ()
    }

  override def run = ZIO.log(s"Processing $elementsFahrenheitCSVFile") *> processor.provide(
    ParquetWriter.configured[Element](writeMode = ParquetFileWriter.Mode.OVERWRITE),
    ParquetReader.configured[Element]()
  )
