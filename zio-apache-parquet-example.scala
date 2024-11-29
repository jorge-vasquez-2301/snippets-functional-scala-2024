//> using jvm graalvm-java11:22.3.3
//> using scala 3.5.2
//> using dep me.mnedokushev::zio-apache-parquet-core:0.1.4
//> using dep dev.zio::zio-interop-cats:23.1.0.3
//> using dep info.fingo::spata:3.2.1
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
    given schema: Schema.CaseClass4.WithFields[
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
    given schemaEncoder: SchemaEncoder[Element] = Derive.derive[SchemaEncoder, Element](SchemaEncoderDeriver.default)

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

  val fahrenheitCSVFile      = Paths.get("testdata/elements-fahrenheit.csv")
  val celsiusParquetFile     = Path(Paths.get("testdata/elements-celsius.parquet"))
  val celsiusFilteredCSVFile = Paths.get("testdata/elements-celsius-filtered.csv")

  val readFromCsvAndProcess: ZStream[Scope, Throwable, Element] =
    def fahrenheitToCelsius(f: Double): Double = (f - 32.0) * (5.0 / 9.0)

    Reader[Eff]
      .read(fahrenheitCSVFile)
      .through(csvParser)
      .toZStream()
      .mapZIO(record => ZIO.fromEither(record.to[Element]))
      .map(_.updateTemps(fahrenheitToCelsius))

  def writeStreamToCsv(stream: ZStream[Scope, Throwable, Element], path: java.nio.file.Path) =
    stream
      .map(_.toRecord)
      .toFs2Stream
      .through(csvRenderer)
      .through(Writer[Eff].write(path))
      .compile
      .drain

  val processor: RIO[ParquetWriter[Element] & ParquetReader[Element], Unit] =
    ZIO.scoped {
      for
        _                      <- ZIO.log(s"Writing all elements to $celsiusParquetFile")
        _                      <- ZIO.log(s"The Parquet schema will be:")
        parquetSchema           = Element.schemaEncoder.encode(Element.schema, "element", optional = false)
        _                      <- ZIO.log(parquetSchema.toString)
        //                      required group element {
        //                        required binary element (STRING);
        //                        required binary symbol (STRING);
        //                        required double meltingTemp;
        //                        required double boilingTemp;
        //                      }
        _                      <- ZIO.serviceWithZIO[ParquetWriter[Element]] {
                                    _.writeStream(celsiusParquetFile, readFromCsvAndProcess)
                                  }
        _                      <- ZIO.log(s"Reading all elements from $celsiusParquetFile, as a Chunk")
        allElementsChunk       <- ZIO.serviceWith[ParquetReader[Element]](_.readChunk(celsiusParquetFile))
        _                      <- ZIO.log(s"Reading all elements from $celsiusParquetFile, as a ZStream")
        allElementsStream      <- ZIO.serviceWith[ParquetReader[Element]](_.readStream(celsiusParquetFile))
        _                      <- ZIO.log(s"Reading/filtering elements from $celsiusParquetFile, as a Chunk")
        filteredElementsChunk  <- ZIO.serviceWith[ParquetReader[Element]] {
                                    _.readChunkFiltered(
                                      celsiusParquetFile,
                                      filter(Element.element =!= "hydrogen" `and` Element.meltingTemp > 0)
                                    )
                                  }
        _                      <- ZIO.log(s"Reading/filtering elements from $celsiusParquetFile, as a ZStream")
        filteredElementsStream <- ZIO.serviceWith[ParquetReader[Element]] {
                                    _.readStreamFiltered(
                                      celsiusParquetFile,
                                      filter(Element.element =!= "hydrogen" `and` Element.meltingTemp > 0)
                                    )
                                  }
        _                      <- ZIO.log(s"Writing filtered elements to $celsiusFilteredCSVFile")
        _                      <- writeStreamToCsv(filteredElementsStream, celsiusFilteredCSVFile)
      yield ()
    }

  override def run =
    ZIO.log(s"Processing $fahrenheitCSVFile")
      *> processor.provide(
        ParquetWriter.configured[Element](writeMode = ParquetFileWriter.Mode.OVERWRITE),
        ParquetReader.configured[Element]()
      )
