//> using jvm graalvm-java23:23.0.0
//> using javaOpt "--add-opens", "java.base/java.nio=ALL-UNNAMED", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED"
//> using scala 3.5.2
//> using dep fr.janalyse::zio-lmdb:1.8.2
//> using dep info.fingo::spata:3.2.1
//> using dep dev.zio::zio-interop-cats:23.1.0.3

import zio.*
import zio.interop.catz.*
import zio.lmdb.*
import zio.json.*
import zio.stream.interop.fs2z.*
import java.io.File
import info.fingo.spata.{ CSVParser, Record }
import info.fingo.spata.io.Reader
import java.nio.file.Paths
import zio.lmdb.StorageUserError.CollectionNotFound
import zio.lmdb.StorageUserError.JsonFailure
import zio.lmdb.StorageUserError.OverSizedKey

object SimpleExample extends ZIOAppDefault:
  final case class Element(element: String, symbol: String, meltingTemp: Double, boilingTemp: Double) derives JsonCodec:
    self =>

    def updateTemps(f: Double => Double) =
      self.copy(meltingTemp = f(self.meltingTemp), boilingTemp = f(self.boilingTemp))

  val csvParser: fs2.Pipe[Task, Char, Record] =
    CSVParser.config
      .mapHeader(Map("melting temperature [F]" -> "meltingTemp", "boiling temperature [F]" -> "boilingTemp"))
      .parser[Task]
      .parse

  def processor(elements: LMDBCollection[Element]): IO[
    CollectionNotFound | JsonFailure | StorageSystemError | Throwable | OverSizedKey | Option[FetchErrors],
    Unit
  ] =
    val fahrenheitCSV = "testdata/elements-fahrenheit.csv"

    def processElement(element: Element): IO[UpsertErrors | Option[FetchErrors], Option[Element]] =
      def fahrenheitToCelsius(f: Double): Double = (f - 32.0) * (5.0 / 9.0)

      elements.upsertOverwrite(element.symbol, element)
        *> elements.contains(element.symbol) @@ ZIOAspect.logged(
          s"Checking whether ${elements.name} contains ${element.symbol}"
        )
        *> elements.fetch(element.symbol).some @@ ZIOAspect.logged("Found element")
        *> elements.update(
          element.symbol,
          _.updateTemps(fahrenheitToCelsius)
        ) @@ ZIOAspect.logged("Updated element")

    ZIO.log(s"Processing $fahrenheitCSV")
      *> Reader[Task]
        .read(Paths.get(fahrenheitCSV))
        .through(csvParser)
        .toZStream()
        .mapZIO(record => ZIO.fromEither(record.to[Element]))
        .mapZIO(processElement)
        .runDrain

  val example =
    val collectionName = "elements"

    for
      _              <- LMDB.collectionExists(
                          collectionName
                        ) @@ ZIOAspect.logged(s"Checking whether collection $collectionName exists")
      elements       <- LMDB.collectionCreate[Element](
                          collectionName,
                          failIfExists = false
                        ) @@ ZIOAspect.logged(s"Created collection $collectionName")
      _              <- LMDB.collectionsAvailable() @@ ZIOAspect.logged("Available collections")
      _              <- processor(elements)
      collected      <- elements.collect() @@ ZIOAspect.logged(s"Collected all $collectionName")
      collectionSize <- elements.size() @@ ZIOAspect.logged(s"Number of collected $collectionName")
      _              <- ZIO.foreach(collected)(Console.printLine(_))
      filtered       <- elements.collect(
                          keyFilter = _.startsWith("H"),
                          valueFilter = _.meltingTemp < -15
                        ) @@ ZIOAspect.logged(s"Filtered $collectionName")
      _              <- ZIO.log(s"Clearing collection $collectionName")
      _              <- elements.clear()
      _              <- ZIO.log(s"Dropping collection $collectionName")
      _              <- LMDB.collectionDrop(collectionName)
    yield ()

  override def run = example.provide(LMDB.liveWithDatabaseName("lmdb-sample-database"), Scope.default)
