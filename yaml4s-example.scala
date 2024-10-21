//> using jvm graalvm-java23:23.0.0
//> using scala 3.5.1
//> using dep dev.zio::zio:2.1.11
//> using dep dev.zio::zio-streams:2.1.11
//> using dep dev.zio::zio-json:0.7.3
//> using dep dev.hnaderi::yaml4s-backend:0.3.0
//> using dep dev.hnaderi::yaml4s-zio-json:0.3.0

import zio.*
import zio.stream.*
import zio.json.*
import zio.json.ast.*
import dev.hnaderi.yaml4s.*
import dev.hnaderi.yaml4s.ziojson.*

object Yaml4sExample extends ZIOAppDefault:
  final case class Friend(id: Int, name: String)
  object Friend:
    given JsonCodec[Friend] = DeriveJsonCodec.gen[Friend]

  final case class Person(
    id: String,
    index: Int,
    guid: String,
    isActive: Boolean,
    balance: String,
    picture: String,
    age: Int,
    eyeColor: String,
    name: String,
    gender: String,
    company: String,
    email: String,
    phone: String,
    address: String,
    about: String,
    registered: String,
    latitude: Double,
    longitude: Double,
    tags: List[String],
    friends: List[Friend],
    greeting: String,
    favoriteFruit: String
  )

  object Person:
    given JsonCodec[Person] = DeriveJsonCodec.gen[Person]

  def readFile(fileName: String): Task[String] =
    ZStream.fromFileName(fileName).via(ZPipeline.utf8Decode).runCollect.map(_.mkString)

  def printFile(contents: String, fileName: String): Task[Long] =
    ZStream(contents).via(ZPipeline.utf8Encode).run(ZSink.fromFileName(fileName))

  val run =
    for
      yamlStr              <- readFile("testdata/people.yaml")
      yaml                 <- ZIO.fromEither(Backend.parse[YAML](yamlStr))
      json                 <- ZIO.fromEither(Backend.parse[Json](yamlStr))
      people               <- ZIO.fromEither(json.as[List[Person]])
      filteredPeople        = people.filter(_.tags.contains("est"))
      filteredPeopleJson   <- ZIO.fromEither(filteredPeople.toJsonAST)
      filteredPeopleYamlStr = Backend.print(filteredPeopleJson)
      _                    <- printFile(filteredPeopleYamlStr, "testdata/filteredPeople.yaml")
    yield ()
