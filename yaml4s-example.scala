//> using jvm graalvm-java23:23.0.0
//> using scala 3.5.2
//> using dep dev.hnaderi::yaml4s-backend:0.3.0
//> using dep dev.hnaderi::yaml4s-zio-json:0.3.0

import zio.*
import zio.stream.*
import zio.json.*
import zio.json.ast.*
import dev.hnaderi.yaml4s.*
import dev.hnaderi.yaml4s.ziojson.*

object Yaml4sExample extends ZIOAppDefault:
  final case class Friend(id: Int, name: String) derives JsonCodec

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
  ) derives JsonCodec

  def readFile(fileName: String): Task[String] =
    ZStream.fromFileName(fileName).via(ZPipeline.utf8Decode).runCollect.map(_.mkString)

  def printFile(contents: String, fileName: String): Task[Long] =
    ZStream(contents).via(ZPipeline.utf8Encode).run(ZSink.fromFileName(fileName))

  val peopleYaml         = "testdata/people.yaml"
  val filteredPeopleYaml = "testdata/filteredPeople.yaml"

  val run =
    for
      _                    <- ZIO.log(s"Processing $peopleYaml")
      yamlStr              <- readFile(peopleYaml)
      yaml                 <- ZIO.fromEither(Backend.parse[YAML](yamlStr))
      json                 <- ZIO.fromEither(Backend.parse[Json](yamlStr))
      people               <- ZIO.fromEither(json.as[List[Person]])
      filteredPeople        = people.filter(_.tags.contains("est"))
      filteredPeopleJson   <- ZIO.fromEither(filteredPeople.toJsonAST)
      filteredPeopleYamlStr = Backend.print(filteredPeopleJson)
      _                    <- ZIO.log(s"Writing $filteredPeopleYaml")
      _                    <- printFile(filteredPeopleYamlStr, filteredPeopleYaml)
    yield ()
