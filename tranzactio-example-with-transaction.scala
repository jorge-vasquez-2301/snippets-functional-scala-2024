//> using jvm graalvm-java11:22.3.3
//> using scala 3.5.2
//> using dep org.tpolecat::doobie-core:1.0.0-RC5
//> using dep org.tpolecat::doobie-h2:1.0.0-RC5
//> using dep io.github.gaelrenoux::tranzactio-doobie:5.2.0

import zio.*
import zio.stream.*
import zio.interop.catz.*
import doobie.*
import doobie.implicits.*
import io.github.gaelrenoux.tranzactio.*
import io.github.gaelrenoux.tranzactio.doobie.*
import org.h2.jdbcx.JdbcDataSource

// Based on https://github.com/typelevel/doobie/blob/main/modules/example/src/main/scala/example/FirstExample.scala
object TranzactioExampleWithTransactions extends ZIOAppDefault:

  final case class Supplier(id: Int, name: String, street: String, city: String, state: String, zip: String)
  final case class Coffee(name: String, supplierId: Int, price: Double, sales: Int, total: Int)

  val suppliers =
    List(
      Supplier(101, "Acme, Inc.", "99 Market Street", "Groundsville", "CA", "95199"),
      Supplier(49, "Superior Coffee", "1 Party Place", "Mendocino", "CA", "95460"),
      Supplier(150, "The High Ground", "100 Coffee Lane", "Meadows", "CA", "93966")
    )

  val coffees =
    List(
      Coffee("Colombian", 101, 7.99, 0, 0),
      Coffee("French_Roast", 49, 8.99, 0, 0),
      Coffee("Espresso", 150, 9.99, 0, 0),
      Coffee("Colombian_Decaf", 101, 8.99, 0, 0),
      Coffee("French_Roast_Decaf", 49, 9.99, 0, 0)
    )

  val dataSourceLayer =
    ZLayer.succeed {
      val dataSource = JdbcDataSource()
      dataSource.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
      dataSource.setUser("sa")
      dataSource.setPassword("")
      dataSource
    }

  val program: ZIO[Database, DbException, Unit] =
    for
      _                                    <- Database.transactionOrDie(Repository.create)
      (numberOfSuppliers, numberOfCoffees) <- Database.transactionOrDie {
                                                Repository.insertSuppliers(suppliers) <*> Repository.insertCoffees(
                                                  coffees
                                                )
                                              }
      _                                    <- ZIO.log(s"Inserted $numberOfSuppliers suppliers and $numberOfCoffees coffees")
      _                                    <- ZIO.log("Getting all coffees")
      _                                    <- ZStream
                                                .fromIterableZIO(Database.transactionOrDie(Repository.allCoffees))
                                                .mapZIO(coffee => Console.printLine(coffee).orDie)
                                                .runDrain
      _                                    <- ZIO.log("Getting cheap coffees")
      _                                    <- ZStream
                                                .fromIterableZIO(Database.transactionOrDie(Repository.coffeesLessThan(9.0)))
                                                .mapZIO(coffee => Console.printLine(coffee).orDie)
                                                .runDrain
    yield ()

  val run = program.provide(Database.fromDatasource, dataSourceLayer)

  object Repository:
    def coffeesLessThan(price: Double): TranzactIO[List[(String, String)]] =
      tzio(Queries.coffeesLessThan(price))

    def insertSuppliers(suppliers: List[Supplier]): TranzactIO[Int] =
      tzio(Queries.insertSuppliers(suppliers))

    def insertCoffees(coffees: List[Coffee]): TranzactIO[Int] =
      tzio(Queries.insertCoffees(coffees))

    val allCoffees: TranzactIO[List[Coffee]] =
      tzio(Queries.allCoffees)

    val create: TranzactIO[Unit] =
      tzio(Queries.create).unit

  object Queries:

    def coffeesLessThan(price: Double): ConnectionIO[List[(String, String)]] =
      sql"""
        SELECT cof_name, sup_name
        FROM coffees JOIN suppliers ON coffees.sup_id = suppliers.sup_id
        WHERE price < $price
      """.query[(String, String)].to[List]

    def insertSuppliers(suppliers: List[Supplier]): ConnectionIO[Int] =
      Update[Supplier]("INSERT INTO suppliers VALUES (?, ?, ?, ?, ?, ?)").updateMany(suppliers)

    def insertCoffees(coffees: List[Coffee]): ConnectionIO[Int] =
      Update[Coffee]("INSERT INTO coffees VALUES (?, ?, ?, ?, ?)").updateMany(coffees)

    val allCoffees: ConnectionIO[List[Coffee]] =
      sql"SELECT cof_name, sup_id, price, sales, total FROM coffees".query[Coffee].to[List]

    val create: ConnectionIO[Int] =
      sql"""
        CREATE TABLE suppliers (
          sup_id   INT     NOT NULL PRIMARY KEY,
          sup_name VARCHAR NOT NULL,
          street   VARCHAR NOT NULL,
          city     VARCHAR NOT NULL,
          state    VARCHAR NOT NULL,
          zip      VARCHAR NOT NULL
        );
        CREATE TABLE coffees (
          cof_name VARCHAR NOT NULL,
          sup_id   INT     NOT NULL,
          price    DOUBLE  NOT NULL,
          sales    INT     NOT NULL,
          total    INT     NOT NULL
        );
        ALTER TABLE coffees
        ADD CONSTRAINT coffees_suppliers_fk FOREIGN KEY (sup_id) REFERENCES suppliers(sup_id);
      """.update.run
