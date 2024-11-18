//> using jvm graalvm-java11:22.3.3
//> using scala 3.5.2
//> using dep org.tpolecat::doobie-core:1.0.0-RC5
//> using dep org.tpolecat::doobie-h2:1.0.0-RC5
//> using dep dev.zio::zio:2.1.11
//> using dep dev.zio::zio-streams:2.1.11
//> using dep dev.zio::zio-interop-cats:23.1.0.3

import zio.*
import zio.stream.*
import zio.interop.catz.*
import doobie.*
import doobie.implicits.*
import doobie.h2.H2Transactor

object DoobieExampleWithTransactions extends ZIOAppDefault:

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

  val transactorLayer: TaskLayer[Transactor[Task]] =
    ZLayer.scoped {
      ZIO.executorWith { executor =>
        H2Transactor
          .newH2Transactor[Task](
            url = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1",
            user = "sa",
            pass = "",
            connectEC = executor.asExecutionContext
          )
          .toScopedZIO
      }
    }

  val run =
    (for
      _                                    <- Repository.create
      (numberOfSuppliers, numberOfCoffees) <- Repository.insertSuppliersAndCoffees(suppliers, coffees)
      _                                    <- ZIO.log(s"Inserted $numberOfSuppliers suppliers and $numberOfCoffees coffees")
      _                                    <- ZIO.log("Getting all coffees")
      _                                    <- ZStream.fromIterableZIO(Repository.allCoffees).mapZIO(Console.printLine(_)).runDrain
      _                                    <- ZIO.log("Getting cheap coffees")
      _                                    <- ZStream.fromIterableZIO(Repository.coffeesLessThan(9.0)).mapZIO(Console.printLine(_)).runDrain
    yield ()).provideLayer(transactorLayer)

  object Repository:
    extension [A](connectionIO: ConnectionIO[A])
      def transactZIO: RIO[Transactor[Task], A] = ZIO.serviceWithZIO[Transactor[Task]](connectionIO.transact)

    def coffeesLessThan(price: Double): RIO[Transactor[Task], List[(String, String)]] =
      Queries.coffeesLessThan(price).transactZIO

    def insertSuppliers(suppliers: List[Supplier]): RIO[Transactor[Task], Int] =
      Queries.insertSuppliers(suppliers).transactZIO

    def insertCoffees(coffees: List[Coffee]): RIO[Transactor[Task], Int] =
      Queries.insertCoffees(coffees).transactZIO

    def insertSuppliersAndCoffees(suppliers: List[Supplier], coffees: List[Coffee]): RIO[Transactor[Task], (Int, Int)] =
      Queries.insertSuppliersAndCoffees(suppliers, coffees).transactZIO

    val allCoffees: RIO[Transactor[Task], List[Coffee]] =
      Queries.allCoffees.transactZIO

    val create: RIO[Transactor[Task], Unit] =
      Queries.create.transactZIO.unit

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

    def insertSuppliersAndCoffees(suppliers: List[Supplier], coffees: List[Coffee]): ConnectionIO[(Int, Int)] =
      for
        suppliers <- insertSuppliers(suppliers)
        coffees   <- insertCoffees(coffees)
      yield (suppliers, coffees)

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
