package com.daml.platform.store.backend

import java.sql.{Connection, DriverManager}
import scala.util._

object OracleQueryTestingApp extends App {
  println("hello oracle")

  val systemPwd = "hunter2"
  val port = 1521

  Class.forName("oracle.jdbc.driver.OracleDriver")

  def withConnection[A](user: String, password: String)(f: Connection => A): Try[A] = Using(
    DriverManager.getConnection(
      s"jdbc:oracle:thin:@localhost:$port/ORCLPDB1",
      user,
      password,
    )
  )(f)

  val (user, password) = createRandomUser()

  withConnection(user, password) { connection =>
    for {
      _ <- createTestTable(connection)
      _ <- insertSampleData(connection)
      _ <- executeTest2(connection)
    } yield 1
  } match {
    case Success(_) =>
      println("OK")
    case Failure(ex) =>
      println(s"EXCEPTION: $ex")
      throw ex
  }

  def createTestTable(connection: Connection): Try[Boolean] = {
    val createTableSql = s"""CREATE TABLE "TABLE1"
                            |   (
                            |  "ID" VARCHAR2(4000) NOT NULL ENABLE,
                            |  "JSONCOLUMN" CLOB CONSTRAINT ensure_json CHECK (JSONCOLUMN IS JSON) NOT NULL ENABLE
                            |)""".stripMargin
    Using(connection.prepareStatement(createTableSql)) { stmt =>
      println("Creating table")
      stmt.execute()
    }
  }

  def insertSampleData(connection: Connection): Try[Boolean] = {
    val insertSql = s"""INSERT INTO TABLE1
                           |(ID, JSONCOLUMN)
                           |VALUES('cid1', '["Emma"]' )""".stripMargin
    Using(connection.prepareStatement(insertSql)) { stmt =>
      println(s"Inserting sample data")
      stmt.execute()
    }
  }

  def executeTest1(connection: Connection): Try[Boolean] = {

    val sql = s"""SELECT COUNT(*)
                FROM TABLE1
                WHERE EXISTS (SELECT 1 FROM JSON_TABLE(JSONCOLUMN , '$$[*]' COLUMNS (VALUE PATH '$$')) WHERE VALUE IN (?))
                """
    Using(connection.prepareStatement(sql)) { stmt =>
      println(s"Executing test query")
      stmt.setString(1, "Emma")
      stmt.execute()
    }
  }

  def executeTest2(connection: Connection): Try[Unit] = {
    val parties = Set("Emma", "David")
    val placeholders = List.fill(parties.size)("?").mkString(", ")
//    val sql = s"""SELECT ID
//                FROM TABLE1
//                WHERE EXISTS (SELECT 1 FROM JSON_TABLE(JSONCOLUMN , '$$[*]' COLUMNS (VALUE PATH '$$')) WHERE VALUE IN ($placeholders))
//                """
val sql = s"""SELECT ID
                FROM TABLE1
                WHERE EXISTS (SELECT 1 FROM JSON_TABLE(JSONCOLUMN , '$$' COLUMNS (VALUE PATH '$$[*]')) WHERE VALUE IN ($placeholders))
                """
    Using(connection.prepareStatement(sql)) { stmt =>
      println(s"Executing test query")
      parties.zipWithIndex.foreach { case (party, index) =>
        stmt.setString(index + 1, party)
      }
      Using(stmt.executeQuery()) { rs =>
        val it = new Iterator[String] {
          override def hasNext: Boolean = rs.next()
          override def next(): String = rs.getString(1)
        }
        it.foreach(r => println(s"GOT ROW: $r"))
      }
    }.flatten
  }

  def createRandomUser(): (String, String) = {
    val u = "TEST" + Random.alphanumeric.take(26).mkString("")
    createNewUser(u.toUpperCase)
  } match {
    case Success((user, pass)) =>
      println(s"CREATED USER: $user")
      (user, pass)
    case Failure(ex) =>
      println(s"CANNOT CREATE USER")
      throw ex
  }

  def createNewUser(name: String, pwd: String = "hunter2"): Try[(String, String)] = {
    withConnection("sys as sysdba", systemPwd) { connection =>
      val stmt = connection.createStatement()
      stmt.execute(s"""create user $name identified by $pwd""")
      stmt.execute(s"""grant connect, resource to $name""")
      stmt.execute(
        s"""grant create table, create materialized view, create view, create procedure, create sequence, create type to $name"""
      )
      stmt.execute(s"""alter user $name quota unlimited on users""")

      // for DBMS_LOCK access
      stmt.execute(s"""GRANT EXECUTE ON SYS.DBMS_LOCK TO $name""")
      stmt.execute(s"""GRANT SELECT ON V_$$MYSTAT TO $name""")
      stmt.execute(s"""GRANT SELECT ON V_$$LOCK TO $name""")

      (name, pwd)
    }
  }

  protected def dropUser(name: String): Unit = {
    Using.Manager { use =>
      val con = use(
        DriverManager.getConnection(
          s"jdbc:oracle:thin:@localhost:$port/ORCLPDB1",
          "system",
          systemPwd,
        )
      )
      val stmt = use(con.createStatement())
      stmt.execute(s"""drop user $name cascade""")
    }.get
    ()
  }

}
