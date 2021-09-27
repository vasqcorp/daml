package com.daml.platform.store.backend

import java.sql.{Connection, DriverManager}
import scala.util._

object OracleQueryTestingApp2 extends App {
  println("hello oracle")

  val systemPwd = "hunter2"
  val port = 1521
  val testUser = "UZX4WVS3FVCSOM5OUW9UISXMPGYL6F"

  Class.forName("oracle.jdbc.driver.OracleDriver")

//  test1()
  test2()
  test3()

  def withConnection[A](user: String, password: String)(f: Connection => A): Try[A] = Using(
    DriverManager.getConnection(
      s"jdbc:oracle:thin:@localhost:$port/ORCLPDB1",
      user,
      password,
    )
  )(f)

  def withNewRandomUser(doStuff: Connection => Try[Unit]): Unit = {
    val (user, password) = createRandomUser()
    withConnection(user, password) { connection =>
      doStuff(connection)
    }.flatten match {
      case Success(_) =>
        println("SUCCESS")
      case Failure(ex) =>
        println(s"EXCEPTION: ${ex.getMessage}")
    }
  }

  def withTestUser(doStuff: Connection => Try[Unit]): Unit = {
    withConnection(testUser, systemPwd) { connection =>
      doStuff(connection)
    }.flatten match {
      case Success(_) =>
        println("SUCCESS")
      case Failure(ex) =>
        println(s"EXCEPTION: ${ex.getMessage}")
    }
  }

  def withSystemUser(doStuff: Connection => Try[Unit]): Unit = {
    val user = "system"
    val password = systemPwd
    withConnection(user, password) { connection =>
      doStuff(connection)
    }.flatten match {
      case Success(_) =>
        println("SUCCESS")
      case Failure(ex) =>
        println(s"EXCEPTION: ${ex.getMessage}")
    }
  }

  def test1(): Unit = {
    println("\n\nTEST 1")
    withNewRandomUser { connection =>
      for {
        _ <- createTestTable(connection)
        _ <- insertSampleData(connection)
        _ <- {
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
      } yield ()
    }
  }

  def test2(): Unit = {
    println("\n\nTEST 2222")
    withTestUser { connection =>
//    withNewRandomUser { connection =>
      for {
//        _ <- createTestTable(connection)
        _ <- createIndex(connection)
//        _ <- insertSampleData(connection)
        _ <- {
          println("RUNNING TEST 2")
          val parties = Set("Emma", "David")
          val placeholders = List.fill(parties.size)("?").mkString(", ")
          val sql = s"""SELECT ID
                FROM TABLE1
                WHERE EXISTS (SELECT 1 FROM JSON_TABLE(JSONCOLUMN , '$$' COLUMNS (VALUE PATH '$$[*]')) WHERE VALUE IN ($placeholders))
                """

//          val sql = s"""SELECT CONTRACT_ID
//                FROM participant_events_divulgence
//                WHERE EXISTS (SELECT 1 FROM JSON_TABLE(tree_event_witnesses , '$$' COLUMNS (VALUE PATH '$$[*]')) WHERE VALUE IN ($placeholders))
//                """
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
      } yield ()
    }
  }

  def test3(): Unit = {
    println("\n\nTEST 3")
    withTestUser { connection =>
      val parties = Set("Emma", "David")
      val placeholders = List.fill(parties.size)("?").mkString(", ")
      val sql = s"""SELECT CONTRACT_ID
                FROM participant_events_divulgence
                WHERE EXISTS (SELECT 1 FROM JSON_TABLE(tree_event_witnesses , '$$' COLUMNS (VALUE PATH '$$[*]')) WHERE VALUE IN ($placeholders))
                """
      Using(connection.prepareStatement(sql)) { stmt =>
        println(s"Executing test query")
        parties.zipWithIndex.foreach { case (party, index) =>
          stmt.setString(index + 1, party)
        }
        Using(stmt.executeQuery()) { rs =>
          println("GOT RESULT SET")
          val it = new Iterator[String] {
            override def hasNext: Boolean = rs.next()
            override def next(): String = rs.getString(1)
          }
          it.foreach(r => println(s"GOT ROW: $r"))
        }
      }.flatten
    }
  }

  def createTestTable(connection: Connection): Try[Boolean] = {
    val createTableSql = s"""CREATE TABLE "TABLE1"
                            |   (
                            |  "ID" VARCHAR2(4000) NOT NULL ENABLE,
                            |  "JSONCOLUMN" CLOB DEFAULT '[]' NOT NULL ENABLE
                            |  CONSTRAINT "ENSURE_JSON_JSONCOLUMN" CHECK (JSONCOLUMN IS JSON) ENABLE
                            |)""".stripMargin
    Using(connection.prepareStatement(createTableSql)) { stmt =>
      println("Creating table")
      stmt.execute()
    }
  }

  def createIndex(connection: Connection): Try[Boolean] = {
    val sql = """CREATE INDEX "JSONCOLUMN_IDX" ON TABLE1 ("JSONCOLUMN")
                |   INDEXTYPE IS "CTXSYS"."CONTEXT_V2"  PARAMETERS ('SIMPLIFIED_JSON')""".stripMargin

    Using(connection.prepareStatement(sql)) { stmt =>
      println("Creating index")
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
