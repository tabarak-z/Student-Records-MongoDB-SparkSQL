
import org.mongodb.scala._
import org.mongodb.scala.model.Indexes
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.bson._
import org.mongodb.scala.bson.collection.immutable.Document

import org.bson.conversions.Bson

import scala.io.Source
import scala.io.StdIn
import scala.concurrent.Await
import scala.concurrent.duration._

import java.time.LocalDate
import java.time.ZoneOffset
import java.io.PrintWriter

object MongoApp {

  case class Student(
    id: Int,
    firstName: String,
    lastName: String,
    dateOfBirth: LocalDate,
    courseIds: Seq[Int],
    gpa: Double
  ) {

    def toDocument: Document = {
      Document(
        "_id" -> id,
        "firstName" -> firstName,
        "lastName" -> lastName,
        "dateOfBirth" -> BsonDateTime(
          dateOfBirth.atStartOfDay(ZoneOffset.UTC)
            .toInstant.toEpochMilli
        ),
        "courseIds" -> courseIds,
        "gpa" -> gpa
      )
    }

    def toJsonLine: String = {
      s"""{"_id":$id,"firstName":"$firstName","lastName":"$lastName","dateOfBirth":"${dateOfBirth}T00:00:00Z","courseIds":[${courseIds.mkString(",")}],"gpa":$gpa}"""
    }
  }

  def parseStudent(line: String): Student = {

    val openBracket = line.indexOf("[")
    val closeBracket = line.indexOf("]")

    val firstPart =
      line.substring(0, openBracket).split(",").map(_.trim)

    val coursesPart =
      line.substring(openBracket + 1, closeBracket)

    val gpaPart =
      line.substring(closeBracket + 1).stripPrefix(",").trim

    val id = firstPart(0).toInt
    val firstName = firstPart(1)
    val lastName = firstPart(2)
    val dateOfBirth = LocalDate.parse(firstPart(3))

    val courseIds =
      coursesPart.split(",").map(_.trim.toInt).toSeq

    val gpa = gpaPart.toDouble

    Student(
      id,
      firstName,
      lastName,
      dateOfBirth,
      courseIds,
      gpa
    )
  }

  def readInt(message: String): Int = {

    var number = 0
    var valid = false
    while (!valid) {

      print(message)
      val input = StdIn.readLine()

   try {
  number = input.trim.toInt
  valid = true
} catch {
  case e: NumberFormatException =>
    println("Please enter a valid integer!")
}
    }

    number
  }

def readGPA(message: String): Double = {
  var gpa = 0.0
  var valid = false
  while (!valid) {
    print(message)
    val input = StdIn.readLine()
    try {
      gpa = input.trim.toDouble
      if (gpa >= 0.0 && gpa <= 4.0) {
        valid = true
      } else {
        println("GPA must be a number between 0.0 and 4.0")
      }
    } catch {
      case e: NumberFormatException =>
        println("GPA must be a number between 0.0 and 4.0")
    }
  }
  gpa
}
  def displayStudents(students: Seq[Document]): Unit = {
    students.foreach { student =>

      val id = student.getInteger("_id")
      val firstName = student.getString("firstName")
      val lastName = student.getString("lastName")
      val gpa = student.getDouble("gpa")

      println(
        s"ID: $id | Name: $firstName $lastName | GPA: $gpa"
      )
    }

    println("Number of matches: " + students.length)
  }

  def main(args: Array[String]): Unit = {
    val mongoClient = MongoClient("mongodb://localhost:27017")
    val database = mongoClient.getDatabase("university")
    val collection = database.getCollection("students")

    try {

      // PART 1
      val fileName =
        if (args.length > 0) args(0)
        else "students.csv"
      val source = Source.fromFile(fileName)

      val lines = try {
        source.getLines().drop(1).filter(_.trim.nonEmpty).toList
      } finally {
        source.close()
      }

      val students = lines.map(parseStudent)
      val documents = students.map(_.toDocument)

      Await.result(
        collection.drop().toFuture(),
        Duration.Inf
      )

      Await.result(
        collection.insertMany(documents).toFuture(),
        Duration.Inf
      )

      println("Number of inserted students: " + students.length)

      Await.result(
        collection.createIndex(
          Indexes.ascending("courseIds")
        ).toFuture(),
        Duration.Inf
      )

      Await.result(
        collection.createIndex(
          Indexes.ascending("gpa")
        ).toFuture(),
        Duration.Inf
      )

      val writer = new PrintWriter("students.json")

      try {
        students.foreach { student =>
          writer.println(student.toJsonLine)
        }
      } finally {
        writer.close()
      }

      val indexes = Await.result(
        collection.listIndexes().toFuture(),
        Duration.Inf
      )

      println("\nIndexes:")

      indexes.foreach(println)

      // PART 2
      val courseId = readInt("Enter Course ID: ")
      val courseFilter = equal("courseIds", courseId)

      val courseStudents = Await.result(
        collection.find(courseFilter)
          .sort(ascending("lastName"))
          .toFuture(),
        Duration.Inf
      )

      println("\nStudents registered in course " + courseId)
      displayStudents(courseStudents)

      val explainResult = Await.result(
        collection.find(courseFilter).explain().toFuture(),
        Duration.Inf
      )

      if (explainResult.toString.contains("IXSCAN"))
        println("courseIds index was used (IXSCAN).")
      else
        println("courseIds index was not used.")

      // PART 3
      val minimumGPA =
        readGPA("Enter minimum GPA (0.0 - 4.0): ")

      val gpaFilter = gt("gpa", minimumGPA)

      val gpaStudents = Await.result(
        collection.find(gpaFilter)
          .sort(descending("gpa"))
          .toFuture(),
        Duration.Inf
      )

      println("\nStudents with GPA greater than " + minimumGPA)

      displayStudents(gpaStudents)

      // COMBINED QUERY
      println("\n========== COMBINED QUERY ==========")
var validCombination = false

var combinedCourse = 0
var combinedGPA = 0.0

var hasCourse = false
var hasGPA = false

while (!validCombination) {

  println("Enter Course ID (or press Enter to skip):")
  val courseInput = StdIn.readLine().trim

  var courseValid = true
  var gpaValid = true

  hasCourse = false
  hasGPA = false

  if (courseInput.nonEmpty) {

    try {
      combinedCourse = courseInput.toInt
      hasCourse = true
    } catch {
      case e: NumberFormatException =>
        println("Invalid Course ID!")
        courseValid = false
    }
  }

  println("Enter minimum GPA (or press Enter to skip):")
  val gpaInput = StdIn.readLine().trim

  if (gpaInput.nonEmpty) {

    try {
      combinedGPA = gpaInput.toDouble

      if (combinedGPA >= 0.0 && combinedGPA <= 4.0) {
        hasGPA = true
      } else {
        println("Invalid GPA!")
        gpaValid = false
      }

    } catch {
      case e: NumberFormatException =>
        println("Invalid GPA!")
        gpaValid = false
    }
  }

  if (courseValid && gpaValid && (hasCourse || hasGPA)) {
    validCombination = true
  } else {
    println("Please enter valid conditions!")
  }
}

var combinedFilter: Bson = equal("_id", -1)

if (hasCourse && hasGPA) {

  combinedFilter = and(
    equal("courseIds", combinedCourse),
    gt("gpa", combinedGPA)
  )

} else if (hasCourse) {

  combinedFilter = equal("courseIds", combinedCourse)

} else if (hasGPA) {

  combinedFilter = gt("gpa", combinedGPA)
}

      val combinedStudents = Await.result(
        collection.find(combinedFilter)
          .sort(ascending("lastName"))
          .toFuture(),
        Duration.Inf
      )

      println("\nCombined Query Results:")

      displayStudents(combinedStudents)

      println("\nProgram finished successfully!")

    } finally {
      mongoClient.close()
    }
  }
}
