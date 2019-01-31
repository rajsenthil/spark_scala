package com.senthil.projects.bigdata.scala

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, Period}
import java.util.Scanner

object LearningScala1 {

  def main(args: Array[String]): Unit = {
    val picard1 = "Picard"
    val picard2 = "Picard"
    println(f"Result ${picard1 == picard2}")

    println("Enter Birthdate (yyyy/mm/dd): ")
    val scanner = new Scanner(System.in)
    val birthDate = scanner.nextLine
    val format = DateTimeFormatter.ofPattern("yyyy/MM/dd")
//    val format = DateTimeFormatter.BASIC_ISO_DATE
    val period = Period.between(LocalDate.parse(birthDate, format), LocalDate.now())
    println(s"Age is ${period.getYears}")
  }
}
