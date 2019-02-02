package com.senthil.projects.bigdata.scala

import scala.annotation.tailrec

object Factorial {

  def factorial(n: Int): Int = {
    @tailrec
    def iter(x: Int, result: Int): Int =
      if (x == 0) result
      else iter(x - 1, result * x)

    iter(n, 1)
  }

  def main(args: Array[String]): Unit = {
    println(s"factorial(3) = ${factorial(3)}")
    println(s"factorial(4) = ${factorial(4)}")
    println(s"factorial(6) = ${factorial(6)}")
  }
}
