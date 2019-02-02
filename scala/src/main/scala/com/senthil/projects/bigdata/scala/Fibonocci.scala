package com.senthil.projects.bigdata.scala

import java.util.Scanner

import scala.annotation.tailrec

object Fibonocci {

  /*
   * 0,1,2,3,4,5,......nth
   * 1,1,2,3,5,8,.........
   */
  def fib(num: Int) : Int = {
    @tailrec
    def  fibHelper(num: Int, prev: Int, curr: Int): Int = {
      if (num <= 0) curr
      else fibHelper(num-1, prev = prev+curr, curr = prev)
    }
    fibHelper(num, 1, 0);
  }

  def main(args: Array[String]): Unit = {
    println("Enter number of items")
    val scanner = new Scanner(System.in);
    val num = scanner.nextLine.toInt

    println(s"$num th fibonacci is ${fib(num)}")

  }
}
