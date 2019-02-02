package com.senthil.projects.bigdata.scala

object LearningScala2 {

  def squareIt(x: Int) : Int = {
    x*x
  }

  def cubeIt(x: Int) : Int = {
    x*x*x
  }

  def transformInt(x: Int, f: Int => Int) : Int = {
    f(x)
  }

  def main(args: Array[String]): Unit = {
    println(s"cube(2) = ${transformInt(2, cubeIt)}")
    println(s"square(3) = ${transformInt(3, squareIt)}")
    println(s"cube(3) = ${transformInt(3, x=>x*x*x)}")
    println(s"transformInt(3, x => {val y = x * 2; y*y} ) = ${transformInt(3, x => {val y = x * 2; y*y} )}")
  }
}
