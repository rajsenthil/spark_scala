package com.senthil.projects.bigdata.scala

object ScalaData {

  val tuple = (1, "First Name", "Last Name", "2011/01/01")

  println(tuple._1)

  val shipList = List("Enterprise", "Defiant", "Voyager", "Deep Space Nine")
  println(shipList(1))
  println(shipList.head)
  println(shipList.tail)

  val reverseShipsInList = shipList.map((ship: String) => {ship.reverse})
  println(reverseShipsInList)

  val numberList = List(1, 2, 3, 4, 5)              //> numberList  : List[Int] = List(1, 2, 3, 4, 5)
  val sum = numberList.reduce( (x: Int, y: Int) => x + y)
  println(sum)

  val reverseList = shipList.reverse
  println(reverseList)

  val sortedList = shipList.sorted
  println(sortedList)

  val list = List(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
  val listby3 = list.filter(x=> x%3== 0)
  println(listby3)


  def main(args: Array[String]): Unit = {
  }
}
