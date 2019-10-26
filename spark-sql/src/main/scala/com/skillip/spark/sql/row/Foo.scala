package com.skillip.spark.sql.row

class Foo(foo: String) {
}

object Foo {
  def apply(foo: String, id: Int) = foo + id
}


object MyTest {
  def main(args: Array[String]): Unit = {
    val i = Foo("test", 1)
    println(i)
  }
}