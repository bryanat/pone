//package example
import acrosscompilers.SharedClass

object Main {
  def main(args: Array[String]): Unit = {
    val thisWorks = new SharedClass
    thisWorks.sayGoodbye()
    println("from the js compiler")
    println(s"Using Scala.js version ${System.getProperty("java.vm.version")}")
  }
}