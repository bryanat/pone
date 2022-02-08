//package samepackageequalsautoimportsacrosscompilers
package acrosscompilers

object AutoImportMe {
  def someMethod(): Unit = {
    val thisWorks = new SharedClass
    thisWorks.sayGoodbye()
    println("from the js compiler")
    println(s"Using Scala.js version ${System.getProperty("java.vm.version")}")
  }
}
