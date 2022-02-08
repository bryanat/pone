//package samepackageequalsautoimportsacrosscompilers
package acrosscompilers

object AutoImportMe {
  def someMethod(): Unit = {
    val thisWorks = new SharedClass
    thisWorks.sayGoodbye()
    println("from the jvm compiler")
    println(s"Using a JVM version ${System.getProperty("java.vm.version")}")
  }
}
