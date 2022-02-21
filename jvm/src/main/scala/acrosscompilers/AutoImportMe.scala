//package samepackageequalsautoimportsacrosscompilers
package acrosscompilers

object AutoImportMe {
  def someMethod(): Unit = {

    SharedClass.sayGoodbye()

  }
}
