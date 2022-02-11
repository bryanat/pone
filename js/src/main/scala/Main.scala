
//package acrosscompilers

import acrosscompilers.SharedClass

// import scala.scalajs.js
// import org.scalajs.dom
// import org.singlespaced.d3js.d3
// import org.singlespaced.d3js.Ops._


//object ScalaJSExample extends js.JSApp {

object Main {
  def main(args: Array[String]): Unit = {
    val thisWorks = new SharedClass
    thisWorks.sayGoodbye()
    println("from the js compiler")
    println(s"Using Scala.js version ${System.getProperty("java.vm.version")}")

    // val sel=d3.selectAll("div").data(js.Array(5,2,4,6))
    // sel.style("width", (d:Int) => d*2 )
    
  }
}