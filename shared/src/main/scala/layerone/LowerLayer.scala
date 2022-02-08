package layerone

class MyLibrary {
  def doSomething(word: String): String = {
    return s"$word do something in a lower layer used by both jvm and js"
  }
}
