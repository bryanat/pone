package mongocrudops

import mongocrudops.Helpers._
import org.mongodb.scala._

object MongoInserts {

  /*
   * Inserts n usernames and passwords into the collectionName collection
   */
  def insertUsernamesAndPasswords(n: Int, collectionName: MongoCollection[Document]): Unit = {
    for (i <- 1 to n) {
      var m = i.toString()
      var newUser = "user"+m
      println(newUser)
      println(newUser.getClass)
      var newPass = "password"+m
      var newDoc = Document("username" -> newUser, "password" -> newPass)
      // ucollection.insertOne(newDoc).printResults()
      collectionName.insertOne(newDoc).printResults()
    }
  }

  

}