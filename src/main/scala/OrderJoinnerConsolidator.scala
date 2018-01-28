
package main.scala
import Consolidator.sqlJoiner
import org.apache.spark.sql.SparkSession

/*TODO:: before do some coding ask urself this:

  will working at shopify makes you to go to shopping more frequently??

  TODO:: answer: Maybe.. but hey.. i love shopping anyways, why not help shopify's merchants to sell more stuff lol

*/
object OrderJoinnerConsolidator extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("shopifyDataChallenge")
    .getOrCreate()

  //val file1 = "/home/benxin/shop/customers.json"
 //val file2 = "/home/benxin/shop/orders.json"

  val condition=args(3)
  val file1= args(0)
  val file2= args(1)
  val joinType =args(2)

   sqlJoiner.consolidateRecords(file1,file2,joinType,condition)
}
