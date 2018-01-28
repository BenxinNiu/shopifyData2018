package Consolidator

import main.scala.OrderJoinnerConsolidator.spark

import org.apache.spark.sql.DataFrame

object sqlJoiner extends Consolidator {

  override def callJoiner(df1:DataFrame,df2:DataFrame,joinType:String,condition:String=null): DataFrame ={

    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")

   val df=joinType.toLowerCase match{
     case "left_outer" => leftJoin(condition)
     case "left" => leftJoin(condition)
     case "right_outer" => rightJoin(condition)
     case "right" => rightJoin(condition)
     case "inner" => innerJoin(condition)
   }
    df
  }

  // use spark sql
 private def leftJoin(condition:String=null):DataFrame={
   spark.sql(generateSqlStr("left",condition))
  }

 private def rightJoin(condition:String=null):DataFrame={
   spark.sql(generateSqlStr("right",condition))
  }

 private def innerJoin(condition:String=null):DataFrame={
   spark.sql(generateSqlStr("inner",condition))
  }

  private def generateSqlStr(joinType:String,condition:String):String={
    val join= joinType match{
      case "left"=> "LEFT JOIN"
      case "right"=>"RIGHT JOIN"
      case "inner"=>"INNER JOIN"
    }

    val sql= new StringBuilder()
      .append("SELECT *")
      .append(" FROM df1 ")
      .append(join)
      .append(" df2")
    if(condition!=null)
      sql.append(" ON ").append(condition)
    sql.toString
  }
}
