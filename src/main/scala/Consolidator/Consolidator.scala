package Consolidator

import java.io.PrintWriter

import org.apache.spark.sql.DataFrame
import main.scala.OrderJoinnerConsolidator.{spark}
import org.apache.spark.sql.functions._
import scala.io.Source

trait Consolidator {

  //in case need to load csv file
  def consolidateRecords(file1:String, file2:String, joinType:String, condition:String=null, writeOutputToFile:Boolean=false,fileType:String="csv",location:String=null):Unit={

val df1= if(file1.endsWith(".csv"))
             loadCSVFiles(file1)
          else
             loadJsonFiles(file1)
val df2= if(file1.endsWith(".csv"))
            loadCSVFiles(file2)
        else
            loadJsonFiles(file2)

          val df= getResult(callJoiner(df1,df2,joinType,condition),df1,df2)

          generateReport(df,file1.substring(0,file1.lastIndexOf("/"))+ "/result.json")

  if (writeOutputToFile)
      writeOutput(df, fileType, location)
}


  def callJoiner(df1:DataFrame,df2:DataFrame,joinType:String,condition:String=null): DataFrame = {
  return null
  }

  private def getResult(finalDf:DataFrame,left:DataFrame,right:DataFrame):DataFrame={

    val headers=finalDf.columns
    val leftPart=finalDf.select(col(headers(0)),col(headers(1)))
    val rightPart=finalDf.select(col(headers(2)),col(headers(3)),col(headers(4)))

     left.except(leftPart).createOrReplaceTempView("leftSkipped")
    left.cache()
     right.except(rightPart).createOrReplaceTempView("rightSkipped")
    right.cache()
finalDf
  }

  private def generateReport(df:DataFrame,path:String): Unit ={
    val pw= new PrintWriter(path)
    pw.write("{\n \"count\":"+ df.count+ ",\n \"rowsSkippedLeft\":[")
    spark.sql("SELECT * FROM leftSkipped").toJSON.collect.foreach(a=>{pw.write(a.toString)})
    pw.write("],\n \"rowsSkippedRight\":[")
    spark.sql("SELECT * FROM rightSkipped").toJSON.collect.foreach(a=>{pw.write(a.toString)})
    pw.write("],\n \"finalResult\":[")
    df.toJSON.collect.foreach(a=>{pw.write(a.toString)})
    pw.write("],\n }")
    pw.close
  }

  //unpretty the json file for spark to interpret the json array
 private def loadJsonFiles(FilePath:String):DataFrame={
    val tmp =Source.fromFile(FilePath).mkString
    val sparkFriendlyJson=tmp.substring(1,tmp.length-2).replaceAll("[\n ]","").replaceAll("},","},\n")
    new PrintWriter(FilePath.substring(0,FilePath.lastIndexOf("."))+ "Processing.json") { write(sparkFriendlyJson); close }
   val df= spark.read.json(FilePath.substring(0,FilePath.lastIndexOf("."))+ "Processing.json")
    df
  }

 private def loadCSVFiles(FilePath:String): DataFrame ={
  spark.read.option("header",true).csv(FilePath)
  }

  //TODO:: this is not needed for this challenge
  //TODO: repartion is not ideal if the file is really large which could exceed memeory space
  //TODO: if it were for production, the better way is to let spark write partition files in  hadoop cluster then merge together
  def writeOutput(df:DataFrame,fileType:String,location:String):Unit={
   if (fileType.toLowerCase().replaceAll(".","")=="csv")
       df.repartition(1).write.option("header", true).csv(location)
   else
       df.repartition(1).write.json(location)

  }
}
