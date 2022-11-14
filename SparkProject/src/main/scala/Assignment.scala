
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, collect_list, collect_set, dayofmonth, days, lit, month, regexp_replace, sum, year}
import org.apache.spark.sql.types.{DataType, DateType, IntegerType, StringType}
import org.apache.spark.storage.StorageLevel

import scala.Console.in

object Assignment extends App{


  val spark = SparkSession.builder()
    .appName("Repartition and Coalesce")
    .master("local[*]")
    .getOrCreate()


  val df = spark.read.option("header", true).csv("file:///Users/danishakhlaque/Desktop/input.csv")
  //df.printSchema()

  val df2 = df.select(df.columns.map {
    case column@"Qty Sold" =>
      col(column).cast("Int").as(column)
    case column@"SumOfPOS Sales Dollars" =>
      col(column).cast("Int").as(column)
    case column@"Zak Start Date" =>
      col(column).cast("Date").as(column)
    case column@"Date Posted" =>
      col(column).cast("Date").as(column)
    case column =>
      col(column)
  }: _*)
  //df2.printSchema()


  def selectByType(colType: DataType, df: DataFrame) = {

    val cols = df.schema.toList
      .filter(x => x.dataType == colType)
      .map(c => col(c.name))
    df.select(cols: _*)

  }

  val resString = selectByType(StringType, df2)
  val resInteger = selectByType(IntegerType, df2)
  val resDates = selectByType(DateType, df2)

  val resStringList=resString.columns
  val resIntegerList=resInteger.columns
  val resDatesList=resDates.columns


  val oddIndex = resIntegerList.zipWithIndex.filter(_._2 % 2 == 1).map(_._1)
  println(oddIndex(0))
  val evenIndex = resIntegerList.zipWithIndex.filter(_._2 % 2 != 1).map(_._1)
  println(evenIndex(0))


  val oddDf: DataFrame = resInteger.select(oddIndex.map(m => col(m)): _*)
  val evenDf: DataFrame = resInteger.select(evenIndex.map(m => col(m)): _*)

  oddDf.select("*").show()
  evenDf.select("*").show()

  val evenDfAnswer: DataFrame = evenDf.select(evenIndex.map(m => sum(m)):_*)
  val oddDfAnswer: DataFrame = oddDf.select(oddIndex.map(m => avg(m)):_*)


  val merged_df = evenDfAnswer.unionByName(oddDfAnswer, true).show()





  val df4 = resDatesList.foldLeft(resDates)((tempdf, colName) =>
    {
      tempdf.withColumn("day_"+colName,  dayofmonth(col(colName)))
      .withColumn("month_"+colName,  month(col(colName)))
        .withColumn("year_"+colName,  year(col(colName)))

    })

  resDates.printSchema()
  println(resDatesList.mkString(","))


  df4.show()



}
