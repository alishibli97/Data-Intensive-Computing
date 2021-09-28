import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.Tuple2
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{min, max, avg, desc, udf, col, explode, count}

// number 1
@SerialVersionUID(100L)
case class Log(val project_code: String, val page_title: String, val page_hits: Long, val page_size: Long) extends Serializable
{
    // Class method
    def display() {
        println(project_code,page_title,page_hits,page_size);
    }

    def get_pagetitle_length(): Int = {
      return page_title.length()
    }
}

object SimpleApp {

  // number 2
  def convert_string_log(line: String) : Log = {
    var splits = line.split(" ")
    var logobject = new Log(splits(0), splits(1), splits(2).toLong, splits(3).toLong)
    return logobject
  }

  // number 3
  def convert_to_rdd_log(pagecounts:RDD[String]): RDD[Log] = {
    var pagecounts_log = pagecounts.map(a=>convert_string_log(a))// .foreach(a => convert_string_log(a))
    return pagecounts_log
  }

  // 1
  def retrieve_top_15_records(pagecounts:RDD[String]){
    pagecounts.take(15).foreach(a => convert_string_log(a).display())
  }

  // 2
  def get_number_of_records(pagecounts:RDD[String]): Long = { 
    return pagecounts.count
  }

  // 3
  def get_pagesize_stats(pagecounts:RDD[String]) : List[Long] = {
    var min: Long = convert_string_log(pagecounts.take(1)(0)).page_size
    var max: Long = convert_string_log(pagecounts.take(1)(0)).page_size
    var sum: Long = convert_string_log(pagecounts.take(1)(0)).page_size
    for (new_page <- pagecounts.collect()){
      var new_size: Long = convert_string_log(new_page).page_size
      if (min > new_size) {
        min = new_size
      }
      if (max < new_size) {
        max = new_size
      }
      sum = sum + new_size
    }
    var avg: Long = sum/get_number_of_records(pagecounts)
    return List(min,max,avg)
  }

  // 4
  def get_all_max_records_by_pagesize(pagecounts:RDD[String]) : List[Log] = {  
    var loglist = new ListBuffer[Log]()

    var max: Long = get_pagesize_stats(pagecounts)(1)
    // println("maximum value is", max)
    for (new_page <- pagecounts.collect()){
      var row_log = convert_string_log(new_page)
      var new_size: Long = row_log.page_size
      
      if (max == new_size) {
        loglist += row_log
      }
    }
    return loglist.toList
  }

  // 5
  def get_max_record_by_pagesize_and_popularity(pagecounts:RDD[String]) : Log = {
    val loglist: List[Log] = get_all_max_records_by_pagesize(pagecounts)
    var result: Log = loglist(0)
    for (row <- loglist){
      if (result.page_hits < row.page_hits) {
        result = row
      }
    }
    return result
  }

  // 6
  def get_all_max_records_by_pagetile_length(pagecounts:RDD[String]) : RDD[Log] = {
    val pagecounts_log = convert_to_rdd_log(pagecounts)
    val sortedRDD = pagecounts_log.sortBy(word => -word.get_pagetitle_length())

    val max_length = sortedRDD.take(1)(0).get_pagetitle_length
    val resultRDD = sortedRDD.filter(x => x.get_pagetitle_length == max_length)

    return resultRDD
  }

  // 7
  def create_rdd_with_big_pagesizes(pagecounts:RDD[String]) : RDD[Log] = {
    val stats: List[Long] = get_pagesize_stats(pagecounts) // (max,min,avg)
    val avg: Long = stats(2)
    val pagecounts_log = convert_to_rdd_log(pagecounts)
    val result = pagecounts_log.filter(x => x.page_size>=avg)
    return result
  }

  // 8
  def get_number_of_pageviews_per_project(pagecounts:RDD[String]): Map[String, Long] = {
    val pagecounts_log = convert_to_rdd_log(pagecounts)
    var pagecounts_codes = pagecounts_log.map(x => x.project_code).distinct().collect().toList

    var results: Map[String,Long] = Map(pagecounts_codes map {s => (s, 0.asInstanceOf[Number].longValue)} : _*)
    var len = results.size
    var i = 0
    for (code <- pagecounts_codes){
      i+=1
      results(code) = pagecounts_log.filter(x => x.project_code==code).count
      println(s"Finished $i out of $len")
    }

    return results
  }

  // 9
  def get_top10_rows_by_hits(pagecounts:RDD[String]): Array[Log] = {
    val pagecounts_log = convert_to_rdd_log(pagecounts)
    val sortedRDD = pagecounts_log.sortBy(word => -word.page_hits).take(10)
    return sortedRDD
  }

  // 10 
  def get_num_rows_with_pagetitle_starting_with_the_and_are_not_english(pagecounts:RDD[String]): List[Long] = {
    val pagecounts_log = convert_to_rdd_log(pagecounts)
    var count1: Long = pagecounts_log.filter(x => x.page_title.startsWith("The")).count
    var count2: Long = pagecounts_log.filter(x => x.page_title.startsWith("The") && x.project_code!="en").count
    return List(count1,count2)
  }

  // 11
  def get_num_words_with_one_hit(pagecounts:RDD[String]): Double = {
    val pagecounts_log = convert_to_rdd_log(pagecounts)
    var hits = pagecounts_log.filter(x => x.page_hits == 1).count
    val res : Double = hits.toDouble / pagecounts.count.toDouble
    return res*100
  }

  //12
  def get_num_unique_title_words(pagecounts:RDD[String]): Long = {
    val pagecounts_log = convert_to_rdd_log(pagecounts)
    val title_words = pagecounts_log.flatMap(x => x.page_title.split("_"))
    title_words.map(x => x.toLowerCase.replaceAll("[-+.^:,]",""))
    //title_words.map(x => x.toLowerCase.replaceAll("[-+.^:,]",""))

    val unique_title_words : Long = title_words.distinct().count
    return unique_title_words
  }

  // 13
  def get_most_occuring_title_word(pagecounts:RDD[String]): String = {
    val pagecounts_log = convert_to_rdd_log(pagecounts)
    val title_words = pagecounts_log.flatMap(x => x.page_title.split("_"))
    title_words.map(x => x.toLowerCase.replaceAll("[-+.^:,]",""))

    val map = title_words.groupBy(identity).mapValues(_.size).collect()
    val result = map.maxBy(_._2)

    println(result)

    return result._1
  }

  // Task 2
  def create_dataframe(pagecounts:RDD[String], spark:SparkSession) : DataFrame =  {
    val pagecounts_log = convert_to_rdd_log(pagecounts)
    import spark.implicits._
    val df = pagecounts_log.toDF("project_code","page_title","page_hits","page_size")
    // df.show()
    return df
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Lab 1")
    val sc = new SparkContext(conf)    

    val spark = SparkSession.builder().getOrCreate()
    // sc.setLogLevel("WARN")

    val pagecounts = sc.textFile("pagecounts/pagecounts-20160101-000000_parsed.out")
    println("Beginning")
    
    // 1
    // retrieve_top_15_records(pagecounts)
    
    // 2
    // println(get_number_of_records(pagecounts))
    
    // 3 
    // println(get_pagesize_stats(pagecounts))
    
    // 4
    // // println(get_all_max_records_by_pagesize(pagecounts))
    // val result: List[Log] = get_all_max_records_by_pagesize(pagecounts)
    // for (res <- result) res.display()
    
    // 5
    // // println(get_max_record_by_pagesize_and_popularity(pagecounts))
    // get_max_record_by_pagesize_and_popularity(pagecounts).display()
    
    // 6
    // println(get_all_max_records_by_pagetile_length(pagecounts))
    // var result: RDD[Log] = get_all_max_records_by_pagetile_length(pagecounts)
    // println(result.count())
    // for(res <- result) println(res.display())
    
    // 7
    // println(create_rdd_with_big_pagesizes(pagecounts).count())
    // val result: RDD[Log] = create_rdd_with_big_pagesizes(pagecounts)
    // var average: Long = get_pagesize_stats(pagecounts)(2)
    // for (res <- result) println(res.page_size>=average)

    // 8
    // println(get_number_of_pageviews_per_project(pagecounts))

    // 9
    // println(get_top10_rows_by_hits(pagecounts))
    // var result = get_top10_rows_by_hits(pagecounts)
    // for(k <- result) println(k.display())

    // 10
    // println(get_num_rows_with_pagetitle_starting_with_the_and_are_not_english(pagecounts))

    //11
    // println(get_num_words_with_one_hit(pagecounts))

    // 12
    // println(get_num_unique_title_words(pagecounts))

    // 13
    // print(get_most_occuring_title_word(pagecounts))

    // Task 2
    // Create RDD[Log] then create DataFrame from the RDD
    var df = create_dataframe(pagecounts, spark)

    // 3
    // df.select(min("page_size"),max("page_size"),avg("page_size")).show()

    // 5
    // var x = df.select(max("page_size")).first()(0) //(0).asInstanceOf[Long]
    // val df_filtered = df.filter(df("page_size") === x)
    // var df_sorted = df_filtered.orderBy(desc("page_hits"))
    // df_sorted.show()

    // 7
    // val avgg = df.select(avg("page_size")).first()(0)
    // var df_filtered = df.filter(df("page_size")>=avgg.asInstanceOf[Number].longValue)
    // println(df_filtered.count)

    // 12 & 13
    // val split_function = {
    //   (xs : String) => 
    //     var res = xs.split("_")
    //     for (x<-res) x.toLowerCase.replaceAll("[-+.^:,]","")
    //     res
    // }
    // val splitUDF = udf(split_function)
    // var df_splitted = df.withColumn("page_title_splitted", splitUDF(col("page_title")))
    // df_splitted.show()
    // var exploded = df_splitted.withColumn("page_title_splitted_final", explode(col("page_title_splitted")))
    // exploded.show()

    // 12
    // println(exploded.select(exploded("page_title_splitted_final")).distinct.count)
    
    // 13
    // var ress = exploded.groupBy("page_title_splitted_final").agg(count("project_code").alias("occurence")).orderBy(desc("occurence")).first()
    // println(ress)

    println("End")

  }
}
