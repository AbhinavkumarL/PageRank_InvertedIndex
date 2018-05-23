// importing necessary libraries.
import sys.process._
import org.apache.spark.SparkConf

// Get params passed at run time into an array
// val args = sc.getConf.get("spark.driver.args").split("\\s+")
// we can access the arguments from within your scala code using below code:
val sconf = new SparkConf()
val paramsString = sconf.get("spark.driver.extraJavaOptions")
val paramsSlice = paramsString.slice(2,paramsString.length)
val paramsArray = paramsSlice.split(",")
val arg1 = paramsArray(0)
val arg2 = paramsArray(1)

var loadData = "wget -P /tmp "+arg1 !
print(loadData)
loadData

//use the first param to load data into DataFrame
val webLinksDF = sc.textFile("file:/tmp/"+arg2)

// Structure the data into URL and Initial Page Rank
var initialRanks = (webLinksDF.map(x => ((x.split("\\s+"))(0), (x.split("\\s+"))(1).toDouble))).distinct()

// Structure the data into URL and List of Links
val linksListMap = (webLinksDF.map(x => ((x.split("\\s+"))(0), (x.split("\\s+"))(2).replaceAll("[\\{ \\} \\( \\)]", "").split(",")))).distinct()

// Join the two DFs created above and calculate the Page Rank
for (i<-1 to 100){
val contribs = linksListMap.join(initialRanks).values.flatMap{
  case(urls,initialRanks)=>
  val size=urls.size
  urls.map(url=>(url,initialRanks/size))}
  initialRanks = contribs.reduceByKey(_ + _)
}

// Take top 100 Pages form Result
initialRanks.sortBy(-_._2).take(500)


// command to run 
// spark-shell -i Assignment-2-part1.scala --conf spark.driver.extraJavaOptions="-Dhttp://www.utdallas.edu/~axn112530/cs6350/data/PRData/webcrawl,webcrawl"
// Write data to out put file.
// import org.apache.spark.sql.SQLContext

// val sqlContext = new SQLContext(sc)

// val selectedData = initialRanks.select("urls", "initialRanks")
// selectedData.write
//     .format("com.databricks.spark.csv")
//     .option("header", "true")
//     .save("newRanks.csv")