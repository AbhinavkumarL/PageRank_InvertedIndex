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
val arg3 = paramsArray(1).split("tar")(0).dropRight(1)
val arg4 = paramsArray(2)
val arg5 = paramsArray(3)

print("*****************************\n")
print("wget -P /tmp "+arg1 +" !\n")
print("tar -xvf /tmp/"+arg2+" -C /tmp" +" !\n")
print(arg3+"\n")
print("*****************************\n")

// Run shell commands
var cmd1 = "wget -P /tmp "+arg1 !
var cmd2 = "tar -xvf /tmp/"+arg2+" -C /tmp" !

// Load train data
val trainData = sc.textFile("file:/tmp/"+arg3+"-train/*").flatMap(line => line.split("""\W+""")).filter(x => x.length > 5)
val Atheism = sc.textFile("file:/tmp/"+arg3+"-train/alt.atheism/*")
val Graphics = sc.textFile("file:/tmp/"+arg3+"-train/comp.graphics/*")
val WindowsMisc = sc.textFile("file:/tmp/"+arg3+"-train/comp.os.ms-windows.misc/*")
val PcHardware = sc.textFile("file:/tmp/"+arg3+"-train/comp.sys.ibm.pc.hardware/*")
val MacHardware = sc.textFile("file:/tmp/"+arg3+"-train/comp.sys.mac.hardware/*")
val WindowsX = sc.textFile("file:/tmp/"+arg3+"-train/comp.windows.x/*")
val Forsale = sc.textFile("file:/tmp/"+arg3+"-train/misc.forsale/*")
val Autos = sc.textFile("file:/tmp/"+arg3+"-train/rec.autos/*")
val Motorcycles = sc.textFile("file:/tmp/"+arg3+"-train/rec.motorcycles/*")
val Baseball = sc.textFile("file:/tmp/"+arg3+"-train/rec.sport.baseball/*")
val Hockey = sc.textFile("file:/tmp/"+arg3+"-train/rec.sport.hockey/*")
val Crypt = sc.textFile("file:/tmp/"+arg3+"-train/sci.crypt/*")
val Electronics = sc.textFile("file:/tmp/"+arg3+"-train/sci.electronics/*")
val Med = sc.textFile("file:/tmp/"+arg3+"-train/sci.med/*")
val Space = sc.textFile("file:/tmp/"+arg3+"-train/sci.space/*")
val Christian = sc.textFile("file:/tmp/"+arg3+"-train/soc.religion.christian/*")
val Guns = sc.textFile("file:/tmp/"+arg3+"-train/talk.politics.guns/*")
val Mideast = sc.textFile("file:/tmp/"+arg3+"-train/talk.politics.mideast/*")
val PoliticsMisc = sc.textFile("file:/tmp/"+arg3+"-train/talk.politics.misc/*")
val ReligionMisc = sc.textFile("file:/tmp/"+arg3+"-train/talk.religion.misc/*")

// Apply Mapper on the data in the testFiles read
val wordsAtheism = Atheism.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsGraphics = Graphics.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsWindowsMisc = WindowsMisc.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsPcHardware = PcHardware.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsMacHardware = MacHardware.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsWindowsX = WindowsX.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsForsale = Forsale.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsAutos = Autos.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsMotorcycles = Motorcycles.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsBaseball = Baseball.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsHockey = Hockey.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsCrypt = Crypt.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsElectronics = Electronics.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsMed = Med.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsSpace = Space.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsChristian = Christian.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsGuns = Guns.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsMideast = Mideast.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsPoliticsMisc = PoliticsMisc.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)
val wordsReligionMisc = ReligionMisc.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => 1)

// Load sparkContext libraries
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
val mergeNi = sc.union(wordsAtheism,wordsGraphics,wordsWindowsMisc,wordsPcHardware,wordsMacHardware,wordsWindowsX,wordsForsale,wordsAutos,wordsMotorcycles,wordsBaseball,wordsHockey,wordsCrypt,wordsElectronics,wordsMed,wordsSpace,wordsChristian,wordsGuns,wordsMideast,wordsPoliticsMisc,wordsReligionMisc)

// Apply reducer on the Mapped Data
val ni = mergeNi.reduceByKey((count1, count2) => count1 + count2)

// View sample of the Result
ni.sortBy(-_._2).take(10)

// build a tf-idf index Data
val tfwordsAtheism = Atheism.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsGraphics = Graphics.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsWindowsMisc = WindowsMisc.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsPcHardware = PcHardware.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsMacHardware = MacHardware.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsWindowsX = WindowsX.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsForsale = Forsale.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsAutos = Autos.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsMotorcycles = Motorcycles.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsBaseball = Baseball.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsHockey = Hockey.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsCrypt = Crypt.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsElectronics = Electronics.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsMed = Med.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsSpace = Space.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsChristian = Christian.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsGuns = Guns.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsMideast = Mideast.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsPoliticsMisc = PoliticsMisc.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)
val tfwordsReligionMisc = ReligionMisc.flatMap(line => line.split("""\W+""")).filter(x => x.length > 5).map(word => (word, 1)).reduceByKey((count1, count2) => count1 + count2)

// Apply a Mapper to calcute the grouped index.
val tf1wordsAtheism = tfwordsAtheism.map{case (x:String,y:Int)=>((x:String,"Atheism"),y:Int)}
val tf1wordsGraphics = tfwordsGraphics.map{case (x:String,y:Int)=>((x:String,"Graphics"),y:Int)}
val tf1wordsWindowsMisc = tfwordsWindowsMisc.map{case (x:String,y:Int)=>((x:String,"WindowsMisc"),y:Int)}
val tf1wordsPcHardware = tfwordsPcHardware.map{case (x:String,y:Int)=>((x:String,"PcHardware"),y:Int)}
val tf1wordsMacHardware = tfwordsMacHardware.map{case (x:String,y:Int)=>((x:String,"MacHardware"),y:Int)}
val tf1wordsWindowsX = tfwordsWindowsX.map{case (x:String,y:Int)=>((x:String,"WindowsX"),y:Int)}
val tf1wordsForsale = tfwordsForsale.map{case (x:String,y:Int)=>((x:String,"Forsale"),y:Int)}
val tf1wordsAutos = tfwordsAutos.map{case (x:String,y:Int)=>((x:String,"Autos"),y:Int)}
val tf1wordsMotorcycles =tfwordsMotorcycles.map{case (x:String,y:Int)=>((x:String,"Motorcycles"),y:Int)}
val tf1wordsBaseball = tfwordsBaseball.map{case (x:String,y:Int)=>((x:String,"Baseball"),y:Int)}
val tf1wordsHockey = tfwordsHockey.map{case (x:String,y:Int)=>((x:String,"Hockey"),y:Int)}
val tf1wordsCrypt = tfwordsCrypt.map{case (x:String,y:Int)=>((x:String,"Crypt"),y:Int)}
val tf1wordsElectronics = tfwordsElectronics.map{case (x:String,y:Int)=>((x:String,"Electronics"),y:Int)}
val tf1wordsMed = tfwordsMed.map{case (x:String,y:Int)=>((x:String,"Med"),y:Int)}
val tf1wordsSpace = tfwordsSpace.map{case (x:String,y:Int)=>((x:String,"Space"),y:Int)}
val tf1wordsChristian = tfwordsChristian.map{case (x:String,y:Int)=>((x:String,"Christian"),y:Int)}
val tf1wordsGuns = tfwordsGuns.map{case (x:String,y:Int)=>((x:String,"Guns"),y:Int)}
val tf1wordsMideast = tfwordsMideast.map{case (x:String,y:Int)=>((x:String,"Mideast"),y:Int)}
val tf1wordsPoliticsMisc = tfwordsPoliticsMisc.map{case (x:String,y:Int)=>((x:String,"PoliticsMisc"),y:Int)}
val tf1wordsReligionMisc = tfwordsReligionMisc.map{case (x:String,y:Int)=>((x:String,"ReligionMisc"),y:Int)}

// Take union of all the Mapped DataFrames
val tf = sc.union(tf1wordsAtheism,tf1wordsGraphics,tf1wordsWindowsMisc,tf1wordsPcHardware,tf1wordsMacHardware,tf1wordsWindowsX,tf1wordsForsale,tf1wordsAutos,tf1wordsMotorcycles,tf1wordsBaseball,tf1wordsHockey,tf1wordsCrypt,tf1wordsElectronics,tf1wordsMed,tf1wordsSpace,tf1wordsChristian,tf1wordsGuns,tf1wordsMideast,tf1wordsPoliticsMisc,tf1wordsReligionMisc)


// Test the tf-idx using the sample data from the test link
var testCmd = "wget -P /tmp "+arg4 !

import scala.io.Source
var wordlist = Source.fromFile("/tmp/"+arg5).getLines.toList

// Final Answer:
for (word <- wordlist)
{
  var nicountlist = ni.lookup(word)
  var nicount = nicountlist(0).toDouble
  var row = tf.filter{case (x:(String,String),y:Int) => x._1 == word}
  var ans = row.map{case (x:(String,String),y:Int)=> (x._2, y*Math.log(20.0/nicount))}
  print(word+ " ") 
  ans.sortBy(-_._2).take(5).foreach(print)
  println()
}

val az = ni.lookup("fundamentalists")
az(0).toDouble

// Apply filter and Map the result.
val row = tf.filter{case (x:(String,String),y:Int) => x._1 == "fundamentalists"}
val finalAnswer = row.map{case (x:(String,String),y:Int)=> (x._2, y*Math.log(20.0/4.0))}
finalAnswer.sortBy(-_._2).take(5).foreach(print)

