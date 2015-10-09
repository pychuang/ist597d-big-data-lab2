import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import scala.collection.mutable
import scala.collection.immutable.ListMap
import scala.collection.immutable.Seq

object TweetParser {

  private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/2.3.0.0-2557/hadoop/conf/core-site.xml");
  private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/2.3.0.0-2557/hadoop/conf/hdfs-site.xml");
  private final val CSV_NUMBER_OF_COLUMNS : Int = 7;

  def main(args: Array[String]) {
    //val inputFile = new Path("/ist597j/tweets/nyc-twitter-data-2013.csv"); // Use a local path here to test on your machine
    val inputFile = new Path("/home/ratbert/share/work/classes/ist597d-big-data/lab2/nyc-twitter-data-2013.csv.test"); // Use a local path here to test on your machine
    val outputFile = "output.txt"
    val config = initializeConfiguration();
    val reader = instantiateReader(config, inputFile);
    val lines = readCSV(reader);    
    closeHandles(reader);
    val stats = processData(lines)
    outputData(outputFile, stats)
  }

  // Configure the HDFS interface parameters
  private final def initializeConfiguration() : Configuration = {
    val configuration = new Configuration()
    configuration.addResource(CORE_SITE_CONFIG_PATH)
    configuration.addResource(HDFS_SITE_CONFIG_PATH)
    return configuration
  }

  // Create a Reader object to talk to the file
  private final def instantiateReader(configuration: Configuration, path: Path) : BufferedReader = {
    val fileSystem = path.getFileSystem(configuration)
    return new BufferedReader(new InputStreamReader(fileSystem.open(path))) 
  }

  // Iterate through the file, line by line, store to a mutable linked-list structure, and return an immutable one
  private final def readCSV(bufferedReader: BufferedReader) : IndexedSeq[String] = {
      val listBuffer = new mutable.ListBuffer[String];
      while(bufferedReader.ready())
        listBuffer.append(bufferedReader.readLine())
        return listBuffer.toIndexedSeq
  }

  // Release handles to the CSV file
  private final def closeHandles(bufferedReader: BufferedReader) : Unit = {
      return bufferedReader.close();
  }

  def processData(lines: IndexedSeq[String]): ListMap[String, Int] = {
    println("process...")
    val texts = lines.map(getTweetText)
    println("got text")
    val words = texts.flatMap(splitWords)
    println("got words")
    val hashtags = words.filter(isHashtag).toList
    println("got hashtags")
    val htStats = countHashtags(hashtags)
    println("calc hashtags")
    val top100 = top100Hashtags(htStats)
    println("top 100")
    return top100
  }

  def getTweetText(line: String): String = {
    val cols = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
    if (cols.size < 7)
      return ""
    else
      return cols(6)
  }

  def splitWords(text: String): Array[String] = {
    val words = text.split("[ .,?!:\"]")
    return words.filter(_.size > 0)
  }

  def isHashtag(word: String): Boolean = {
    return word(0) == '#'
  }

  def countHashtags(hashtags: List[String]): Map[String, Int] = {
    return hashtags.groupBy(x=>x).map(x=>(x._1, x._2.size))
  }
  
  def top100Hashtags(htStats : Map[String, Int]) : ListMap[String, Int] = {
    return ListMap(htStats.toSeq.sortWith(_._2 > _._2):_*).take(100)
  }

  def outputData(outputFile: String, stats: ListMap[String, Int]) :Unit = {
    val writer = new PrintWriter(new File(outputFile))
    stats.foreach(x => writer.write(x._1 + "\t" + x._2 + "\n"))
    writer.close()
  }
}
