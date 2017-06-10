import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._

object FlightData {
  val defaultAirport = ("Fehlend")

  case class Flight(OriginID: Long, Origin: String, DestID: Long, Dest: String, Distance: Double)
  
    //Einlesen des CSV-Files
  def readCsv(sc: SparkContext): RDD[String] = {
    val textRDD = sc.textFile("../GraphX/ressource/FlugDaten.csv")
    return textRDD
  }  
  
  //Umwandeln des RDD Objektes in ein Flight Objekt
  def parseFlight(str: String): Flight = {
    val line = str.split(",")
    Flight(line(0).toLong, line(1), line(2).toLong, line(3), line(4).toDouble)
  }

  //Erstellen eines Graphen, sowie die dazugehörogen Knoten und Kanten
  def createGraph(textRDD: RDD[String]): Graph[String, Double] = {
    val flightsRDD = textRDD.map(parseFlight).cache()

    //Erstellen der Knoten und Kanten RDDs
    val airports = flightsRDD.map(flight => (flight.OriginID.toLong, flight.Origin)).distinct()
    val routes = flightsRDD.map(flight => ((flight.OriginID, flight.DestID), flight.Distance)).distinct()
    

    //routes.collect().foreach(println)

    val flightEdges = routes.map {
      case ((org_id, dest_id), distance) => Edge(org_id.toLong, dest_id.toLong, distance)
    }

    val graph = Graph(airports, flightEdges, defaultAirport)
    
    println(graph.numVertices)
    return graph
  }

  //Anwenden des Connected Components Algorithmus, um die verbundenen Komponenten herauszufinden
  def connectedComponents(graph: Graph[String, Double]) = {
    
    //Connected Components auf dem Graphen ausführen
    val ccGraph = graph.connectedComponents().vertices
    println("-----------------")
    
    //Zusammenführen der Ergenisse mit den Knoten (Flughäfen), um die 3-Letter-Codes zu bekommen
    val temp = graph.vertices.join(ccGraph).map{
      case(id, (origin, cc)) => ("Flughafen: " + origin, " Kleinste Id: " + cc)
    }
    temp.collect().foreach(println)
  }
  
    //Anwenden des Triangle Counting Algortihmus, um die n Flughäfen mit dem höchsten Verkehrsaufkommen zu finden
  def triangleCounting(graph: Graph[String, Double], n: Int) = {

    //Triangle Counting auf dem Graphen ausführen
    val ranks = graph.triangleCount().vertices

    //Zusammenführen der Ergenisse mit den Knoten (Flughäfen), um die 3-Letter-Codes zu bekommen
    val temp = ranks.join(graph.vertices)

    //Sortieren der Ergebnisse
    val temp2 = temp.sortBy(_._2._1, false)
    
    println("Die " + n + " Flughäfen mit den höchsten Verkehrsaufkommen")
    temp2.take(n).foreach(println)
  }

  //Anwenden des PageRank Algortihmus, um die n Flughäfen mit dem höchsten Verkehrsaufkommen zu finden
  def pageRank(graph: Graph[String, Double], n: Int) = {

    //PageRank auf dem Graphen ausführen
    val ranks = graph.pageRank(0.1).vertices

    //Zusammenführen der Ergenisse mit den Knoten (Flughäfen), um die 3-Letter-Codes zu bekommen
    val temp = ranks.join(graph.vertices)

    //Sortieren der Ergebnisse
    val temp2 = temp.sortBy(_._2._1, false)
    
    println("Die " + n + " Flughäfen mit den höchsten Verkehrsaufkommen")
    temp2.take(n).foreach(println)
  }



  //Listet die n längsten Routen auf
  def longestRoutes(graph: Graph[String, Double], n:Int) = {
    println("Die " + n + " längsten Routen")
    graph.triplets.sortBy(_.attr, ascending = false).map(triplet =>
       "Von " + triplet.srcAttr + " nach " + triplet.dstAttr + " mit der Entfernung " + triplet.attr.toString() + " Meilen")
      .take(n).foreach(println)
      println("")
  }

  def listEdges(graph: Graph[String, Double]) = {
    graph.triplets.take(10).foreach(println)
    
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("FlightData").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    val textRDD = readCsv(sc)
    val graph = createGraph(textRDD)
//    longestRoutes(graph, 100)
//    pageRank(graph, 10)
//    listEdges(graph)
    connectedComponents(graph)
//    triangleCounting(graph)

  }
}