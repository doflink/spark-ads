import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * SE Group TU Dresden, Germany
 * @lequocdo
 */

object DK {

    def jointDegreeDistribution(graph: Graph[Int, Int]): RDD[((Int, Int), Int)] = {
        //graph.vertices.persist
        //graph.edges.persist
        val degrees = graph.degrees.cache()
        val edges = graph.edges.cache()
        val edgesPure = edges.map(x => (x.srcId, x.dstId))
        val degreeMap = degrees.join(edgesPure).map(x => (x._2._2, x._2._1)).join(degrees).map(x => {if (x._2._1 < x._2._2) {x._2} else { (x._2._2, x._2._1)} } )
        val jdd = degreeMap.map(x => (x, 1)).reduceByKey((x, y) => x + y).sortByKey(true).cache() //Sort before join 
     
        return jdd
    }

    //Square of a number
    def sqr(x: Double) = x * x    

    //Max Degree
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
        if (a._2 > b._2) a else b
    }

    //Measure the defference betwen consecutive graphs using Eucludean Distance of dK-2 of the graphs
    def differenceGraphs(graph1: Graph[Int, Int], graph2: Graph[Int, Int]): Double = {
        val jdd1 = jointDegreeDistribution(graph1)
        val jdd2 = jointDegreeDistribution(graph2)
        val maxDegrees1 = graph1.degrees.reduce(max)
        val maxDegrees2 = graph2.degrees.reduce(max)
        
        val joinDegree = {if ( maxDegrees1._2 > maxDegrees2._2){  //{if (jdd1.count() > jdd2.count()) {
            jdd1.leftOuterJoin(jdd2)
        }else{
            jdd2.leftOuterJoin(jdd1)
            }
        }
        val squareJDD = joinDegree.mapValues(x => (if (x._2 == None){ sqr(x._1)} else { sqr(x._1 - x._2.toList(0).toInt)})).map(x => (if (x._1._1 == x._1._2) {x._2} else {2*x._2})).cache()
       
        val euclidean = squareJDD.reduce((x, y) => x + y)
        //println("######## Euclidean Distance ##########", euclidean)
        
        return math.sqrt(euclidean)
    }

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("dK-2")

        //Tuning performance of Spark
        conf.set("spark.io.compression.codec", "lzf") //compress to improve shuffle performance
        conf.set("spark.speculation", "true") //Turn of speculative execution to present stragglers
        conf.set("spark.eventLog.enabled","true")


        val sc = new SparkContext(conf)
        
        //println("####### Start with Graph has index", i)
        val graphInput1 = "/DDoS/" + args(0).toString() + ".txt"
        val graphInput2 = "/DDoS/" + args(1).toString() + ".txt"

        //Change partioning heuristic
        val graph1 = GraphLoader.edgeListFile(sc, graphInput1).cache()
        val graph2 = GraphLoader.edgeListFile(sc, graphInput2).cache() //true).partitionBy(PartitionStrategy.RandomVertexCut)
        val diff = differenceGraphs(graph1, graph2)
        
        println("######## The difference between the consecutive graphs: ##########", diff) 
        sc.stop() 
    }
}
