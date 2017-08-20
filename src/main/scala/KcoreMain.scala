import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.IntMap
import org.apache.spark.storage.StorageLevel
case class Index(clic:String)

object KCore {

    val initialMsg="-10"
    def mergeMsg(msg1: String, msg2:String): String = msg1+":"+msg2

    def vprog(vertexId: VertexId, value: (Int, Int,IntMap[Int],Int), message: String): (Int, Int,IntMap[Int],Int) = {
        if (message == initialMsg){
            return (value._1,value._2,value._3,value._4)
        }
        else{
            val msg=message.split(":")
            // var updates=IntMap[Int]()
            // for (m <-msg){
            //     val k=m.split("#")
            //     updates=updates+((k(0).toInt,k(1).toInt))
            // }
            // val newWeights = updates.unionWith(value._3,(_, av, bv) =>av)
            val elems=msg //newWeights.values
            var counts:Array[Int]=new Array[Int](value._1+1)
            for (m <-elems){
                val im=m.toInt
                if(im<=value._1)
                {
                    counts(im)=counts(im)+1
                }
                else{
                    counts(value._1)=counts(value._1)+1
                }
            }
            var curWeight =  0 //value._4-newWeights.size
            for(i<-value._1 to 1 by -1){
                curWeight=curWeight+counts(i)
                if(i<=curWeight){
                    return (i, value._1,value._3,value._4)  
                }
            }
            return (0, value._1,value._3,value._4)
            }
    }

    def sendMsg(triplet: EdgeTriplet[(Int, Int,IntMap[Int],Int), Int]): Iterator[(VertexId, String)] = {
        val sourceVertex = triplet.srcAttr
        val destVertex=triplet.dstAttr
        // if(sourceVertex._1==sourceVertex._2)
        // {
        //     return Iterator.empty
        // }else{
            //return Iterator((triplet.dstId, (triplet.srcId.toString+"#"+sourceVertex._1.toString)))
            return Iterator((triplet.dstId,sourceVertex._1.toString),(triplet.srcId,destVertex._1.toString))
        // }
    }



    def main(args: Array[String]){
        val startTimeMillis = System.currentTimeMillis()
        //setting up spark environment
        val conf: SparkConf = new SparkConf()
            .setAppName("KCore")
        val sc = new SparkContext(conf)
        //val operation="Kclique"
        val maxIter=args(0).toInt
        val ygraph=GraphLoader.edgeListFile(sc,args(1), true,args(2).toInt,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut).groupEdges((e1, e2) => e1)
        //val revYgraph=ygraph.reversie
        //print(ygraph.vertices.count())
        //println(ygraph.edges.count())

        //val nvertexes=ygraph.vertices.map({case (id,attr)=>(id,attr:Any)})
        //val nedges=ygraph.edges.union(revYgraph.edges)
        //ygraph.unpersist()
        //revYgraph.unpersist()

        //val modGraph= Graph(ygraph.vertices.map({case (id,attr)=>(id,attr:Any)}),ygraph.edges.union(ygraph.reverse.edges),StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut)
        //val degrees=modGraph.outDegrees
        val deg=ygraph.degrees

        //val mgraph=modGraph.outerJoinVertices(degrees)((id, oldattr, newattr) =>newattr.getOrElse(0)).mapVertices((id, attr) =>(attr,-1,IntMap[Int](),attr))
        val mgraph=ygraph.outerJoinVertices(deg)((id, oldattr, newattr) =>newattr.getOrElse(0)).mapVertices((id, attr) =>(attr,-1,IntMap[Int](),attr))
        //modGraph.unpersist()
        ygraph.unpersist()
        //val degreecount= mgraph.vertices.count()
        //val edgecount= mgraph.edges.count()
        val minGraph = mgraph.pregel(initialMsg,maxIter,EdgeDirection.Either)(vprog,sendMsg,mergeMsg)

        // minGraph.vertices.take(5).foreach(println)
        println(minGraph.vertices.map(_._2._1).max)
        val endTimeMillis = System.currentTimeMillis()
        val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
        println("Total Execution Time : "+durationSeconds.toString() + "s")
        //println(degreecount)
        //println(edgecount)
        sc.stop()
    }
}