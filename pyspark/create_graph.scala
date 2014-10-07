import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.io.Source

val vertices_dat: RDD[String] = sc.textFile("/data/graph/vertices.dat")
val edges_dat: RDD[String] = sc.textFile("/data/graph/edges.dat")


vertices_dat.first

val vertices = vertices_dat.map { line =>
  val fields = line.split('\t')
  (fields(1).toLong, fields(0))
}
//1232132141 articlename

val edges = edges_dat.map { line =>
  val fields = line.split('\t')
  Edge(fields(0).toLong, fields(1).toLong, fields(2).toLong)
}
//1231231231 123123132131


val graph = Graph(vertices,edges,"").cache()

graph.vertices.count