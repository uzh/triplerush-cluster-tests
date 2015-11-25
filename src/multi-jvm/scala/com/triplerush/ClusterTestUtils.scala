package com.triplerush

import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.examples.ColoredVertex
import com.signalcollect.interfaces.ModularAggregationOperation
import com.signalcollect.triplerush.sparql.{Sparql, TripleRushGraph}
import com.signalcollect.triplerush.{TriplePattern, TripleRush}
import com.signalcollect.{ExecutionConfiguration, ExecutionInformation, Graph, Vertex}
import org.apache.jena.graph.{NodeFactory, Triple => JenaTriple}

import scala.collection.JavaConversions._
import scala.util.Try

class ModularAggregator(verify: Vertex[_, _, _, _] => Boolean) extends ModularAggregationOperation[Boolean] {
  val neutralElement = true

  def aggregate(a: Boolean, b: Boolean): Boolean = a && b

  def extract(v: Vertex[_, _, _, _]): Boolean = verify(v)
}

object ClusterTestUtils {

  val executionModes = List(ExecutionMode.ContinuousAsynchronous)

  def test(graphProviders: List[() => Graph[Any, Any]], verify: Vertex[_, _, _, _] => Boolean,
           buildGraph: Graph[Any, Any] => Unit = (graph: Graph[Any, Any]) => (), signalThreshold: Double = 0.01, collectThreshold: Double = 0): Boolean = {
    var correct = true
    var computationStatistics = Map[String, List[ExecutionInformation[Any, Any]]]()

    for (executionMode <- executionModes) {
      for (graphProvider <- graphProviders) {
        val graph = graphProvider()
        try {
          buildGraph(graph)
          graph.awaitIdle
          val stats = graph.execute(ExecutionConfiguration(executionMode = executionMode, signalThreshold = signalThreshold))
          graph.awaitIdle
          correct &= graph.aggregate(new ModularAggregator(verify))
          if (!correct) {
            System.err.println("Test failed. Computation stats: " + stats)
          }
        } finally {
          graph.shutdown
        }
      }
    }
    correct
  }

  def testLoadingAndQuerying(tr: TripleRush): Boolean = {
    val sparql = s"""
                    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                    SELECT ?person ?projectCount
                     WHERE {
                      {
                        SELECT ?person (COUNT(?project) as ?projectCount)
                        WHERE {
                          ?person foaf:name ?name .
                          ?person foaf:currentProject ?project .
                        }
                         GROUP BY ?person
                      }
                      FILTER (?projectCount > 1)
                    }
    """
    val triplesAdded: Try[Unit] = Try {
      tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/name", "\"Arnie\"")
      tr.addStringTriple("http://PersonB", "http://xmlns.com/foaf/0.1/name", "\"Bob\"")
      tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/currentProject", "\"Gardening\"")
      tr.addStringTriple("http://PersonA", "http://xmlns.com/foaf/0.1/currentProject", "\"Skydiving\"")
      tr.addStringTriple("http://PersonB", "http://xmlns.com/foaf/0.1/currentProject", "\"Volleyball\"")
    }
    val expectedCount = 5
    val count = tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size
    val cntBool = count == expectedCount
    val graph = TripleRushGraph(tr)
    implicit val model = graph.getModel
    val results = Sparql(sparql)
    val resultBindings = results.map { bindings =>
      bindings.getResource("person").toString
    }.toSet
    val resBool = resultBindings == Set("http://PersonA")
    tr.shutdown()
    cntBool && resBool
  }

}

class VerifiedColoredVertex(id: Int, numColors: Int) extends ColoredVertex(id, numColors, 0, false) {
  // only necessary to allow access to vertex internals
  def publicMostRecentSignals: Iterable[Int] = mostRecentSignalMap.values
}

/**
 * Required for integration testing. Returns an undirected grid-structured graph.
 * Example Grid(2,2): Edges=(1,3), (3,1), (1,2), (2,1), (2,4), (4,2), (3,4), (4,3)
 * 1-2
 * | |
 * 3-4
 */
class Grid(val width: Int, height: Int) extends Traversable[(Int, Int)] with Serializable {

  def foreach[U](f: ((Int, Int)) => U) = {
    val max = width * height
    for (n <- 1 to max) {
      if (n + width <= max) {
        f((n, n + width))
        f((n + width, n))
      }
      if (n % height != 0) {
        f((n, n + 1))
        f((n + 1, n))
      }
    }
  }
}

class Torus(val width: Int, height: Int) extends Traversable[(Int, Int)] with Serializable {

  def foreach[U](f: ((Int, Int)) => U) = {
    val max = width * height
    for (y <- 0 until height) {
      for (x <- 0 until width) {
        val flattenedCurrentId = flatten((x, y), width)
        for (neighbor <- neighbors(x, y, width, height).map(flatten(_, width))) {
          f((flattenedCurrentId, neighbor))
        }
      }
    }
  }

  def neighbors(x: Int, y: Int, width: Int, height: Int): List[(Int, Int)] = {
    List(
      (x, decrease(y, height)),
      (decrease(x, width), y), (increase(x, width), y),
      (x, increase(y, height)))
  }

  def decrease(counter: Int, limit: Int): Int = {
    if (counter - 1 >= 0) {
      counter - 1
    } else {
      width - 1
    }
  }

  def increase(counter: Int, limit: Int): Int = {
    if (counter + 1 >= width) {
      0
    } else {
      counter + 1
    }
  }

  def flatten(coordinates: (Int, Int), width: Int): Int = {
    coordinates._1 + coordinates._2 * width
  }
}



