package com.triplerush

import com.signalcollect.{ExecutionConfiguration, ExecutionInformation, Vertex, Graph}
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.interfaces.ModularAggregationOperation

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

}



