package com.triplerush

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import akka.util.Timeout
import com.signalcollect._
import com.signalcollect.nodeprovisioning.cluster.{ClusterNodeProvisionerActor, RetrieveNodeActors}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import com.triplerush.ClusterTestUtils._
import scala.concurrent.duration._

class ClusterVertexColoringSpecMultiJvmNode1 extends ClusterVertexColoringSpec

class ClusterVertexColoringSpecMultiJvmNode2 extends ClusterVertexColoringSpec

class ClusterVertexColoringSpecMultiJvmNode3 extends ClusterVertexColoringSpec

object ClusterVertexColoringConfig extends MultiNodeConfig {
  val provisioner = role("provisioner")
  val node1 = role("node1")
  val node2 = role("node2")

  val clusterName = "ClusterVertexColoringSpec"
  val seedPort = 2561

  nodeConfig(provisioner) {
    MultiJvmConfig.provisionerCommonConfig(seedPort)
  }

  commonConfig {
    val mappingsConfig =
      """akka.actor.kryo.mappings {
        |  "com.triplerush.ModularAggregator" = 133,
        |  "com.triplerush.ClusterVertexColoringSpec$$anonfun$2" = 134,
        |  "com.triplerush.VerifiedColoredVertex" = 135,
        |  "com.signalcollect.StateForwarderEdge" = 136
        |    }""".stripMargin

    MultiJvmConfig.nodeCommonConfig(clusterName, seedPort, mappingsConfig)
  }
}

class ClusterVertexColoringSpec extends MultiNodeSpec(ClusterVertexColoringConfig) with STMultiNodeSpec
with ImplicitSender with ScalaFutures {

  import ClusterVertexColoringConfig._

  override def initialParticipants = roles.size

  val workers = roles.size

  override def atStartup() = println("Starting")

  override def afterTermination() = println("Terminated")

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(300, Seconds)), interval = scaled(Span(1000, Millis)))

  val vertexColoringVerifier: Vertex[_, _, _, _] => Boolean = v => {
    v match {
      case v: VerifiedColoredVertex =>
        val verified = !v.publicMostRecentSignals.iterator.contains(v.state)
        if (!verified) {
          System.err.println("Vertex Coloring: " + v + " has the same color as one of its neighbors.\n" +
            "Most recent signals received: " + v.publicMostRecentSignals + "\n" +
            "Score signal: " + v.scoreSignal)
        }
        verified
      case other =>
        System.err.println("Vertex " + other + " is not of type VerifiedColoredVertex"); false
    }
  }

  def buildVertexColoringGraph(numColors: Int, graph: Graph[Any, Any], edgeTuples: Traversable[Tuple2[Int, Int]]): Graph[Any, Any] = {
    edgeTuples foreach {
      case (sourceId, targetId) =>
        graph.addVertex(new VerifiedColoredVertex(sourceId, numColors))
        graph.addVertex(new VerifiedColoredVertex(targetId, numColors))
        graph.addEdge(sourceId, new StateForwarderEdge(targetId))
    }
    graph
  }

  "VertexColoring algorithm" should {
    implicit val timeout = Timeout(30.seconds)

    "deliver correct results on a symmetric 4-cycle" in {
      val prefix = TestConfig.prefix
      val symmetricFourCycleEdges = List((0, 1), (1, 0), (1, 2), (2, 1), (2, 3), (3, 2), (3, 0), (0, 3))

      runOn(provisioner) {
        val masterActor = system.actorOf(Props(classOf[ClusterNodeProvisionerActor], MultiJvmConfig.idleDetectionPropagationDelayInMilliseconds,
          prefix, workers), "ClusterMasterBootstrap")
        val nodeActorsFuture = (masterActor ? RetrieveNodeActors).mapTo[Array[ActorRef]]
        whenReady(nodeActorsFuture) { nodeActors =>
          assert(nodeActors.length == workers)
          val computeGraphFactories: List[() => Graph[Any, Any]] = List(() => GraphBuilder.withActorSystem(system)
            .withPreallocatedNodes(nodeActors).build)
          test(graphProviders = computeGraphFactories, verify = vertexColoringVerifier, buildGraph = buildVertexColoringGraph(2, _, symmetricFourCycleEdges)) shouldBe true
        }
      }
      enterBarrier("VertexColoring - test1 done")
    }

    "deliver correct results on a symmetric 5-star" in {
      val prefix = TestConfig.prefix
      val symmetricFiveStarEdges = List((0, 4), (4, 0), (1, 4), (4, 1), (2, 4), (4, 2), (3, 4), (4, 3))
      runOn(provisioner) {
        val masterActor = system.actorOf(Props(classOf[ClusterNodeProvisionerActor], MultiJvmConfig.idleDetectionPropagationDelayInMilliseconds,
          prefix, workers), "ClusterMasterBootstrap")
        val nodeActorsFuture = (masterActor ? RetrieveNodeActors).mapTo[Array[ActorRef]]
        whenReady(nodeActorsFuture) { nodeActors =>
          assert(nodeActors.length == workers)
          val computeGraphFactories: List[() => Graph[Any, Any]] = List(() => GraphBuilder.withActorSystem(system)
            .withPreallocatedNodes(nodeActors).build)
          test(graphProviders = computeGraphFactories, verify = vertexColoringVerifier, buildGraph = buildVertexColoringGraph(2, _, symmetricFiveStarEdges)) shouldBe true
        }
      }
      enterBarrier("VertexColoring - test2 done")
    }

    "deliver correct results on a 2*2 symmetric grid" in {
      val prefix = TestConfig.prefix
      val symmetricTwoOnTwoGridEdges = new Grid(2, 2)
      runOn(provisioner) {
        val masterActor = system.actorOf(Props(classOf[ClusterNodeProvisionerActor], MultiJvmConfig.idleDetectionPropagationDelayInMilliseconds,
          prefix, workers), "ClusterMasterBootstrap")
        val nodeActorsFuture = (masterActor ? RetrieveNodeActors).mapTo[Array[ActorRef]]
        whenReady(nodeActorsFuture) { nodeActors =>
          assert(nodeActors.length == workers)
          val computeGraphFactories: List[() => Graph[Any, Any]] = List(() => GraphBuilder.withActorSystem(system)
            .withPreallocatedNodes(nodeActors).build)
          test(graphProviders = computeGraphFactories, verify = vertexColoringVerifier, buildGraph = buildVertexColoringGraph(2, _, symmetricTwoOnTwoGridEdges)) shouldBe true
        }
      }
      enterBarrier("VertexColoring - test3 done")
    }
  }
  enterBarrier("VertexColoring - all tests done")
}
