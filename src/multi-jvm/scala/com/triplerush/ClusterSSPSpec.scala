package com.triplerush

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import akka.util.Timeout
import com.signalcollect.examples.{Location, Path}
import com.signalcollect.nodeprovisioning.cluster.{ClusterNodeProvisionerActor, RetrieveNodeActors}
import com.signalcollect.{Graph, GraphBuilder, Vertex}
import com.triplerush.ClusterTestUtils._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._

class ClusterSSSPSpecMultiJvmNode1 extends ClusterSSSPSpec

class ClusterSSSPSpecMultiJvmNode2 extends ClusterSSSPSpec

class ClusterSSSPSpecMultiJvmNode3 extends ClusterSSSPSpec

object ClusterSSSPConfig extends MultiNodeConfig {
  val provisioner = role("provisioner")
  val node1 = role("node1")
  val node2 = role("node2")

  val clusterName = "ClusterSSSPSpec"
  val seedPort = 2563

  nodeConfig(provisioner) {
    MultiJvmConfig.provisionerCommonConfig(seedPort)
  }

  commonConfig {
    val mappingsConfig =
      """akka.actor.kryo.mappings {
        |  "com.triplerush.ModularAggregator" = 133,
        |  "com.triplerush.ClusterSSSPSpec$$anonfun$2" = 134,
        |  "com.triplerush.ClusterSSSPSpec$$anonfun$3" = 135
        |    }""".stripMargin

    MultiJvmConfig.nodeCommonConfig(clusterName, seedPort, mappingsConfig)
  }
}

class ClusterSSSPSpec extends MultiNodeSpec(ClusterSSSPConfig) with STMultiNodeSpec
with ImplicitSender with ScalaFutures {

  import ClusterSSSPConfig._

  override def initialParticipants = roles.size

  val workers = roles.size

  override def atStartup() = println("Starting")

  override def afterTermination() = println("Terminated")

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(300, Seconds)), interval = scaled(Span(1000, Millis)))

  val ssspSymmetricsFourCycleVerifier: Vertex[_, _, _, _] => Boolean = v => {
    val state = v.state.asInstanceOf[Option[Int]].get
    val expectedState = v.id
    val correct = state == expectedState
    if (!correct) {
      System.err.println("Problematic vertex:  id=" + v.id + ", expected state=" + expectedState + ", actual state=" + state)
    }
    correct
  }

  val ssspSymmetricFiveStarVerifier: Vertex[_, _, _, _] => Boolean = v => {
    val state = v.state.asInstanceOf[Option[Int]].get
    val expectedState = if (v.id == 4) 0 else 1
    val correct = state == expectedState
    if (!correct) {
      System.err.println("Problematic vertex:  id=" + v.id + ", expected state=" + expectedState + ", actual state=" + state)
    }
    correct
  }

  def buildSsspGraph(pathSourceId: Any, graph: Graph[Any, Any], edgeTuples: Traversable[Tuple2[Int, Int]]): Graph[Any, Any] = {
    edgeTuples foreach {
      case (sourceId, targetId) =>
        if (sourceId.equals(pathSourceId)) {
          graph.addVertex(new Location(sourceId, Some(0)))
        } else {
          graph.addVertex(new Location(sourceId, None))
        }
        if (targetId.equals(pathSourceId)) {
          graph.addVertex(new Location(targetId, Some(0)))
        } else {
          graph.addVertex(new Location(targetId, None))
        }
        graph.addEdge(sourceId, new Path(targetId))
    }
    graph
  }

  "SSSP algorithm" should {
    implicit val timeout = Timeout(30.seconds)

    "deliver correct results on a symmetric 4-cycle" in {
      val prefix = TestConfig.prefix
      val symmetricFourCycleEdges = List((0, 1), (1, 2), (2, 3), (3, 0))
      runOn(provisioner) {
        val masterActor = system.actorOf(Props(classOf[ClusterNodeProvisionerActor], MultiJvmConfig.idleDetectionPropagationDelayInMilliseconds,
          prefix, workers), "ClusterMasterBootstrap")
        val nodeActorsFuture = (masterActor ? RetrieveNodeActors).mapTo[Array[ActorRef]]
        whenReady(nodeActorsFuture) { nodeActors =>
          assert(nodeActors.length == workers)
          val computeGraphFactories: List[() => Graph[Any, Any]] = List(() => GraphBuilder.withActorSystem(system)
            .withPreallocatedNodes(nodeActors).build)
          test(graphProviders = computeGraphFactories, verify = ssspSymmetricsFourCycleVerifier, buildGraph = buildSsspGraph(0, _, symmetricFourCycleEdges)) shouldBe true
        }
      }
      enterBarrier("SSSP - test1 done")
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
          test(graphProviders = computeGraphFactories, verify = ssspSymmetricFiveStarVerifier, buildGraph = buildSsspGraph(4, _, symmetricFiveStarEdges)) shouldBe true
        }
      }
      enterBarrier("SSSP - test2 done")
    }
  }
  enterBarrier("SSSP - all tests done")
}
