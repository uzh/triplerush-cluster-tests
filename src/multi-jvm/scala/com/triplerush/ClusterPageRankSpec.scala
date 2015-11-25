package com.triplerush

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import akka.util.Timeout
import com.signalcollect.examples.{PageRankEdge, PageRankVertex}
import com.signalcollect.nodeprovisioning.cluster.{ClusterNodeProvisionerActor, RetrieveNodeActors}
import com.signalcollect.{Graph, GraphBuilder, Vertex}
import com.triplerush.ClusterTestUtils._
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._

class ClusterPageRankMultiJvmNode1 extends ClusterPageRankSpec

class ClusterPageRankMultiJvmNode2 extends ClusterPageRankSpec

class ClusterPageRankMultiJvmNode3 extends ClusterPageRankSpec

object ClusterPageRankConfig extends MultiNodeConfig {
  val provisioner = role("provisioner")
  val node1 = role("node1")
  val node2 = role("node2")
  val clusterName = "ClusterPageRankSpec"
  val seedPort = 2559

  nodeConfig(provisioner) {
    MultiJvmConfig.provisionerCommonConfig(seedPort)
  }

  commonConfig {
    val mappingsConfig =
      """akka.actor.kryo.mappings {
        |  "com.triplerush.ModularAggregator" = 133,
        |  "com.triplerush.ClusterPageRankSpec$$anonfun$2" = 134,
        |  "com.triplerush.ClusterPageRankSpec$$anonfun$3" = 135,
        |  "com.triplerush.ClusterPageRankSpec$$anonfun$4" = 136,
        |  "com.triplerush.ClusterPageRankSpec$$anonfun$5" = 137
        |    }""".stripMargin

    val largeTimeoutConfig =
      """
        |akka {
        |  testconductor {
        |
        |    # Timeout for joining a barrier: this is the maximum time any participants
        |    # waits for everybody else to join a named barrier.
        |    barrier-timeout = 100s
        |
        |    # Timeout for interrogation of TestConductor’s Controller actor
        |    query-timeout = 20s
        |
        |    # amount of time for the ClientFSM to wait for the connection to the conductor
        |    # to be successful
        |    connect-timeout = 60s
        |
        |    # Number of connect attempts to be made to the conductor controller
        |    client-reconnects = 20
        |    }
        |}
      """.stripMargin
    ConfigFactory.parseString(largeTimeoutConfig).withFallback(MultiJvmConfig.nodeCommonConfig(clusterName, seedPort, mappingsConfig))
  }
}

class ClusterPageRankSpec extends MultiNodeSpec(ClusterPageRankConfig) with STMultiNodeSpec
with ImplicitSender with ScalaFutures {

  import ClusterPageRankConfig._


  override def initialParticipants = roles.size

  val workers = roles.size

  override def atStartup() = println("Starting")

  override def afterTermination() = println("Terminated")

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(300, Seconds)), interval = scaled(Span(1000, Millis)))

  def buildPageRankGraph(graph: Graph[Any, Any], edgeTuples: Traversable[Tuple2[Int, Int]]): Graph[Any, Any] = {
    edgeTuples foreach {
      case (sourceId: Int, targetId: Int) =>
        graph.addVertex(new PageRankVertex(sourceId, 0.85))
        graph.addVertex(new PageRankVertex(targetId, 0.85))
        graph.addEdge(sourceId, new PageRankEdge(targetId))
    }
    graph
  }

  val pageRankFiveCycleVerifier: Vertex[_, _, _, _] => Boolean = v => {
    val state = v.state.asInstanceOf[Double]
    val expectedState = 1.0
    val correct = (state - expectedState).abs < 0.01
    if (!correct) {
      System.err.println("Problematic vertex:  id=" + v.id + ", expected state=" + expectedState + ", actual state=" + state)
    }
    correct
  }

  val pageRankFiveStarVerifier: (Vertex[_, _, _, _]) => Boolean = v => {
    val state = v.state.asInstanceOf[Double]
    val expectedState = if (v.id == 4.0) 0.66 else 0.15
    val correct = (state - expectedState).abs < 0.00001
    if (!correct) {
      System.err.println("Problematic vertex:  id=" + v.id + ", expected state=" + expectedState + ", actual state=" + state)
    }
    correct
  }

  val pageRankTwoOnTwoGridVerifier: Vertex[_, _, _, _] => Boolean = v => {
    val state = v.state.asInstanceOf[Double]
    val expectedState = 1.0
    val correct = (state - expectedState).abs < 0.01
    if (!correct) {
      System.err.println("Problematic vertex:  id=" + v.id + ", expected state=" + expectedState + ", actual state=" + state)
    }
    correct
  }

  val pageRankTorusVerifier: Vertex[_, _, _, _] => Boolean = v => {
    val state = v.state.asInstanceOf[Double]
    val expectedState = 1.0
    val correct = (state - expectedState).abs < 0.01
    if (!correct) {
      System.err.println("Problematic vertex:  id=" + v.id + ", expected state=" + expectedState + ", actual state=" + state)
    }
    correct
  }

  "pageRank algorithim" must {
    implicit val timeout = Timeout(30.seconds)
    "deliver correct results on a 5-cycle graph" in within(100.seconds) {
      val prefix = TestConfig.prefix
      val fiveCycleEdges = List((0, 1), (1, 2), (2, 3), (3, 4), (4, 0))
      runOn(provisioner) {
        val masterActor = system.actorOf(Props(classOf[ClusterNodeProvisionerActor], MultiJvmConfig.idleDetectionPropagationDelayInMilliseconds,
          prefix, workers), "ClusterMasterBootstrap")
        val nodeActorsFuture = (masterActor ? RetrieveNodeActors).mapTo[Array[ActorRef]]
        whenReady(nodeActorsFuture) { nodeActors =>
          assert(nodeActors.length == workers)
          val computeGraphFactories: List[() => Graph[Any, Any]] = List(() => GraphBuilder.withActorSystem(system).withActorNamePrefix(prefix)
            .withPreallocatedNodes(nodeActors).build)
          test(graphProviders = computeGraphFactories, verify = pageRankFiveCycleVerifier, buildGraph = buildPageRankGraph(_, fiveCycleEdges),
            signalThreshold = 0.001) shouldBe true
        }
      }
      enterBarrier("PageRank - test1 done")
    }


    "deliver correct results on a 5-star graph" in {
      val prefix = TestConfig.prefix
      val fiveStarEdges = List((0, 4), (1, 4), (2, 4), (3, 4))

      runOn(provisioner) {
        val masterActor = system.actorOf(Props(classOf[ClusterNodeProvisionerActor], MultiJvmConfig.idleDetectionPropagationDelayInMilliseconds,
          prefix, workers), "ClusterMasterBootstrap")
        val nodeActorsFuture = (masterActor ? RetrieveNodeActors).mapTo[Array[ActorRef]]
        whenReady(nodeActorsFuture) { nodeActors =>
          assert(nodeActors.length == workers)
          val computeGraphFactories: List[() => Graph[Any, Any]] = List(() => GraphBuilder.withActorSystem(system)
            .withPreallocatedNodes(nodeActors).build)
          test(graphProviders = computeGraphFactories, verify = pageRankFiveStarVerifier, buildGraph = buildPageRankGraph(_, fiveStarEdges)) shouldBe true
        }
      }
      enterBarrier("PageRank - test2 done")
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
          test(graphProviders = computeGraphFactories, verify = pageRankTwoOnTwoGridVerifier, buildGraph = buildPageRankGraph(_, symmetricTwoOnTwoGridEdges), signalThreshold = 0.001) shouldBe true
        }
      }
      enterBarrier("PageRank - test3 done")
    }

    "deliver correct results on a 5*5 torus" in {
      val prefix = TestConfig.prefix
      val symmetricTorusEdges = new Torus(5, 5)

      runOn(provisioner) {
        val masterActor = system.actorOf(Props(classOf[ClusterNodeProvisionerActor], MultiJvmConfig.idleDetectionPropagationDelayInMilliseconds,
          prefix, workers), "ClusterMasterBootstrap")
        val nodeActorsFuture = (masterActor ? RetrieveNodeActors).mapTo[Array[ActorRef]]
        whenReady(nodeActorsFuture) { nodeActors =>
          assert(nodeActors.length == workers)
          val computeGraphFactories: List[() => Graph[Any, Any]] = List(() => GraphBuilder.withActorSystem(system)
            .withPreallocatedNodes(nodeActors).build)
          test(graphProviders = computeGraphFactories, verify = pageRankTorusVerifier, buildGraph = buildPageRankGraph(_, symmetricTorusEdges), signalThreshold = 0.001) shouldBe true
        }
      }
      enterBarrier("PageRank - test4 done")
    }
  }
  enterBarrier("PageRank - all tests done")
}

