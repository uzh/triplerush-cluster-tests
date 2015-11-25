package com.triplerush

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import akka.util.Timeout
import com.signalcollect.GraphBuilder
import com.signalcollect.nodeprovisioning.cluster.{ClusterNodeProvisionerActor, RetrieveNodeActors}
import com.signalcollect.triplerush.TripleRush
import com.signalcollect.triplerush.mapper.RelievedNodeZeroTripleMapperFactory
import com.triplerush.ClusterTestUtils._
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._

class ClusterTripleRushMultiJvmNode1 extends ClusterTripleRushSpec

class ClusterTripleRushMultiJvmNode2 extends ClusterTripleRushSpec

class ClusterTripleRushMultiJvmNode3 extends ClusterTripleRushSpec

object ClusterTripleRushConfig extends MultiNodeConfig {
  val provisioner = role("provisioner")
  val node1 = role("node1")
  val node2 = role("node2")
  val clusterName = "ClusterTripleRushSpec"
  val seedPort = 2561

  nodeConfig(provisioner) {
    MultiJvmConfig.provisionerCommonConfig(seedPort)
  }

  commonConfig {
    val mappingsConfig =
      """akka.actor.kryo.mappings {
        |  "com.triplerush.ModularAggregator" = 133,
        |  "com.signalcollect.triplerush.CombiningMessageBusFactory" = 134,
        |  "com.signalcollect.triplerush.mapper.DistributedTripleMapperFactory$" = 135,
        |  "com.signalcollect.triplerush.handlers.TripleRushEdgeAddedToNonExistentVertexHandlerFactory$" = 136,
        |  "com.signalcollect.triplerush.util.TripleRushStorage$" = 137,
        |  "com.signalcollect.triplerush.handlers.TripleRushUndeliverableSignalHandlerFactory$" = 138,
        |  "com.signalcollect.triplerush.util.TripleRushWorkerFactory" = 139,
        |  "com.signalcollect.triplerush.BlockingIndexVertexEdge" = 140
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

class ClusterTripleRushSpec extends MultiNodeSpec(ClusterTripleRushConfig) with STMultiNodeSpec
with ImplicitSender with ScalaFutures {

  import ClusterTripleRushConfig._


  override def initialParticipants = roles.size

  val workers = roles.size

  override def atStartup() = println("Starting")

  override def afterTermination() = println("Terminated")

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(300, Seconds)), interval = scaled(Span(1000, Millis)))


  "distributed triplerush" must {
    implicit val timeout = Timeout(30.seconds)
    "load triples and query triplerush store" in within(100.seconds) {
      val prefix = TestConfig.prefix
      val mapperFactory = if (workers >= 2) {
        system.log.info("TripleRush is using the RelievedNodeZeroTripleMapper factory.")
        Some(RelievedNodeZeroTripleMapperFactory)
      } else {
        system.log.info(s"TripleRush is using the default triple mapper factory.")
        None
      }
      runOn(provisioner) {
        val masterActor = system.actorOf(Props(classOf[ClusterNodeProvisionerActor], MultiJvmConfig.idleDetectionPropagationDelayInMilliseconds,
          prefix, workers), "ClusterMasterBootstrap")
        val nodeActorsFuture = (masterActor ? RetrieveNodeActors).mapTo[Array[ActorRef]]
        whenReady(nodeActorsFuture) { nodeActors =>
          assert(nodeActors.length == workers)
          val graphBuilder = new GraphBuilder[Long, Any]().
            withActorSystem(system).
            withPreallocatedNodes(nodeActors)
          val trInstance = TripleRush(
            graphBuilder = graphBuilder,
            tripleMapperFactory = mapperFactory,
            console = false)

          system.log.info(s"TripleRush has been initialized.")
          testLoadingAndQuerying(trInstance) shouldBe true
        }
      }
      enterBarrier("Clustered TR - test1 done")
    }


  }
  enterBarrier("Clustered TR - all tests done")
}
