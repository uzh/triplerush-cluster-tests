package com.triplerush

import akka.actor.{ActorRef, Props}
import akka.cluster.Cluster
import akka.pattern.ask
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec, MultiNodeSpecCallbacks}
import akka.testkit.ImplicitSender
import akka.util.Timeout
import com.signalcollect.nodeprovisioning.cluster.{ClusterNodeProvisionerActor, RetrieveNodeActors}
import com.triplerush.ClusterPageRankConfig._
import com.typesafe.config.ConfigFactory
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._
import scala.language.postfixOps

class ClusterSpecMultiJvmNode1 extends ClusterNodeProvisionerSpec

class ClusterSpecMultiJvmNode2 extends ClusterNodeProvisionerSpec

class ClusterSpecMultiJvmNode3 extends ClusterNodeProvisionerSpec


object ClusterNodeProvisionerConfig extends MultiNodeConfig {
  val provisioner = role("provisioner")
  val worker1 = role("worker1")
  val worker2 = role("worker2")
  val seedPort = 2558
  val clusterName = "ClusterNodeProvisionerSpec"

  nodeConfig(provisioner) {
    MultiJvmConfig.provisionerCommonConfig(seedPort)
  }
  // this configuration will be used for all nodes
  // note that no fixed host names and ports are used
  commonConfig {
    val mappingsConfig =
      """akka.actor.kryo.mappings {
        |  "com.triplerush.ModularAggregator" = 133,
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

class ClusterNodeProvisionerSpec extends MultiNodeSpec(ClusterNodeProvisionerConfig) with STMultiNodeSpec
with ImplicitSender with ScalaFutures {

  import ClusterNodeProvisionerConfig._

  override def initialParticipants = roles.size

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(300, Seconds)), interval = scaled(Span(1000, Millis)))

  val masterAddress = node(provisioner).address
  val worker1Address = node(worker1).address
  val worker2Address = node(worker2).address
  val workers = 3
  val idleDetectionPropagationDelayInMilliseconds = 500
  muteDeadLetters(classOf[Any])(system)


  "SignalCollect" should {
    "support setting the number of workers created on each node" in {
      runOn(provisioner) {
        system.actorOf(Props(classOf[ClusterNodeProvisionerActor], idleDetectionPropagationDelayInMilliseconds,
          "ClusterMasterBootstrap", workers), "ClusterMasterBootstrap")
      }
      enterBarrier("all nodes are up")

      runOn(worker1) {
        Cluster(system).join(worker1Address)
      }
      enterBarrier("worker1 started")

      runOn(worker2) {
        Cluster(system).join(worker2Address)
      }
      enterBarrier("worker2 started")

      runOn(provisioner) {
        implicit val timeout = Timeout(300.seconds)
        val masterActor = system.actorSelection(node(provisioner) / "user" / "ClusterMasterBootstrap")
        val nodeActorsFuture = (masterActor ? RetrieveNodeActors).mapTo[Array[ActorRef]]
        whenReady(nodeActorsFuture) { nodeActors =>
          assert(nodeActors.size == workers)
        }
      }
      testConductor.enter("all done!")
    }
  }

  override def beforeAll: Unit = multiNodeSpecBeforeAll()

  override def afterAll: Unit = multiNodeSpecAfterAll()
}
