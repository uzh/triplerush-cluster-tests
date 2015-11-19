import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import sbtassembly.Plugin._
import net.virtualvoid.sbt.graph.Plugin

val project = Project(
  id = "triplerush-cluster-tests",
  base = file("."),
  settings = Project.defaultSettings ++ SbtMultiJvm.multiJvmSettings ++  net.virtualvoid.sbt.graph.Plugin.graphSettings ++ Seq(
    organization := "io.cotiviti",
    name := "triplerush-cluster-tests",
    version := "0.0.1-SNAPSHOT",
    scalaVersion := "2.11.7",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value % "compile",
      "org.scalatest" %% "scalatest" % "2.2.4" % "test",
      "com.signalcollect" %% "triplerush" % "8.0.1",

      "com.typesafe.akka" %% "akka-remote-tests" % "2.4-M2",
      "com.google.protobuf" % "protobuf-java" % "2.6.1",
      "com.typesafe" % "config" % "1.3.0"
    ),
    //    dependencyOverrides += ("io.netty" % "netty" % "3.10.5.Final"),
    javaOptions in (Test,run) += "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSIncrementalPacing -XX:+CMSIncrementalMode -XX:ParallelGCThreads=40 -XX:ParallelCMSThreads=40",
    parallelExecution in Test := false,
    parallelExecution in Global := false,
    // make sure that MultiJvm test are compiled by the default test compilation
    compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
    // make sure that MultiJvm tests are executed by the default test target,
    // and combine the results from ordinary test and multi-jvm tests
    executeTests in Test <<= (executeTests in Test, executeTests in MultiJvm) map {
      case (testResults, multiNodeResults) =>
        val overall =
          if (testResults.overall.id < multiNodeResults.overall.id)
            multiNodeResults.overall
          else
            testResults.overall
        Tests.Output(overall,
          testResults.events ++ multiNodeResults.events,
          testResults.summaries ++ multiNodeResults.summaries)
    }
  )
) configs (MultiJvm)