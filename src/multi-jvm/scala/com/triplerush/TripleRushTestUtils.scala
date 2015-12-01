package com.triplerush

import java.io.InputStream
import java.util.zip.GZIPInputStream

import com.signalcollect.triplerush.sparql.{Sparql, TripleRushGraph}
import com.signalcollect.triplerush.{TriplePattern, TripleRush}
import org.apache.jena.{graph => jena}

import org.semanticweb.yars.nx
import org.semanticweb.yars.nx.parser.NxParser

import scala.collection.JavaConversions._
import scala.util.Try

object TripleRushTestUtils {

  private[this] def jenaNode(nxNode: nx.Node): jena.Node = nxNode match {
    case bNode: nx.BNode       => jena.NodeFactory.createBlankNode(bNode.getLabel)
    case literal: nx.Literal   => jena.NodeFactory.createLiteral(literal.getLabel, literal.getLanguageTag)
    case resource: nx.Resource => jena.NodeFactory.createURI(resource.getLabel)
  }

  def triplesFromFile(resourceName: String): Iterator[jena.Triple] = {
    val inputStream = getClass.getClassLoader.getResourceAsStream(resourceName)
    val gzipInputStream: InputStream = new GZIPInputStream(inputStream)
    val parser = new NxParser
    val iterator = parser.parse(gzipInputStream).map { triple =>
      require(triple.size == 3)
      new jena.Triple(jenaNode(triple(0)), jenaNode(triple(1)), jenaNode(triple(2)))
    }
    new AutoClosingIterator[jena.Triple](iterator) {
      override protected def close(): Unit = gzipInputStream.close()
    }
  }

  private[this] abstract class AutoClosingIterator[T](iterator: Iterator[T]) extends Iterator[T] {
    private[this] var isClosed = false

    protected def close(): Unit

    override def hasNext: Boolean = {
      if (!isClosed && !iterator.hasNext) {
        close()
        isClosed = true
      }
      iterator.hasNext
    }

    override def next(): T = iterator.next()
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

  def testLoadingFromGzipFile(tr: TripleRush, srcName: String): Boolean = {
    val triplesIterator = triplesFromFile(srcName)
    tr.addTriples(triplesIterator)
    val countOne = tr.resultIteratorForQuery(Seq(TriplePattern(-1, -2, -3))).size
    println("from history ---> " + countOne)
    tr.shutdown()
    true
  }


}
