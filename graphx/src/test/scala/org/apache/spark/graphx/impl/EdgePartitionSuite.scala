/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.impl

import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.graphx._
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.serializer.KryoSerializer

class EdgePartitionSuite extends SparkFunSuite {

  def makeEdgePartition[A: ClassTag](xs: Iterable[(Int, Int, A)]): EdgePartition[A, Int] = {
    val builder = new EdgePartitionBuilder[A, Int]
    for ((src, dst, attr) <- xs) { builder.add(src: VertexId, dst: VertexId, attr) }
    builder.toEdgePartition
  }

  test("withAdditionalEdges") {
    val edges = Set(Edge(0, 1, 0), Edge(1, 2, 0), Edge(2, 0, 0))
    val builder = new EdgePartitionBuilder[Int, Int]
    for (e <- edges) {
      builder.add(e.srcId, e.dstId, e.attr)
    }

    // Add new edge with existing src vertex and existing dst vertex
    val addEdgesWithExSrcExDst = Set(Edge(1, 0, 99), Edge(2, 1, 99))
    var expected = List(Edge(0, 1, 0), Edge(1, 0, 99), Edge(1, 2, 0), Edge(2, 0, 0), Edge(2, 1, 99))
    var edgePartition = builder.toEdgePartition
    assert(edgePartition.withAdditionalEdges(addEdgesWithExSrcExDst.toIterator, 0)
      .iterator.map(_.copy()).toList === expected)

    // Add new edge with new src vertex and existing dst vertex
    val addEdgesWithNewSrcExDst = Set(Edge(3, 0, 99), Edge(3, 1, 99))
    expected = List(Edge(0, 1, 0), Edge(1, 2, 0), Edge(2, 0, 0), Edge(3, 0, 99), Edge(3, 1, 99))
    edgePartition = builder.toEdgePartition
    assert(edgePartition.withAdditionalEdges(addEdgesWithNewSrcExDst.toIterator, 0)
      .iterator.map(_.copy()).toList === expected)

    // Add new edge with existing src vertex and new dst vertex
    val addEdgesWithExSrcNewDst = Set(Edge(0, 3, 99), Edge(1, 3, 99))
    expected = List(Edge(0, 1, 0), Edge(0, 3, 99), Edge(1, 2, 0), Edge(1, 3, 99)
      , Edge(2, 0, 0))
    edgePartition = builder.toEdgePartition
    assert(edgePartition.withAdditionalEdges(addEdgesWithExSrcNewDst.toIterator, 0)
      .iterator.map(_.copy()).toList === expected)

    // Add new edge with new src vertex and new dst vertex
    val addEdgesWithNewSrcNewDst = Set(Edge(3, 3, 99), Edge(4, 5, 99))
    expected = List(Edge(0, 1, 0), Edge(1, 2, 0), Edge(2, 0, 0), Edge(3, 3, 99), Edge(4, 5, 99))
    edgePartition = builder.toEdgePartition
    assert(edgePartition.withAdditionalEdges(addEdgesWithNewSrcNewDst.toIterator, 0)
      .iterator.map(_.copy()).toList === expected)

    // Vertices active set
    val addEdge = Set(Edge(3, 3, 0))
    edgePartition = builder.toEdgePartition.withActiveSet(Iterator(0, 1, 2))
    val ret = edgePartition.withAdditionalEdges(addEdge.toIterator, 0)
    assert(ret.numActives.get == 4)
    assert(ret.isActive(0))
    assert(ret.isActive(1))
    assert(ret.isActive(2))
    assert(ret.isActive(3))

    // Vertex attributes
    val it = builder.toEdgePartition.updateVertices(Iterator((0L, 0), (1L, 1), (2L, 2)))
      .withAdditionalEdges(addEdge.toIterator, 555).tripletIterator(true, false)
    it.foreach { x =>
      if (x.srcId == 3) assert(x.srcAttr == 555)
      else assert(x.srcAttr == x.srcId.toInt)
    }
  }

  test("reverse") {
    val edges = List(Edge(0, 1, 0), Edge(1, 2, 0), Edge(2, 0, 0))
    val reversedEdges = List(Edge(0, 2, 0), Edge(1, 0, 0), Edge(2, 1, 0))
    val builder = new EdgePartitionBuilder[Int, Nothing]
    for (e <- edges) {
      builder.add(e.srcId, e.dstId, e.attr)
    }
    val edgePartition = builder.toEdgePartition
    assert(edgePartition.reverse.iterator.map(_.copy()).toList === reversedEdges)
    assert(edgePartition.reverse.reverse.iterator.map(_.copy()).toList === edges)
  }

  test("map") {
    val edges = List(Edge(0, 1, 0), Edge(1, 2, 0), Edge(2, 0, 0))
    val builder = new EdgePartitionBuilder[Int, Nothing]
    for (e <- edges) {
      builder.add(e.srcId, e.dstId, e.attr)
    }
    val edgePartition = builder.toEdgePartition
    assert(edgePartition.map(e => e.srcId + e.dstId).iterator.map(_.copy()).toList ===
      edges.map(e => e.copy(attr = e.srcId + e.dstId)))
  }

  test("filter") {
    val edges = List(Edge(0, 1, 0), Edge(0, 2, 0), Edge(2, 0, 0))
    val builder = new EdgePartitionBuilder[Int, Int]
    for (e <- edges) {
      builder.add(e.srcId, e.dstId, e.attr)
    }
    val edgePartition = builder.toEdgePartition
    val filtered = edgePartition.filter(et => et.srcId == 0, (vid, attr) => vid == 0 || vid == 1)
    assert(filtered.tripletIterator().toList.map(et => (et.srcId, et.dstId)) === List((0L, 1L)))
  }

  test("groupEdges") {
    val edges = List(
      Edge(0, 1, 1), Edge(1, 2, 2), Edge(2, 0, 4), Edge(0, 1, 8), Edge(1, 2, 16), Edge(2, 0, 32))
    val groupedEdges = List(Edge(0, 1, 9), Edge(1, 2, 18), Edge(2, 0, 36))
    val builder = new EdgePartitionBuilder[Int, Nothing]
    for (e <- edges) {
      builder.add(e.srcId, e.dstId, e.attr)
    }
    val edgePartition = builder.toEdgePartition
    assert(edgePartition.groupEdges(_ + _).iterator.map(_.copy()).toList === groupedEdges)
  }

  test("innerJoin") {
    val aList = List((0, 1, 0), (1, 0, 0), (1, 2, 0), (5, 4, 0), (5, 5, 0))
    val bList = List((0, 1, 0), (1, 0, 0), (1, 1, 0), (3, 4, 0), (5, 5, 0))
    val a = makeEdgePartition(aList)
    val b = makeEdgePartition(bList)

    assert(a.innerJoin(b) { (src, dst, a, b) => a }.iterator.map(_.copy()).toList ===
      List(Edge(0, 1, 0), Edge(1, 0, 0), Edge(5, 5, 0)))
  }

  test("isActive, numActives, replaceActives") {
    val ep = new EdgePartitionBuilder[Nothing, Nothing].toEdgePartition
      .withActiveSet(Iterator(0L, 2L, 0L))
    assert(ep.isActive(0))
    assert(!ep.isActive(1))
    assert(ep.isActive(2))
    assert(!ep.isActive(-1))
    assert(ep.numActives == Some(2))
  }

  test("tripletIterator") {
    val builder = new EdgePartitionBuilder[Int, Int]
    builder.add(1, 2, 0)
    builder.add(1, 3, 0)
    builder.add(1, 4, 0)
    val ep = builder.toEdgePartition
    val result = ep.tripletIterator().toList.map(et => (et.srcId, et.dstId))
    assert(result === Seq((1, 2), (1, 3), (1, 4)))
  }

  test("serialization") {
    val aList = List((0, 1, 1), (1, 0, 2), (1, 2, 3), (5, 4, 4), (5, 5, 5))
    val a: EdgePartition[Int, Int] = makeEdgePartition(aList)
    val javaSer = new JavaSerializer(new SparkConf())
    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)
    val kryoSer = new KryoSerializer(conf)

    for (ser <- List(javaSer, kryoSer); s = ser.newInstance()) {
      val aSer: EdgePartition[Int, Int] = s.deserialize(s.serialize(a))
      assert(aSer.tripletIterator().toList === a.tripletIterator().toList)
    }
  }

  // test("withAdditionalEdgesPerformanceTest") {
  ignore("withAdditionalEdgesPerformanceTest") {
    // scalastyle:off println
    println(" # edges,   # add, full, inc. (ms)")
    // var numAddEdgesItr = Iterator(10)
    val numAddEdgesItr = Iterator(10, 100, 1000, 10000, 100000, 1000000)
    for (numAddEdges <- numAddEdgesItr) {
      val numEdgeItr = Iterator(1000, 10000, 100000, 1000000)
      // val numEdgeItr = Iterator(1000000)
      for (numEdges <- numEdgeItr) {
        val baseBuilder = new EdgePartitionBuilder[Int, Int]
        for (i <- 0 until numEdges) { baseBuilder.add(i, i + 1, i) }
        val basePartition = baseBuilder.toEdgePartition

        val builder = new EdgePartitionBuilder[Int, Int]
        for (i <- 0 until numEdges + numAddEdges) { builder.add(i, i + 1, i) }

        var startTime = System.currentTimeMillis
        val fullPartition = builder.toEdgePartition
        val endTimeScratch = System.currentTimeMillis - startTime

        val addEdgesBuf: scala.collection.mutable.ArrayBuffer[Edge[Int]]
        = new scala.collection.mutable.ArrayBuffer()
        for (i <- 0 until numAddEdges) {
          addEdgesBuf += Edge(numEdges + i, numEdges + i, numEdges + i)
        }
        val addEdgeIterator = addEdgesBuf.toIterator

        startTime = System.currentTimeMillis
        basePartition.withAdditionalEdges(addEdgeIterator, 0)
        val endTimeIncremental = System.currentTimeMillis - startTime

        print("%8d,".format(numEdges) + "%8d,".format(numAddEdges))
        println("%5d,".format(endTimeScratch) + "%5d,".format(endTimeIncremental))
      }
    }
    // scalastyle:on println
  }
}
