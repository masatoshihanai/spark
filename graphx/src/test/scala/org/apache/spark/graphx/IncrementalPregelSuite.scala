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

package org.apache.spark.graphx

import org.apache.spark.graphx.impl.IncrementalPregelImpl

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx.util.GraphGenerators

class IncrementalPregelSuite extends SparkFunSuite with LocalSparkContext {

  test("initRun, apply") {
    withSpark { sc =>
      val numVertices = 5
      val chain = Graph.fromEdgeTuples(
        sc.parallelize((1 until numVertices)
          .map(x => (x: VertexId, x + 1: VertexId))), 0)
        .partitionBy(PartitionStrategy.RandomVertexCut)
        .cache()
      assert(chain.vertices.collect.toSet ===
        (1 to numVertices).map(x => (x: VertexId, 0)).toSet)

      // Minimum ID
      // ID (1) (2) (3) (4) (5)
      //     1 - 2 - 3 - 4 - 5  (iteration 0)
      //     1 - 1 - 2 - 3 - 4  (iteration 1)
      //     1 - 1 - 1 - 2 - 3  (iteration 2)
      //     1 - 1 - 1 - 1 - 2  (iteration 3)
      //     1 - 1 - 1 - 1 - 1  (iteration 4)
      val vprog = { (id: VertexId, value: Int, message: Int) =>
        Math.min(id, message).toInt
      }
      val sendMsg = { et: EdgeTriplet[Int, Int] =>
        if (et.dstAttr != et.srcAttr) {
          Iterator((et.dstId, et.srcAttr))
        } else {
          Iterator.empty
        }
      }
      val mergeMsg = ((a: Int, b: Int) => Math.min(a, b))

      val iPregel = IncrementalPregel.initRun(chain, Int.MaxValue)(vprog, sendMsg, mergeMsg).cache()

      assert(iPregel.asInstanceOf[IncrementalPregelImpl[Int, Int, Int]]._graph.vertices.collect.toSet ===
        Set((1, Seq(0 -> 1, -1 -> 0)),
            (2, Seq(1 -> 1,  0 -> 2, -1 -> 0)),
            (3, Seq(2 -> 1,  1 -> 2,  0 -> 3, -1 -> 0)),
            (4, Seq(3 -> 1,  2 -> 2,  1 -> 3,  0 -> 4, -1 -> 0)),
            (5, Seq(4 -> 1,  3 -> 2,  2 -> 3,  1 -> 4,  0 -> 5, -1 -> 0)))
      )
      assert(iPregel.result.vertices.collect.toSet ===
        Set((1, 1), (2, 1), (3, 1), (4, 1), (5, 1))
      )
    }
  } // end of test("initRun, apply")

  test("run") {
    withSpark { sc =>
      val numVertices = 5

      val chain = Graph.fromEdgeTuples(
        sc.parallelize((1 until numVertices)
          .map(x => (x: VertexId, x + 1: VertexId))), 0)
        .partitionBy(PartitionStrategy.RandomVertexCut)
        .mapVertices((id, _) => id)
        .cache()
      assert(chain.vertices.collect.toSet ===
        (1 to numVertices).map(x => (x, x)).toSet)

      // Minimum ID
      // ID (1) (2) (3) (4) (5)
      //     1 - 2 - 3 - 4 - 5  (iteration 0)
      //     1 - 1 - 2 - 3 - 4  (iteration 1)
      //     1 - 1 - 1 - 2 - 3  (iteration 2)
      //     1 - 1 - 1 - 1 - 2  (iteration 3)
      //     1 - 1 - 1 - 1 - 1  (iteration 4)
      val vprog = { (id: VertexId, value: Long, message: Long) =>
        Math.min(value, message)
      }
      val sendMsg = (edge: EdgeTriplet[VertexId, Int]) => {
        if (edge.srcAttr < edge.dstAttr) {
          Iterator((edge.dstId, edge.srcAttr))
        } else if (edge.srcAttr > edge.dstAttr) {
          Iterator((edge.srcId, edge.dstAttr))
        } else {
          Iterator.empty
        }
      }
      val mergeMsg = ((a: Long, b: Long) => Math.min(a, b))

      val iPregel = IncrementalPregel
        .initRun(chain, Long.MaxValue)(vprog, sendMsg, mergeMsg).cache()

      // Add Edge 0 to 3
      //            (0)
      //             v
      // ID (1) (2) (3) (4)  (5)
      //     1 - 2 - 3 - 4 - 5  (iteration 0)
      //     1 - 1 - 0 - 3 - 4  (iteration 1)
      //     1 - 0 - 0 - 0 - 3  (iteration 2)
      //     0 - 0 - 0 - 0 - 0  (iteration 3)
      //     0 - 0 - 0 - 0 - 0  (iteration 4)
      val addEdges = sc.parallelize(Array(Edge(0, 3, 0)))
      val updated = iPregel.run(addEdges, 0).cache()

      // Check result
      assert(updated.result.vertices.collect.toSet ===
        Set((1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (0, 0)))

      // Check with original pregel
      val fullResult = chain.partitionBy(PartitionStrategy.RandomVertexCut)
        .addEdges(addEdges, PartitionStrategy.RandomVertexCut, 0)
        .pregel(Long.MaxValue)(vprog, sendMsg, mergeMsg).cache()
      assert(updated.result.vertices.collect.toSet ===
        fullResult.vertices.collect.toSet)

      // Check history
      val initValue = -1
      assert(updated.asInstanceOf[IncrementalPregelImpl[Int, Int, Int]]
        ._graph.vertices.collect.toSet ===
          Set((1, List(3 -> 0, 0 -> 1, initValue -> 1)),
              (2, List(2 -> 0, 1 -> 1, 0 -> 2, initValue -> 2)),
              (3, List(1 -> 0, 0 -> 3, initValue -> 3)),
              (4, List(2 -> 0, 1 -> 3, 0 -> 4, initValue -> 4)),
              (5, List(3 -> 0, 2 -> 3, 1 -> 4, 0 -> 5, initValue -> 5)),
              (0, List(0 -> 0, initValue -> 0)))
      )
    } // end of withSpark
  } // end of test run

  test("Connected component") {
    withSpark { sc =>
      val vprog = (id: VertexId, attr: Long, msg: Long) => math.min(attr, msg)
      val sendMessage = (edge: EdgeTriplet[VertexId, Double]) => {
        if (edge.srcAttr < edge.dstAttr) {
          Iterator((edge.dstId, edge.srcAttr))
        } else if (edge.srcAttr > edge.dstAttr) {
          Iterator((edge.srcId, edge.dstAttr))
        } else {
          Iterator.empty
        }
      }
      val mergeMsg = (a: Long, b: Long) => math.min(a, b)

      val rows = 10; val cols = 10
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols).cache()
      val initialMessage = Long.MaxValue
      val addEdge = sc.parallelize(
        (0 until rows).map(x => Edge(10 * 10 + x, x, 1.0))
        ++ (0 until cols - 1).map(y => Edge(10 * 10 + y, 10 * 10 + y + 1, 1.0))
      ).cache()

      val ccGraphPlus = gridGraph
        .partitionBy(PartitionStrategy.RandomVertexCut)
        .mapVertices {case (vid, _) => vid }
        .addEdges(addEdge, PartitionStrategy.RandomVertexCut, 0L).cache()

      val pregelGraphPlus
        = Pregel(ccGraphPlus, initialMessage)(vprog, sendMessage, mergeMsg).cache()

      val ccGraph = gridGraph.mapVertices { case (vid, _) => vid }.cache()
      val iPregelGraph
        = IncrementalPregel(ccGraph, initialMessage)(vprog, sendMessage, mergeMsg).cache()

      val iPregelUptate = iPregelGraph.run(addEdge, 0L).cache()

      assert(pregelGraphPlus.vertices.collect.toList.toSet ===
        iPregelUptate.result.vertices.collect.toList.toSet)
    } // end of withSpark
  } // end of test connected component

  test("PageRank") {
    def compareRanks(a: VertexRDD[Double], b: VertexRDD[Double]): Double = {
      a.leftJoin(b) { case (id, a, bOpt) => (a - bOpt.getOrElse(0.0)) * (a - bOpt.getOrElse(0.0)) }
        .map { case (id, error) => error }.sum()
    }
    withSpark { sc =>
      val rows = 5; val cols = 5
      val resetProb = 0.15
      val tol = 0.0001; val errorTol = 1.0e-5
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols).cache()

      val pagerankGraph = gridGraph
        .outerJoinVertices(gridGraph.outDegrees) {(_, _, deg) => deg.getOrElse(0)}
        .mapTriplets( e => 1.0 / e.srcAttr )
        .mapVertices {(id, attr) => (0.0, 0.0)}
        .cache()

      val vertexProgram = (id: VertexId, attr: (Double, Double), msgSum: Double) => {
        val (oldPR, lastDelta) = attr
        val newPR = oldPR + (1.0 - resetProb) * msgSum
        (newPR, newPR - oldPR)
      }
      val sendMessage = (edge: EdgeTriplet[(Double, Double), Double]) => {
        if (edge.srcAttr._2 > tol) {
          Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
        } else {
          Iterator.empty
        }
      }
      val messageCombiner = (a: Double, b: Double) => a + b

      val iPregelRank = IncrementalPregel(pagerankGraph, resetProb / (1.0 - resetProb),
        activeDirection = EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner).cache()

      val addEdge = sc.parallelize(
        (0 until rows).map(x => Edge(rows * cols + x, x, 1.0))
        //++ (0 until cols - 1).map(x => Edge(rows * cols + x, rows * cols + x + 1, 1.0))
      ).cache()

      val gridGraphPlus = gridGraph
        .partitionBy(PartitionStrategy.EdgePartition1D)
        .addEdges(addEdge, PartitionStrategy.EdgePartition1D, (0, 0)).cache()

      val dynamicRankPlus = gridGraphPlus.pageRank(tol, resetProb).vertices.cache()
      val updated = iPregelRank.run(addEdge, (0.0, 0.0)).cache()

      assert(compareRanks(dynamicRankPlus,
        updated.result.mapVertices((vid, attr) => attr._1).vertices) < errorTol)
      assert(dynamicRankPlus.collect.toSet ===
        updated.result.mapVertices((vid, attr) => attr._1).vertices.collect.toSet)
    } // end of withSpark
  } // end of test pagerank

  // ignore("Incremental Pregel Performance Test")
  test("Incremental Pregel Performance Test") {
    // TODO performacne test
  }

} // end of class IncrementalPregelSuite
