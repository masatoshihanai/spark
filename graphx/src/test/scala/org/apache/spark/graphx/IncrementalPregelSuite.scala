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
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class IncrementalPregelSuite extends SparkFunSuite with LocalSparkContext {

  test("initRun, apply") {
    withSpark { sc =>
      val numVertices = 5
      val chain = Graph.fromEdgeTuples(
        sc.parallelize((1 until numVertices)
          .map(x => (x: VertexId, x + 1: VertexId))), 0)
        .partitionBy(PartitionStrategy.EdgePartition1D)
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

      assert(iPregel.asInstanceOf[IncrementalPregelImpl[Int, Int, Int]]
        ._graph.vertices.map(x => (x._1, x._2.toMap())).collect.toSet ===
        Set((1, Map(                                    0 -> 1, -1 -> 0)),
            (2, Map(                           1 -> 1,  0 -> 2, -1 -> 0)),
            (3, Map(                  2 -> 1,  1 -> 2,  0 -> 3, -1 -> 0)),
            (4, Map(         3 -> 1,  2 -> 2,  1 -> 3,  0 -> 4, -1 -> 0)),
            (5, Map(4 -> 1,  3 -> 2,  2 -> 3,  1 -> 4,  0 -> 5, -1 -> 0)))
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
        .partitionBy(PartitionStrategy.EdgePartition1D)
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
      val updated = iPregel.run(addEdges, Long.MaxValue, initFunc = (id, _) => id).cache()

      // Check result
      assert(updated.result.vertices.collect.toSet ===
        Set((1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (0, 0)))

      // Check with original pregel
      val fullResult = chain.partitionBy(PartitionStrategy.EdgePartition1D)
        .addEdges(addEdges, PartitionStrategy.EdgePartition1D, 0)
        .pregel(Long.MaxValue)(vprog, sendMsg, mergeMsg).cache()
      assert(updated.result.vertices.collect.toSet ===
        fullResult.vertices.collect.toSet)

      // Check history
      val initValue = -1
      assert(updated.asInstanceOf[IncrementalPregelImpl[Int, Int, Int]]
        ._graph.vertices.map(x => (x._1, x._2.toMap())).collect.toSet ===
          Set((1, Map(3 -> 0, 0 -> 1, initValue -> 1)),
              (2, Map(2 -> 0, 1 -> 1, 0 -> 2, initValue -> 2)),
              (3, Map(1 -> 0, 0 -> 3, initValue -> 3)),
              (4, Map(2 -> 0, 1 -> 3, 0 -> 4, initValue -> 4)),
              (5, Map(3 -> 0, 2 -> 3, 1 -> 4, 0 -> 5, initValue -> 5)),
              (0, Map(0 -> 0, initValue -> 0)))
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
        (0 until 1).map(x => Edge(rows * cols, rows * cols - 1, 1.0))
      ).cache()

      val initFunc: (VertexId, Long) => Long = (vid, _) => vid
      val ccGraphPlus = gridGraph
        .partitionBy(PartitionStrategy.EdgePartition1D)
        .mapVertices {case (vid, _) => vid }
        .addEdges(addEdge, PartitionStrategy.EdgePartition1D, 0L, initFunc)
        .cache()

      val pregelGraphPlus
        = Pregel(ccGraphPlus, initialMessage)(vprog, sendMessage, mergeMsg).cache()

      val ccGraph = gridGraph.mapVertices { case (vid, _) => vid }.cache()
      val iPregelGraph
        = IncrementalPregel(ccGraph, initialMessage)(vprog, sendMessage, mergeMsg).cache()

      val iPregelUptate = iPregelGraph.run(addEdge, 0L, initFunc).cache()

      assert(pregelGraphPlus.vertices.collect.toList.toSet ===
        iPregelUptate.result.vertices.collect.toList.toSet)
    } // end of withSpark
  } // end of test connected component

  test("PageRank") {
    def compareRanks(a: VertexRDD[Double], b: VertexRDD[Double]): Double = {
      a.leftJoin(b) { case (id, a, bOpt) => (a - bOpt.getOrElse(0.0)) * (a - bOpt.getOrElse(0.0)) }
        .map { case (id, error) => error }.sum()
    }

    // Three functions for Pagerank with Pregel
    val resetProb = 0.15; val tol = 0.001; val errorTol = 1.0e-3
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

    def updateEdgeAttrFunc[ED: ClassTag](addEdge: RDD[Edge[ED]])(g: Graph[_, Double])
        : Graph[_, Double] = {
      val activateMsg = g.vertices.aggregateUsingIndex(
        addEdge.flatMap(x => Iterator((x.srcId, 0))), (x: Int, y: Int) => x: Int)
      val countOutDeg = (edge: EdgeTriplet[_, Double]) => Iterator((edge.srcId, 1))
      val mergeMsg = (x: Int, y: Int) => x + y
      val updatedOutDeg = GraphXUtils.mapReduceTriplets(
        g, countOutDeg, mergeMsg, Some(activateMsg, EdgeDirection.Out)).cache()

      g.joinTriplets[Int](updatedOutDeg,
        EdgeDirection.Out, et => 1.0 / et.srcAttr).cache()
    }

    val pruning: ((Double, Double), (Double, Double)) => Boolean = { (x, y) =>
      math.abs(x._1 - y._1) < 0.001
    }

    // PageRank on LogNormal
    withSpark { sc =>
      val size = 1000
      val logNormal = GraphGenerators.logNormalGraph(sc, size, seed = 1)
        .partitionBy(PartitionStrategy.EdgePartition1D)
        .removeSelfEdges()
        .groupEdges((x,y) => x)
        .cache()

      val degGraph = logNormal
        .outerJoinVertices(logNormal.outDegrees) { (_, _, deg) => deg.getOrElse(0) }.cache()
      val initGraph = degGraph
        .mapTriplets(e => 1.0 / e.srcAttr)
        .mapVertices { (id, attr) => (0.0, 0.0) }
        .partitionBy(PartitionStrategy.EdgePartition1D)
        .cache()
      initGraph.edges.count()

      val iPregelRank = IncrementalPregel(initGraph, resetProb / (1.0 - resetProb),
        activeDirection = EdgeDirection.Out, partitionStrategy = PartitionStrategy.EdgePartition1D)(
        vertexProgram, sendMessage, messageCombiner).cache()
      iPregelRank.result.edges.count()

      val addEdge: RDD[Edge[Double]]
        = sc.parallelize(Array(Edge(size, 728, 1.0)))
          .cache()
      addEdge.count()

       val updateEdgeAttr = updateEdgeAttrFunc(addEdge)(_)
      val updated = iPregelRank.run(addEdge, (0.0, 0.0),
        updateEdgeAttr = Some(updateEdgeAttr)
        , pruningFunc = pruning
      ).cache()
      updated.countVertices()

      val add: RDD[Edge[Int]]
        = sc.parallelize(Array(Edge(size, 728, 1))).cache()
      val logNormalPlus = logNormal
        .partitionBy(PartitionStrategy.EdgePartition1D)
        .addEdges(add, PartitionStrategy.EdgePartition1D, 0).cache()
      logNormalPlus.edges.count()
      val pagerankFullGraph = logNormalPlus.pageRank(tol, resetProb).cache()
      val pagerankFull = pagerankFullGraph.vertices.cache()
      pagerankFull.count()

      assert(compareRanks(pagerankFull,
        updated.result.mapVertices((vid, attr) => attr._1).vertices) < errorTol)
    }
  } // end of test pagerank

  test("ShortestPath") {
    withSpark { sc =>
      // Init graph
      val edgeSeq = Seq((1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)).flatMap {
        case e => Seq(e, e.swap)
      }
      val edges = sc.parallelize(edgeSeq).map { case (v1, v2) => (v1.toLong, v2.toLong) }
      val graph = Graph.fromEdgeTuples(edges, 1)
      val landmarks = Seq(1, 4).map(_.toLong)

      // Init Pregel function
      type SPMap = Map[VertexId, Int]
      val incrementMap = (spmap: SPMap) => spmap.map { case (v, d) => v -> (d + 1) }
      val addMaps = (spmap1: SPMap, spmap2: SPMap) =>
        (spmap1.keySet ++ spmap2.keySet).map {
          k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
        }.toMap
      val vertexProgram = (id: VertexId, attr: SPMap, msg: SPMap) => {
        addMaps(attr, msg)
      }
      val sendMessage = (edge: EdgeTriplet[SPMap, _]) => {
        val newAttr = incrementMap(edge.dstAttr)
        if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
        else Iterator.empty
      }

      val initialMsg = Map[VertexId, Int]()
      val spGraph = graph.mapVertices { (vid, attr) =>
        if (landmarks.contains(vid)) Map(vid -> 0) else Map[VertexId, Int]()
      }
      val iPregel = IncrementalPregel(spGraph, initialMsg)(vertexProgram, sendMessage, addMaps)
      val addEdge = sc.parallelize(Array(Edge(6, 7, 1), Edge(7, 6, 1)))
      val actual = iPregel.run(addEdge, initialMsg).cache()

      // Run full
      val graphPlus = graph.partitionBy(PartitionStrategy.EdgePartition1D)
        .addEdges(addEdge, PartitionStrategy.EdgePartition1D, 0)
      val expected = ShortestPaths.run(graphPlus, landmarks).cache()

      assert(actual.result.vertices.collect.toSet === expected.vertices.collect.toSet)
    } // end of withSpark
  } // end of test ShortestPath

} // end of class IncrementalPregelSuite
