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

import scala.collection.immutable.HashMap

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx.lib.GridPageRank
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
      // ID (1)  (2)  (3)  (4)  (5)
      //     0 -> 0 -> 0 -> 0 -> 0  Initial message = Int.MaxValue
      //     1 -> 2 -> 3 -> 4 -> 5  (iteration 0)
      //     1 -> 1 -> 2 -> 3 -> 4  (iteration 1)
      //     1 -> 1 -> 1 -> 2 -> 3  (iteration 2)
      //     1 -> 1 -> 1 -> 1 -> 2  (iteration 3)
      //     1 -> 1 -> 1 -> 1 -> 1  (iteration 4)
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

      val currValue = -2
      val initValue = -1
      assert(iPregel.graphWithHistory.vertices.collect.toSet ===
        Set((1, HashMap(currValue -> 1, initValue -> 0, 0 -> 1)),
            (2, HashMap(currValue -> 1, initValue -> 0, 0 -> 2, 1 -> 1)),
            (3, HashMap(currValue -> 1, initValue -> 0, 0 -> 3, 1 -> 2, 2 -> 1)),
            (4, HashMap(currValue -> 1, initValue -> 0, 0 -> 4, 1 -> 3, 2 -> 2, 3 -> 1)),
            (5, HashMap(currValue -> 1, initValue -> 0, 0 -> 5, 1 -> 4, 2 -> 3, 3 -> 2, 4 -> 1)))
      )
      assert(iPregel.result.vertices.collect.toSet ===
        Set((1, 1), (2, 1), (3, 1), (4, 1), (5, 1))
      )
    }
  } // end of test("initRun, apply")

  test("run") {
    withSpark { sc =>
      val numVertices = 5
      val defalutVertexValue = 0
      val chain = Graph.fromEdgeTuples(
        sc.parallelize((1 until numVertices)
          .map(x => (x: VertexId, x + 1: VertexId))), defalutVertexValue)
        .partitionBy(PartitionStrategy.RandomVertexCut)
        .cache()
      assert(chain.vertices.collect.toSet ===
        (1 to numVertices).map(x => (x: VertexId, defalutVertexValue)).toSet)

      // Minimum ID
      // ID (1)  (2)  (3)  (4)  (5)
      //     0 -> 0 -> 0 -> 0 -> 0  Initial message = Int.MaxValue
      //     1 -> 2 -> 3 -> 4 -> 5  (iteration 0)
      //     1 -> 1 -> 2 -> 3 -> 4  (iteration 1)
      //     1 -> 1 -> 1 -> 2 -> 3  (iteration 2)
      //     1 -> 1 -> 1 -> 1 -> 2  (iteration 3)
      //     1 -> 1 -> 1 -> 1 -> 1  (iteration 4)
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

      // Add Edge 0 to 3
      //              (0)
      //               v
      // ID (1)  (2)  (3)  (4)  (5)
      //     0 -> 0 -> 0 -> 0 -> 0  Initial message = Int.MaxValue
      //     1 -> 2 -> 3 -> 4 -> 5  (iteration 0)
      //     1 -> 1 -> 0 -> 3 -> 4  (iteration 1)
      //     1 -> 1 -> 0 -> 0 -> 3  (iteration 2)
      //     1 -> 1 -> 0 -> 0 -> 0  (iteration 3)
      //     1 -> 1 -> 0 -> 0 -> 0  (iteration 4)
      val addEdges = sc.parallelize(Array(Edge(0, 3, 0)))
      val updated = iPregel.run(addEdges, 0, 0).cache()

      assert(updated.result.vertices.collect().toSet ===
        Set((1, 1), (2, 1), (3, 0), (4, 0), (5, 0), (0, 0))
      )
      // TODO check history
    }
  }

  test("Chain Propagation") {
    withSpark { sc =>
      val n = 5
      val chain = Graph.fromEdgeTuples(
        sc.parallelize((1 until n).map(x => (x: VertexId, x + 1: VertexId)), 3),
        0).cache()
      assert(chain.vertices.collect.toSet === (1 to n).map(x => (x: VertexId, 0)).toSet)
      val chainWithSeed = chain.mapVertices { (vid, attr) => if (vid == 1) 1 else 0 }.cache()
      assert(chainWithSeed.vertices.collect.toSet ===
        Set((1: VertexId, 1)) ++ (2 to n).map(x => (x: VertexId, 0)).toSet)
      val iPregelInit = IncrementalPregel(chainWithSeed, 0)(
        (vid, attr, msg) => math.max(msg, attr),
        et => if (et.dstAttr != et.srcAttr) Iterator((et.dstId, et.srcAttr)) else Iterator.empty,
        (a: Int, b: Int) => math.max(a, b))
      assert(iPregelInit.result.vertices.collect.toSet ===
        chain.vertices.mapValues { (vid, attr) => attr + 1 }.collect.toSet)

      // Incremental Processing
      val addEdges = sc.parallelize(List(Edge()))
      // TODO Chain Propagation implement incremental processing

    }
  }

  def compareRanks(a: VertexRDD[Double], b: VertexRDD[Double]): Double = {
    a.leftJoin(b) { case (id, a, bOpt) => (a - bOpt.getOrElse(0.0)) * (a - bOpt.getOrElse(0.0)) }
      .map { case (id, error) => error }.sum()
  }

  test("PageRank") {
    withSpark { sc =>
      val rows = 10
      val cols = 10
      val resetProb = 0.15
      val tol = 0.0001
      val numIter = 50
      val errorTol = 1.0e-5
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols).cache()

      val staticRanks = gridGraph.staticPageRank(numIter, resetProb).vertices.cache()
      val dynamicRanks = gridGraph.pageRank(tol, resetProb).vertices.cache()
      val referenceRanks = VertexRDD(
        sc.parallelize(GridPageRank(rows, cols, numIter, resetProb))).cache()

      assert(compareRanks(staticRanks, referenceRanks) < errorTol)
      assert(compareRanks(dynamicRanks, referenceRanks) < errorTol)

      // TODO PageRank implmenet incremental processing
    }
  }

  // ignore("Incremental Pregel Performance Test")
  test("Incremental Pregel Performance Test") {
    // TODO performacne test
  }

}
