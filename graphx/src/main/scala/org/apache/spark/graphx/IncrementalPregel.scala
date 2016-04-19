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

import scala.reflect.ClassTag

import org.apache.spark.graphx.impl.IncrementalPregelImpl
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


/**
 * The graph for incremental Pregel processing, which includes basic graph and
 * all historical processing data in initial Pregel running.
 *
 * @tparam VD the vertex attribute type
 * @tparam ED the edge attribute type
 * @tparam A the message data type for Pregel processing
 */
abstract class IncrementalPregel[VD: ClassTag, ED: ClassTag, A: ClassTag] protected ()
  extends Serializable {

  /**
   * An graph containing latest final result of incremental processing.
   *
   * @return an graph containing latest final result
   */
  val result: Graph[VD, ED]

  /**
   * Run incremental Pregel processing with inputs(`addEdges`).
   * The processing results are exactly same as full Pregel processing.
   *
   * @param addEdges RDD containing new additional edges
   * @param defaultValue Default vertices' value for additional vertices if exist.
   * @param updateEdgeAttr update function for edge attributes before running incremental Pregel.
   *
   * @return an incremental Pregel instance including same Pregel functions and updated processing history.
   */
  def run(
      addEdges: RDD[Edge[ED]],
      defaultValue: VD,
      updateEdgeAttr: Option[Graph[_, ED] => (Graph[_, ED], VertexRDD[_])] = None)
    : IncrementalPregel[VD, ED, A]

  /**
   * Cache the graph and historical processing data at the specified storage level.
   *
   * @param newLevel the level at which to cache the graph and historical data
   *
   * @return a reference to this graph
   */
  def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : IncrementalPregel[VD, ED, A]

  /**
   * Cache the graph and historical processing data at the previously-specified storage level,
   * which default to `MEMORY_ONLY`.
   *
   * @return a reference to this graph
   */
  def cache(): IncrementalPregel[VD, ED, A]

  /**
   * Mark the graph and historical processing data for checkpointing.
   */
  def checkpoint(): Unit

  /**
   * Return whether this graph has been checkpointed or not.
   */
  def isCheckpointed: Boolean

  /**
   * Gets the name ot the files to which the graph and historical processing data was checkpointed.
   */
  def getCheckpointFiles: Seq[String]

  /**
   * Uncache teh graph and historical processing data.
   */
  def unpersist(blocking: Boolean = true): IncrementalPregel[VD, ED, A]
}

/**
 * The IncrementalPregel object contains a collection of routines
 * used to construct and initially run with storing.
 */
object IncrementalPregel extends Logging {

  /**
   * Construct a IncrementalPregel and initially run original Pregel with storing processed log.
   * The processing algorithm consists of 3 part (`vprog`, `sendMsg`, `mergeMsg`).
   * You can use exactly same function with [[Pregel]]'s one.
   * The processing engine automatically runs in the incremental way.
   *
   * You should to assign [[PartitionStrategy]] as well as Pregel's parameters
   * for later additional edges and vertices ([[PartitionStrategy.RandomVertexCut]] by default).
   *
   * @tparam VD (Same as [[Pregel]]) the vertex data type
   * @tparam ED (Same as [[Pregel]]) the edge data type
   * @tparam A (Same as [[Pregel]]) the Pregel message type
   *
   * @param graph (Same as [[Pregel]]) the input graph
   *
   * @param initialMsg (Same as [[Pregel]]) the message each vertex will receive at the first iteration
   *
   * @param maxIterations (Same as [[Pregel]]) the maximum number of iterations to run
   *
   * @param activeDirection (Same as [[Pregel]]) the direction of edges incident to a vertex that
   * received a message in the previous round on which to run `sendMsg`.
   *
   * @param partitionStorategy [[PartitionStrategy]] for input graph and later additional edges.
   *
   * @param vprog (Same as [[Pregel]]) the user-defined vertex program which runs on each vertex
   * and receives the inbound message and computes a new vertex value.
   *
   * @param sendMsg (Same as [[Pregel]]) the user-defined function that is applied to out
   * edges of vertices that received messages in the current iteration.
   *
   * @param mergeMsg (Same as [[Pregel]]) the user-defined function that takes two incoming
   * messages of type A and merges them into a single message of type A.
   * ''This function must be commutative and associative and ideally the size of A
   * should not increase.''
   *
   * @return the resulting graph (instance of [[IncrementalPregel]]), which contains
   * the history of vertices values in all iterations.
   *
   */
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
     (graph: Graph[VD, ED],
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either,
      partitionStorategy: PartitionStrategy = PartitionStrategy.EdgePartition1D)
     (vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
    : IncrementalPregel[VD, ED, A] =
  {
    IncrementalPregel.initRun[VD, ED, A] (
      graph, initialMsg, maxIterations, activeDirection, partitionStorategy)(
      vprog, sendMsg, mergeMsg)
  }

  def initRun[VD: ClassTag, ED: ClassTag, A: ClassTag]
     (graph: Graph[VD, ED],
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either,
      partitionStorategy: PartitionStrategy = PartitionStrategy.EdgePartition1D)
     (vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
  : IncrementalPregel[VD, ED, A] = {
    IncrementalPregelImpl.runAndStoreHistory[VD, ED, A] (
      graph, initialMsg, maxIterations, activeDirection, partitionStorategy)(
      vprog, sendMsg, mergeMsg)
  }

} // end of object IncrementalPregel
