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
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.internal.Logging


// TODO write document
abstract class IncrementalPregel[VD: ClassTag, ED: ClassTag, A: ClassTag]
  extends Serializable {

  val graphWithHistory: Graph[HashMap[Int, VD], ED]

  val result: Graph[VD, ED]

  def run(addEdges: RDD[Edge[ED]], defaultValue: VD, test: A,
      partitionStrategy: PartitionStrategy = PartitionStrategy.RandomVertexCut)
    : IncrementalPregel[VD, ED, A]

  def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : IncrementalPregel[VD, ED, A]

  def cache(): IncrementalPregel[VD, ED, A]

  def checkpoint(): Unit

  def isCheckpointed: Boolean

  def getCheckpointFiles: Seq[String]

  def unpersist(blocking: Boolean = true): IncrementalPregel[VD, ED, A]
}

// TODO write document
object IncrementalPregel extends Logging {

  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
     (graph: Graph[VD, ED],
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either,
      partitionStorategy: PartitionStrategy = PartitionStrategy.RandomVertexCut)
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
      partitionStorategy: PartitionStrategy = PartitionStrategy.RandomVertexCut)
     (vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
  : IncrementalPregel[VD, ED, A] = {
    IncrementalPregelImpl.runAndStoreHistory[VD, ED, A] (
      graph, initialMsg, maxIterations, activeDirection, partitionStorategy)(
      vprog, sendMsg, mergeMsg)
  }

} // end of object IncrementalPregel
