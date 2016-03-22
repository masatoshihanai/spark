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

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

private[graphx]
class IncrementalPregelImpl[VD: ClassTag, ED: ClassTag, A: ClassTag] protected (
    val _graph: Graph[HashMap[Int, VD], ED],
    val _partitionStrategy: PartitionStrategy,
    val _initialMsg: A,
    val _maxIterations: Int = Int.MaxValue,
    val _activeDirection: EdgeDirection = EdgeDirection.Either) (
    val _vprog: (VertexId, VD, A) => VD,
    val _sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
    val _mergeMsg: (A, A) => A)
  extends IncrementalPregel[VD, ED, A] with Serializable {

  /* Default constructor is provided to support serialization */
  protected def this() = this(null, null, null.asInstanceOf[A])(null, null, null)

  @transient override lazy val result: Graph[VD, ED] = {
    _graph.mapVertices{ case (vertexId, vertexValueHistory) =>
      vertexValueHistory(-2)
    }
  }

  override def run(addEdges: RDD[Edge[ED]], defaultValue: VD)
  : IncrementalPregel[VD, ED, A] = {
    // TODO Implement Pruning optimization

    val initValue = IncrementalPregelImpl.initValue
    val currValue = IncrementalPregelImpl.currValue

    def vProgramAndStore(i: Int)(id: VertexId, valueHistory: HashMap[Int, VD], message: A)
    : HashMap[Int, VD] = {
      // TODO the algorithm becomes more complicated for delete or update vertex
      val newVertexValue = _vprog(id, valueHistory(currValue), message)
      valueHistory + (i -> newVertexValue) + (currValue -> newVertexValue)
    }

    def sendMessage(i: Int)(edgeTriplet: EdgeTriplet[HashMap[Int, VD], ED])
    : Iterator[(VertexId, A)] = {
      // TODO the algorithm bacomes more complicated for delete or update vertex
      val edgeTripletWithSingleValue = new EdgeTriplet[VD, ED].set(edgeTriplet)
      edgeTripletWithSingleValue.srcAttr = edgeTriplet.srcAttr.getOrElse(i, defaultValue)
      edgeTripletWithSingleValue.dstAttr = edgeTriplet.dstAttr.getOrElse(i, defaultValue)
      _sendMsg(edgeTripletWithSingleValue)
    }

    // Add Edges
    val newGraph = _graph.addEdges(addEdges,
      HashMap(initValue -> defaultValue, currValue -> defaultValue), _partitionStrategy).cache()

    // TODO what happened if the additional vertex is src?
    // Initiate src vertices with initial message
    val initMessagesToSrc = newGraph.vertices
      .aggregateUsingIndex(addEdges.map(x => (x.srcId, _initialMsg)), _mergeMsg)
    var graph = newGraph.joinVertices(initMessagesToSrc)(vProgramAndStore(0)).cache()

    // Initiate dst vertices with initial messages
    val initMessagesToDst = graph.vertices
      .aggregateUsingIndex(addEdges.map(x => (x.dstId, _initialMsg)), _mergeMsg)
    var messageCount = initMessagesToDst.count()

    // Send messages to neighbors
    var messages = GraphXUtils.mapReduceTriplets(
      graph, sendMessage(0), _mergeMsg, Some(initMessagesToDst, _activeDirection))

    // activateMessages for activate out-going neighbors in next iteration
    var activateMessages = messages.minus(initMessagesToDst)
    // vProgComputeMessages for compute vertex program in this iteration
    var vProgComputeMessages = messages.minus(activateMessages)

    var i = 1
    var oldGraph: Graph[HashMap[PartitionID, VD], ED] = null
    while (messageCount > 0 && i < _maxIterations) {
      oldGraph = graph
      // Compute vertex program with vProgComputeMessages
      graph = oldGraph.joinVertices(vProgComputeMessages)(vProgramAndStore(i))

      val oldMessages = messages
      // Send messages to vertices filtered by activateMessages
      messages = GraphXUtils.mapReduceTriplets(
        graph, sendMessage(i), _mergeMsg, Some(activateMessages, _activeDirection))

      val oldActivateMessages = activateMessages
      activateMessages = messages.minus(oldActivateMessages)
      val oldVProgMessages = vProgComputeMessages
      vProgComputeMessages = messages.minus(activateMessages)

      messageCount = messages.count()
      i += 1

      oldGraph.unpersist(false)
      oldMessages.unpersist(false)
      oldActivateMessages.unpersist(false)
      oldVProgMessages.unpersist(false)
    }
    messages.unpersist(false)
    activateMessages.unpersist(false)
    vProgComputeMessages.unpersist(false)

    new IncrementalPregelImpl(
      graph, _partitionStrategy, _initialMsg, _maxIterations, _activeDirection) (
      _vprog, _sendMsg, _mergeMsg)
  }

  override def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : IncrementalPregel[VD, ED, A] = {
    _graph.persist(newLevel)
    this
  }

  override def cache(): IncrementalPregel[VD, ED, A] = {
    _graph.cache()
    this
  }

  override def checkpoint(): Unit = {
    _graph.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    _graph.isCheckpointed
  }

  override def getCheckpointFiles: Seq[String] = {
    _graph.getCheckpointFiles
  }

  override def unpersist(blocking: Boolean = true): IncrementalPregel[VD, ED, A] = {
    _graph.unpersist(blocking)
    this
  }

} // end of class IncrementalPregelImpl

private[graphx]
object IncrementalPregelImpl extends Logging {

  def runAndStoreHistory[VD: ClassTag, ED: ClassTag, A: ClassTag]
     (graph: Graph[VD, ED],
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either,
      partitionStorategy: PartitionStrategy = PartitionStrategy.RandomVertexCut)
     (vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
    : IncrementalPregelImpl[VD, ED, A] =
  {
    def vProgramAndStore(i: Int)(id: VertexId, valueHistory: HashMap[Int, VD], message: A)
      : HashMap[Int, VD] = {
      val newVertexValue = vprog(id, valueHistory(currValue), message)
      valueHistory + (i -> newVertexValue) + (currValue -> newVertexValue)
    }

    def sendMessage(edgeTriplet: EdgeTriplet[HashMap[Int, VD], ED]): Iterator[(VertexId, A)] = {
      val edgeTripletWithSingleValue = new EdgeTriplet[VD, ED].set(edgeTriplet)
      edgeTripletWithSingleValue.srcAttr = edgeTriplet.srcAttr(currValue)
      edgeTripletWithSingleValue.dstAttr = edgeTriplet.dstAttr(currValue)
      sendMsg(edgeTripletWithSingleValue)
    }

    var graphWithHistory
      = graph.partitionBy(partitionStorategy)
        .mapVertices((_, vData) => HashMap((currValue -> vData), (initValue -> vData)))

    graphWithHistory = graphWithHistory.mapVertices { (vid, vdata) =>
      vProgramAndStore(0)(vid, vdata, initialMsg)
    }.cache()

    var messages = GraphXUtils.mapReduceTriplets(graphWithHistory, sendMessage, mergeMsg)
    var activeMessages = messages.count()

    var prevG: Graph[HashMap[Int, VD], ED] = null
    var i = 1
    while (activeMessages > 0 && i < maxIterations) {
      prevG = graphWithHistory
      graphWithHistory = graphWithHistory.joinVertices(messages)(vProgramAndStore(i)).cache()

      val oldMessages = messages
      messages = GraphXUtils.mapReduceTriplets(
        graphWithHistory, sendMessage, mergeMsg, Some((oldMessages, activeDirection))).cache()

      activeMessages = messages.count()

      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      i += 1
    }
    messages.unpersist(blocking = false)

    new IncrementalPregelImpl(
      graphWithHistory, partitionStorategy, initialMsg, maxIterations, activeDirection) (
      vprog, sendMsg, mergeMsg)
  }

  private val currValue = -2
  private val initValue = -1

} // end to object IncrementalPregelImpl
