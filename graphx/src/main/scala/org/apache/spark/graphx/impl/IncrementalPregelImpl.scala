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

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

private[graphx]
class IncrementalPregelImpl[VD: ClassTag, ED: ClassTag, A: ClassTag] protected (
    val _graph: Graph[Seq[(Int, VD)], ED],
    val _partStrategy: PartitionStrategy,
    val _initMsg: A,
    val _maxIterations: Int = Int.MaxValue,
    val _activeDirection: EdgeDirection = EdgeDirection.Either) (
    val _vprog: (VertexId, VD, A) => VD,
    val _sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
    val _merge: (A, A) => A)
  extends IncrementalPregel[VD, ED, A] with Serializable {

  /* Default constructor is provided to support serialization */
  protected def this() = this(null, null, null.asInstanceOf[A])(null, null, null)

  @transient override lazy val result: Graph[VD, ED] = {
    _graph.mapVertices{ case (vertexId, vertexValueHistory) =>
      vertexValueHistory.head._2
    }
  }

  override def run(
      edges: RDD[Edge[ED]],
      defaultValue: VD,
      updateEdgeAttr: Option[Graph[_, ED] => (Graph[_, ED], VertexRDD[_])])
  : IncrementalPregel[VD, ED, A] = {
    def vProgramAndStore(iteration: Int)(
        id: VertexId, valueHistory: Seq[(Int, VD)], message: A)
    : Seq[(Int, VD)] = {
      def rollback(valueHistory: Seq[(Int, VD)]): Seq[(Int, VD)] = {
        if (valueHistory.head._1 == iteration) valueHistory.tail
        else if (valueHistory.head._1 < iteration) valueHistory
        else rollback(valueHistory.tail)
      }
      val rollbackedHistory = rollback(valueHistory)
      val newVertexValue = _vprog(id, rollbackedHistory.head._2, message)
      (iteration -> newVertexValue) +: rollbackedHistory
    }

    def sendMessage(itr: Int)(edgeTriplet: EdgeTriplet[Seq[(Int, VD)], ED])
    : Iterator[(VertexId, A)] = {
      // TODO try to use index for efficient access (ex. Seq[ItrIndx] and HashMap[(ItrIndx, VD)])
      def rollback(valueHistory: Seq[(Int, VD)]): Seq[(Int, VD)] = {
        if (valueHistory.head._1 == itr) valueHistory
        else if (valueHistory.head._1 < itr) valueHistory
        else rollback(valueHistory.tail)
      }
      val et = new EdgeTriplet[VD, ED].set(edgeTriplet)
      et.srcAttr = rollback(edgeTriplet.srcAttr).head._2
      et.dstAttr = rollback(edgeTriplet.dstAttr).head._2
      // Ignore messages from an origin which does not have any history.
      _sendMsg(et).flatMap { case (vid, msg) =>
        if (vid == et.srcId && (edgeTriplet.dstAttr.head._1 < itr)) Iterator.empty
        else if (vid == et.dstId && (edgeTriplet.srcAttr.head._1 < itr)) Iterator.empty
        else Iterator((vid, msg))
      }
    }

    def sendNull(edgeTriplet: EdgeTriplet[Seq[(Int, VD)], ED])
    : Iterator[(VertexId, Byte)] = {
      if (_activeDirection == EdgeDirection.Out) {
        Iterator((edgeTriplet.dstId, 1.toByte))
      } else { // _activeDirection == EdgeDirection.Either
        Iterator((edgeTriplet.srcId, 1.toByte), (edgeTriplet.dstId, 1.toByte))
      }
    }

    def mergeNull(msg1: Byte, msg2: Byte): Byte = 1.toByte

    var (graph, vProgMsg) = updateEdgeAttr match {
      case Some(f) => { // Case that there are some updated edges.
        val x = f(_graph.addEdges(edges, _partStrategy, Seq(-1 -> defaultValue)).cache())
        val initGraph = x._1.asInstanceOf[Graph[Seq[(PartitionID, VD)], ED]].cache()
        val initActivateMsg = x._2.cache()
        // In this case, _initMsg have to be sent to all edges including src/dst vertices
        val initMsg = (et: EdgeTriplet[Seq[(Int, VD)], ED]) => {
          Iterator((et.dstId, _initMsg), (et.srcId, _initMsg))
        }
        (initGraph,
          GraphXUtils.mapReduceTriplets(initGraph, initMsg, (x: A, y: A) => x,
            Some(initActivateMsg, EdgeDirection.Either)).cache())
      }
      case None => { // Case that there is no updated edge.
        val initGraph = _graph.addEdges(
          edges, _partStrategy, Seq(-1 -> defaultValue)).cache()
        // In this case, _initMsg is sent to only src/dst vertices
        (initGraph,
          initGraph.vertices.aggregateUsingIndex(edges.flatMap(
            x => Iterator((x.srcId, _initMsg), (x.dstId, _initMsg))), _merge).cache())
      }
    }

    var messageCount = vProgMsg.count

    var i = 0
    var oldGraph: Graph[Seq[(PartitionID, VD)], ED] = null
    while (messageCount > 0 && i < _maxIterations) {
      oldGraph = graph
      // Compute vertex program with vProgMsg
      graph = graph.joinVertices(vProgMsg)(vProgramAndStore(i)).cache()

      // Send activate messages to destination neighbors
      val activateMsg = GraphXUtils.mapReduceTriplets(
        graph, sendNull, mergeNull, Some(vProgMsg, _activeDirection)).cache()

      // Pull messages from the neighbors' origin
      val oldVProgMsg = vProgMsg
      vProgMsg = GraphXUtils.mapReduceTriplets(
        graph, sendMessage(i), _merge, Some(activateMsg, _activeDirection.reverse)).cache()

      messageCount = vProgMsg.count()
      i += 1

      oldGraph.unpersistVertices(false)
      oldGraph.edges.unpersist(false)
      oldVProgMsg.unpersist(false)
    }
    vProgMsg.unpersist(false)

    new IncrementalPregelImpl(
      graph, _partStrategy, _initMsg, _maxIterations, _activeDirection) (
      _vprog, _sendMsg, _merge)
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
    def vProgramAndStore(i: Int)(id: VertexId, valueHistory: Seq[(Int, VD)], message: A)
      : Seq[(Int, VD)] = {
      val newVertexValue = vprog(id, valueHistory.head._2, message)
      (i -> newVertexValue) +: valueHistory
    }

    def sendMessage(edgeTriplet: EdgeTriplet[Seq[(Int, VD)], ED]): Iterator[(VertexId, A)] = {
      val edgeTripletWithSingleValue = new EdgeTriplet[VD, ED].set(edgeTriplet)
      edgeTripletWithSingleValue.srcAttr = edgeTriplet.srcAttr.head._2
      edgeTripletWithSingleValue.dstAttr = edgeTriplet.dstAttr.head._2
      sendMsg(edgeTripletWithSingleValue)
    }

    var graphWithHistory = graph.partitionBy(partitionStorategy)
      .mapVertices((_, vData) => Seq(-1 -> vData))
      .mapVertices { (vid, vdata) =>
        vProgramAndStore(0)(vid, vdata, initialMsg)
      }.cache()

    var messages = GraphXUtils.mapReduceTriplets(graphWithHistory, sendMessage, mergeMsg)
    var messageCount = messages.count

    var prevG: Graph[Seq[(Int, VD)], ED] = null
    var i = 1
    while (messageCount > 0 && i < maxIterations) {
      prevG = graphWithHistory
      graphWithHistory = graphWithHistory.joinVertices(messages)(vProgramAndStore(i)).cache()

      val oldMessages = messages
      messages = GraphXUtils.mapReduceTriplets(
        graphWithHistory, sendMessage, mergeMsg, Some((oldMessages, activeDirection))).cache()

      messageCount = messages.count

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

} // end to object IncrementalPregelImpl
