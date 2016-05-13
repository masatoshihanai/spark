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
import scala.collection.immutable.HashMap

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

private[graphx]
class VertexStore[VD: ClassTag] (
    val values: HashMap[Int, VD],
    val latestUpdateItr: Int,
    val ignorable: Boolean) extends Serializable {

  def rollback(itr: Int): VertexStore[VD] = {
    var i = itr - 1
    while (values.get(i).isEmpty) {
      i -= 1
    }
    new VertexStore(values, i, this.ignorable)
  }

  def updated(i: Int, value: VD): VertexStore[VD] = {
    val newLatestItr = if (i > latestUpdateItr) i else latestUpdateItr
    new VertexStore(values.updated(newLatestItr, value), newLatestItr, false)
  }

  def isIgnorable(): Boolean = {
    ignorable
  }

  def ignore(itr: Int): VertexStore[VD] = {
    new VertexStore[VD](this.values, Int.MaxValue, true)
  }

  def getLatest(itr: Int): VD = {
    if (latestUpdateItr > itr) {
      var i = itr
      while (values.get(i).isEmpty) {
        i -= 1
      }
      values.get(i).get
    } else {
      values.get(latestUpdateItr).get
    }
  }

  def getOld(itr: Int): Option[VD] = {
    values.get(itr)
  }

  def isDefined(itr: Int): Boolean = {
    values.isDefinedAt(itr)
  }

  def toMap(): Map[Int, VD] = {
    values.filterKeys(key => key <= latestUpdateItr)
  }
}

private[graphx]
class IncrementalPregelImpl[VD: ClassTag, ED: ClassTag, A: ClassTag] protected (
    val _graph: Graph[VertexStore[VD], ED],
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
      vertexValueHistory.getLatest(100) // FIXME
      //vertexValueHistory.getLatest(Int.MaxValue - 1)
    }
  }

  override def run(
      edges: RDD[Edge[ED]],
      defaultValue: VD,
      initFunc: (VertexId, VD) => VD,
      updateEdgeAttr: Option[Graph[_, ED] => Graph[_, ED]],
      pruningFunc: (VD, VD) => Boolean)
  : IncrementalPregel[VD, ED, A] = {
    def vProgramAndStore(itr: Int)(
        id: VertexId, valueHistory: VertexStore[VD], message: A)
    : VertexStore[VD] = {
      val value = valueHistory.rollback(itr)
      val newValue = _vprog(id, value.getLatest(itr), message)
      value.getOld(itr) match {
        case Some(old) => {
          if (pruningFunc(old, newValue)) {
            value.ignore(itr)
          }
          else value.updated(itr, newValue)
        }
        case None => value.updated(itr, newValue)
      }
    }

    def sendMessage(itr: Int)(edgeTriplet: EdgeTriplet[VertexStore[VD], ED])
    : Iterator[(VertexId, A)] = {
      val et = new EdgeTriplet[VD, ED].set(edgeTriplet)
      et.srcAttr = edgeTriplet.srcAttr.getLatest(itr)
      et.dstAttr = edgeTriplet.dstAttr.getLatest(itr)
      // Ignore messages from an origin which does not have any history.
      _sendMsg(et).flatMap { case (vid, msg) =>
        if (vid == et.srcId && (!edgeTriplet.dstAttr.isDefined(itr))) Iterator.empty
        else if (vid == et.dstId && (!edgeTriplet.srcAttr.isDefined(itr))) Iterator.empty
        else Iterator((vid, msg))
      }
    }

    def sendNull(edgeTriplet: EdgeTriplet[VertexStore[VD], ED])
    : Iterator[(VertexId, Int)] = {
      if (_activeDirection == EdgeDirection.Out) {
        if (edgeTriplet.srcAttr.isIgnorable()) Iterator.empty
        else Iterator((edgeTriplet.dstId, 1), (edgeTriplet.srcId, 1))
      } else { // _activeDirection == EdgeDirection.Either
        if (edgeTriplet.srcAttr.isIgnorable() && edgeTriplet.dstAttr.isIgnorable()) {
          Iterator.empty
        } else if (edgeTriplet.srcAttr.isIgnorable()) {
          Iterator((edgeTriplet.dstId, 1))
        } else if (edgeTriplet.dstAttr.isIgnorable()) {
          Iterator((edgeTriplet.srcId, 1))
        } else {
          Iterator((edgeTriplet.srcId, 1), (edgeTriplet.dstId, 1))
        }
      }
    }

    def mergeNull(msg1: Int, msg2: Int): Int = 1

    val initVertFunc: (VertexId, VertexStore[VD]) => VertexStore[VD] = { (vid, vdata) =>
      new VertexStore[VD](
        HashMap(-1 -> initFunc(vid, vdata.getLatest(-1))),
        vdata.latestUpdateItr,
        vdata.ignorable)
    }

    val updateEdge = updateEdgeAttr match {
      case Some(f) => f
      case _ => {graph: Graph[_, ED] => graph}
    }

    var graph = updateEdge(_graph.addEdges(edges, _partStrategy,
      new VertexStore[VD](HashMap(-1 -> defaultValue), 0, false), initVertFunc))
      .asInstanceOf[Graph[VertexStore[VD], ED]]
    var vProgMsg = graph.vertices.aggregateUsingIndex(edges.flatMap(
      x => Iterator((x.srcId, _initMsg), (x.dstId, _initMsg))), _merge)

    graph.localCheckpoint()
    graph.vertices.count(); graph.edges.count()

    vProgMsg.localCheckpoint()
    var messageCount = vProgMsg.count

    var i = 0
    var oldGraph: Graph[VertexStore[VD], ED] = null

    while (messageCount > 0 && i < _maxIterations) {
      oldGraph = graph
      // Compute vertex program with vProgMsg
      graph = graph.joinVertices(vProgMsg)(vProgramAndStore(i)).localCheckpoint()
      graph.vertices.count(); graph.edges.count()

      // Send activate messages to destination neighbor
      val activateMsg =
        GraphXUtils.mapReduceTriplets(graph, sendNull, mergeNull, Some(vProgMsg, _activeDirection))
        //.unionVertex(vProgMsg.mapValues((_, vdata) => 1)).cache()

      // Pull messages from the neighbors' origin
      val oldVProgMsg = vProgMsg
      vProgMsg = GraphXUtils.mapReduceTriplets(
        graph, sendMessage(i), _merge, Some(activateMsg, _activeDirection.reverse)).localCheckpoint()

      messageCount = vProgMsg.count()
      i += 1

      oldGraph.unpersistVertices(false)
      oldGraph.edges.unpersist(false)
      oldVProgMsg.unpersist(false)
    }
    vProgMsg.unpersist(false)

    // FIXME temporary solution for checkpoint() before action (e.g. Graph.vertices.collect())
    val newGraph = graph.mapVertices((_, vdata) => vdata)

    new IncrementalPregelImpl(
      newGraph, _partStrategy, _initMsg, _maxIterations, _activeDirection) (
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

  override def countVertices(): Long = {
    _graph.vertices.count()
  }

} // end of class IncrementalPregelImpl

private[graphx]
object IncrementalPregelImpl extends Logging {

  def runAndStoreHistory[VD: ClassTag, ED: ClassTag, A: ClassTag]
     (graph: Graph[VD, ED],
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either,
      partitionStorategy: PartitionStrategy = PartitionStrategy.EdgePartition1D)
     (vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
    : IncrementalPregelImpl[VD, ED, A] =
  {
    def vProgramAndStore(iteration: Int)(id: VertexId, valueHistory: VertexStore[VD], message: A)
    : VertexStore[VD] = {
      val value = valueHistory.rollback(iteration)
      val newVertexValue = vprog(id, value.getLatest(iteration), message)
      value.updated(iteration, newVertexValue)
    }

    def sendMessage(itr: Int)(edgeTriplet: EdgeTriplet[VertexStore[VD], ED])
      : Iterator[(VertexId, A)] = {
      val et = new EdgeTriplet[VD, ED].set(edgeTriplet)
      et.srcAttr = edgeTriplet.srcAttr.getLatest(itr)
      et.dstAttr = edgeTriplet.dstAttr.getLatest(itr)
      sendMsg(et)
    }

    var graphWithHistory = graph.partitionBy(partitionStorategy)
      .mapVertices((_, vData) => new VertexStore[VD](HashMap(-1 -> vData), -1, false))
      .mapVertices { (vid, vdata) =>
        vProgramAndStore(0)(vid, vdata, initialMsg)
      }.cache()

    var messages = GraphXUtils.mapReduceTriplets(graphWithHistory, sendMessage(0), mergeMsg)
    var messageCount = messages.count

    var prevG: Graph[VertexStore[VD], ED] = null
    var i = 1
    while (messageCount > 0 && i < maxIterations) {
      prevG = graphWithHistory
      graphWithHistory = graphWithHistory.joinVertices(messages)(vProgramAndStore(i)).localCheckpoint()
      graphWithHistory.vertices.count()
      graphWithHistory.edges.count()

      val oldMessages = messages
      messages = GraphXUtils.mapReduceTriplets(
        graphWithHistory, sendMessage(i), mergeMsg, Some((oldMessages, activeDirection))).localCheckpoint()

      messageCount = messages.count

      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      i += 1
    }
    messages.unpersist(blocking = false)

    // FIXME temporary solution for checkpoint() before action (e.g. Graph.vertices.collect())
    val newGraph = graphWithHistory.mapVertices((_, vdata) => vdata)

    new IncrementalPregelImpl(
      newGraph, partitionStorategy, initialMsg, maxIterations, activeDirection) (
      vprog, sendMsg, mergeMsg)
  }

} // end to object IncrementalPregelImpl
