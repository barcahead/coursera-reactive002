/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeSet extends Actor with ActorLogging {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case Insert(r, id, elem) => root ! Insert(r, id, elem)
    case Contains(r, id, elem) => root ! Contains(r, id, elem)
    case Remove(r, id, elem) => root ! Remove(r, id, elem)
    case GC => {
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case CopyFinished => {
      root ! PoisonPill
      root = newRoot
      context.become(normal)
      pendingQueue.foreach(newRoot ! _)
      pendingQueue = Queue.empty[Operation]
    }
    case op: Operation => pendingQueue = pendingQueue.enqueue(op)
  }

}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor  with ActorLogging{
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(r, id, newElem) => {
      if (newElem == elem) {
        if (removed) removed = false
        r ! OperationFinished(id)
      } else if (newElem < elem) {
        if (subtrees.get(Left).isEmpty) {
          subtrees = subtrees+(Left -> context.actorOf(Props(new BinaryTreeNode(newElem, false))))
        }
        subtrees.get(Left).get ! Insert(r, id, newElem)
      } else {
        if (subtrees.get(Right).isEmpty) {
          subtrees = subtrees+(Right -> context.actorOf(Props(new BinaryTreeNode(newElem, false))))
        }
        subtrees.get(Right).get ! Insert(r, id, newElem)
      }
    }
    case Contains(r, id, el) => {
      if (!removed && el == elem) r ! ContainsResult(id, true)
      else if (el < elem && subtrees.get(Left).isDefined) subtrees.get(Left).get ! Contains(r, id, el)
      else if (el > elem && subtrees.get(Right).isDefined) subtrees.get(Right).get ! Contains(r, id, el)
      else r ! ContainsResult(id, false)
    }
    case Remove(r, id, el) => {
      if (el == elem) {
        removed = true
        r ! OperationFinished(id)
      } else if (el < elem) {
        if (subtrees.get(Left).isDefined) subtrees.get(Left).get ! Remove(r, id, el)
        else r ! OperationFinished(id)
      } else {
        if (subtrees.get(Right).isDefined) subtrees.get(Right).get ! Remove(r, id, el)
        else r ! OperationFinished(id)
      }
    }
    case CopyTo(newNode) => {
      var expected = Set[ActorRef]()
      subtrees.foreach(expected += _._2)
      if (removed) {
        if (expected.isEmpty) context.parent ! CopyFinished
        else context.become(copying(expected, true))
      } else {
        context.become(copying(expected, false))
        newNode ! Insert(self, 0, elem)
      }
      expected.foreach(_ ! CopyTo(newNode))
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case CopyFinished => {
      val newExpected = expected - sender
      if (newExpected.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
//        context.stop(self)
      }
      else context.become(copying(newExpected, insertConfirmed))
    }
    case OperationFinished(_) => {
      if (expected.isEmpty) {
        context.parent ! CopyFinished
//        context.stop(self)
      }
      else context.become(copying(expected, true))
    }
  }


}
