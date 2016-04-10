package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  case class OperationTimeout(id: Long)

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  override val supervisorStrategy = OneForOneStrategy() {
    case _ => Restart
  }

  arbiter ! Join

  val persistActorRef = context.actorOf(persistenceProps)

  var pAcks = Map.empty[Long, Persist]

  context.system.scheduler.schedule(0.second, 100.millisecond, new Runnable {
    override def run(): Unit = {
      pAcks.foreach (persistActorRef ! _._2)
    }
  })

  var p = Set.empty[Long]
  var r = Map.empty[Long, Set[ActorRef]]
  var s = Map.empty[Long, Cancellable]
  var c = Map.empty[Long, ActorRef]

  var next_expected_seq = 0L

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) => {
      s = s.updated(id, context.system.scheduler.scheduleOnce(1.second, self, OperationTimeout(id)))
      c = c.updated(id, sender())
      kv = kv.updated(key, value)
      persistActorRef ! Persist(key, Some(value), id)
      pAcks = pAcks.updated(id, Persist(key, Some(value), id))
      replicators.foreach (_ ! Replicate(key, Some(value), id))
    }
    case Remove(key, id) => {
      s = s.updated(id, context.system.scheduler.scheduleOnce(1.second, self, OperationTimeout(id)))
      c = c.updated(id, sender())
      kv = kv - key
      persistActorRef ! Persist(key, None, id)
      pAcks = pAcks.updated(id, Persist(key, None, id))
      replicators.foreach (_ ! Replicate(key, None, id))
    }
    case OperationTimeout(id) => {
      c.get(id).foreach (_ ! OperationFailed(id))
      p -= id
      r -= id
      c -= id
    }
    case Persisted(key, id) => {
      p += id
      pAcks -= id
      checkAndReply(id)
    }
    case Replicated(key, id) => {
      val set = r.getOrElse(id, Set.empty[ActorRef]) + sender()
      r = r.updated(id, set)
      checkAndReply(id)
    }
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case Replicas(replicas) => {
      (replicas -- secondaries.keySet - self).foreach { newReplica =>
        val newReplicator = context.actorOf(Replicator.props(newReplica))
        secondaries = secondaries.updated(newReplica, newReplicator)
        replicators += newReplicator
        kv.foreach { case (k, v) => newReplicator ! Replicate(k, Some(v), -1) }
      }

      (secondaries.keySet -- replicas - self).foreach { oldReplica =>
        secondaries.get(oldReplica).foreach { oldReplicator =>
          replicators -= oldReplicator
          oldReplicator ! PoisonPill
        }
        secondaries -= oldReplica
        oldReplica ! PoisonPill
      }

      p.foreach (checkAndReply _)
    }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case Snapshot(key, valueOpt, seq) => {
      if (seq < next_expected_seq) sender ! SnapshotAck(key, seq)
      else if (seq == next_expected_seq) {
        if (valueOpt.isDefined) kv = kv.updated(key, valueOpt.get)
        else kv -= key
        c = c.updated(seq, sender())
        persistActorRef ! Persist(key, valueOpt, seq)
        pAcks = pAcks.updated(seq, Persist(key, valueOpt, seq))
      }
    }
    case Persisted(key, seq) => {
      c.get(seq).foreach (_ ! SnapshotAck(key, seq))
      pAcks -= seq
      //c -= seq
      next_expected_seq = Math.max(next_expected_seq, seq+1)
    }
  }

  private def checkAndReply(id: Long): Unit = {
    if ((replicators.isEmpty ||r.get(id).exists(refs => (replicators--refs).isEmpty)) && p.contains(id)) {
      c.get(id).foreach (_ ! OperationAck(id))
      s.get(id).foreach (_.cancel())
      s -= id
      p -= id
      r -= id
      c -= id
    }
  }

}

