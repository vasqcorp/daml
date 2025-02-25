// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.ledger.FailedAuthorization
import com.daml.lf.transaction.Node.GenNode
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

import scala.annotation.tailrec
import scala.collection.immutable.HashMap

final case class VersionedTransaction private[lf] (
    version: TransactionVersion,
    nodes: Map[NodeId, GenNode],
    override val roots: ImmArray[NodeId],
) extends HasTxNodes
    with value.CidContainer[VersionedTransaction]
    with NoCopy {

  override protected def self: this.type = this

  override def mapCid(f: ContractId => ContractId): VersionedTransaction =
    VersionedTransaction(
      version,
      nodes = nodes.map { case (nodeId, node) => nodeId -> node.mapCid(f) },
      roots,
    )

  def mapNodeId(f: NodeId => NodeId): VersionedTransaction =
    VersionedTransaction(
      version,
      nodes.map { case (nodeId, node) => f(nodeId) -> node.mapNodeId(f) },
      roots.map(f),
    )

  // O(1)
  def transaction: GenTransaction =
    GenTransaction(nodes, roots)

}

/** General transaction type
  *
  * Abstracts over NodeId type and ContractId type
  * ContractId restricts the occurrence of contractIds
  *
  * @param nodes The nodes of this transaction.
  * @param roots References to the root nodes of the transaction.
  * Users of this class may assume that all instances are well-formed, i.e., `isWellFormed.isEmpty`.
  * For performance reasons, users are not required to call `isWellFormed`.
  * Therefore, it is '''forbidden''' to create ill-formed instances, i.e., instances with `!isWellFormed.isEmpty`.
  */
final case class GenTransaction(
    nodes: Map[NodeId, Node.GenNode],
    roots: ImmArray[NodeId],
) extends HasTxNodes
    with value.CidContainer[GenTransaction] {

  import GenTransaction._

  override protected def self: this.type = this
  override def mapCid(f: ContractId => ContractId): GenTransaction =
    copy(nodes = nodes.map { case (nodeId, node) => nodeId -> node.mapCid(f) })
  def mapNodeId(f: NodeId => NodeId): GenTransaction =
    copy(
      nodes = nodes.map { case (nodeId, node) => f(nodeId) -> node.mapNodeId(f) },
      roots = roots.map(f),
    )

  /** This function checks the following properties:
    *
    * * No dangling references -- all node ids mentioned in the forest are in the nodes map;
    * * No orphaned references -- all keys of the node map are mentioned in the forest;
    * * No aliasing -- every node id in the node map is mentioned exactly once, in the roots list or as a child of
    *   another node.
    */
  def isWellFormed: Set[NotWellFormedError] = {
    // note that we cannot implement this with fold because fold itself crashes on bad
    // transactions
    @tailrec
    def go(
        errors: Set[NotWellFormedError],
        visited: Set[NodeId],
        toVisit: FrontStack[NodeId],
    ): (Set[NotWellFormedError], Set[NodeId]) =
      toVisit match {
        case FrontStack() => (errors, visited)
        case FrontStackCons(nid, nids) =>
          val alreadyVisited = visited.contains(nid)
          val newVisited = visited + nid
          val newErrors = if (alreadyVisited) {
            errors + NotWellFormedError(nid, AliasedNode)
          } else {
            errors
          }
          nodes.get(nid) match {
            case None =>
              go(newErrors + NotWellFormedError(nid, DanglingNodeId), newVisited, nids)
            case Some(node) =>
              node match {
                case nr: Node.NodeRollback =>
                  go(
                    newErrors,
                    newVisited,
                    if (alreadyVisited) {
                      nids
                    } else {
                      nr.children ++: nids
                    },
                  )
                case _: Node.LeafOnlyActionNode => go(newErrors, newVisited, nids)
                case ne: Node.NodeExercises =>
                  go(
                    newErrors,
                    newVisited,
                    if (alreadyVisited) {
                      nids
                    } else {
                      ne.children ++: nids
                    },
                  )
              }
          }
      }
    val (errors, visited) = go(Set.empty, Set.empty, FrontStack.from(roots))
    val orphaned = nodes.keys.toSet.diff(visited).map(nid => NotWellFormedError(nid, OrphanedNode))
    errors ++ orphaned
  }

  /** Compares two Transactions up to renaming of Nids. You most likely want to use this rather than ==, since the
    * Nid is irrelevant to the content of the transaction.
    */
  def equalForest(other: GenTransaction): Boolean =
    compareForest(other)(_ == _)

  /** Compares two Transactions up to renaming of Nids. with the specified comparision of nodes
    * Nid is irrelevant to the content of the transaction.
    */
  def compareForest(other: GenTransaction)(
      compare: (Node.GenNode, Node.GenNode) => Boolean
  ): Boolean = {
    @tailrec
    def go(toCompare: FrontStack[(NodeId, NodeId)]): Boolean =
      toCompare match {
        case FrontStack() => true
        case FrontStackCons((nid1, nid2), rest) =>
          val node1 = nodes(nid1)
          val node2 = other.nodes(nid2)
          node1 match {
            case nr1: Node.NodeRollback => //TODO: can this be NodeRollback ?
              node2 match {
                case nr2: Node.NodeRollback => //TODO: and here
                  val blankedNr1: Node.NodeRollback =
                    nr1.copy(children = ImmArray.Empty)
                  val blankedNr2: Node.NodeRollback =
                    nr2.copy(children = ImmArray.Empty)
                  compare(blankedNr1, blankedNr2) &&
                  nr1.children.length == nr2.children.length &&
                  go(nr1.children.zip(nr2.children) ++: rest)
                case _ => false
              }
            case nf1: Node.NodeFetch =>
              node2 match {
                case nf2: Node.NodeFetch => compare(nf1, nf2) && go(rest)
                case _ => false
              }
            case nc1: Node.NodeCreate =>
              node2 match {
                case nc2: Node.NodeCreate =>
                  compare(nc1, nc2) && go(rest)
                case _ => false
              }
            case ne1: Node.NodeExercises =>
              node2 match {
                case ne2: Node.NodeExercises =>
                  val blankedNe1: Node.NodeExercises =
                    ne1.copy(children = ImmArray.Empty)
                  val blankedNe2: Node.NodeExercises =
                    ne2.copy(children = ImmArray.Empty)
                  compare(blankedNe1, blankedNe2) &&
                  ne1.children.length == ne2.children.length &&
                  go(ne1.children.zip(ne2.children) ++: rest)
                case _ => false
              }
            case nl1: Node.NodeLookupByKey =>
              node2 match {
                case nl2: Node.NodeLookupByKey =>
                  compare(nl1, nl2) && go(rest)
                case _ => false
              }
          }
      }

    if (roots.length != other.roots.length)
      false
    else
      go(roots.zip(other.roots).toFrontStack)

  }

  /** checks that all the values contained are serializable */
  def serializable(f: Value => ImmArray[String]): ImmArray[String] = {
    fold(BackStack.empty[String]) { case (errs, (_, node)) =>
      node match {
        case Node.NodeRollback(_) =>
          errs
        case _: Node.NodeFetch => errs
        case nc: Node.NodeCreate =>
          errs :++ f(nc.arg) :++ (nc.key match {
            case None => ImmArray.Empty
            case Some(key) => f(key.key)
          })
        case ne: Node.NodeExercises => errs :++ f(ne.chosenValue)
        case nlbk: Node.NodeLookupByKey => errs :++ f(nlbk.key.key)
      }
    }.toImmArray
  }

  /** Visit every `Val`. */
  def foldValues[Z](z: Z)(f: (Z, Value) => Z): Z =
    fold(z) { case (z, (_, n)) =>
      n match {
        case Node.NodeRollback(_) =>
          z
        case c: Node.NodeCreate =>
          val z1 = f(z, c.arg)
          val z2 = c.key match {
            case None => z1
            case Some(k) => f(z1, k.key)
          }
          z2
        case nf: Node.NodeFetch => nf.key.fold(z)(k => f(z, k.key))
        case e: Node.NodeExercises => f(z, e.chosenValue)
        case lk: Node.NodeLookupByKey => f(z, lk.key.key)
      }
    }

  /*
  private[lf] def foreach2(fNid: Nid => Unit, fCid: ContractI => Unit): Unit =
    GenTransaction.foreach2(fNid, fCid)(this)
   */
}

sealed abstract class HasTxNodes {

  import Transaction.{
    KeyInput,
    KeyActive,
    KeyCreate,
    NegativeKeyLookup,
    KeyInputError,
    DuplicateKeys,
    InconsistentKeys,
  }

  def nodes: Map[NodeId, Node.GenNode]

  def roots: ImmArray[NodeId]

  /** The union of the informees of a all the action nodes. */
  lazy val informees: Set[Ref.Party] =
    nodes.values.foldLeft(Set.empty[Ref.Party]) {
      case (acc, node: Node.GenActionNode) => acc | node.informeesOfNode
      case (acc, _: Node.NodeRollback) => acc
    }

  // We assume that rollback node cannot be a root of a transaction.
  // This is correct for an unprojected transaction. For a project transaction,
  // Canton handles rollback nodes itself so this is assumption still holds
  // within the Engine.
  @throws[IllegalArgumentException]
  def rootNodes: ImmArray[Node.GenActionNode] =
    roots.map(nid =>
      nodes(nid) match {
        case action: Node.GenActionNode =>
          action
        case _: Node.NodeRollback =>
          throw new IllegalArgumentException(
            s"invalid transaction, root refers to a Rollback node $nid"
          )
      }
    )

  /** This function traverses the transaction tree in pre-order traversal (i.e. exercise node are traversed before their children).
    *
    * Takes constant stack space. Crashes if the transaction is not well formed (see `isWellFormed`)
    */
  final def foreach(f: (NodeId, Node.GenNode) => Unit): Unit = {

    @tailrec
    def go(toVisit: FrontStack[NodeId]): Unit = toVisit match {
      case FrontStack() =>
      case FrontStackCons(nodeId, toVisit) =>
        val node = nodes(nodeId)
        f(nodeId, node)
        node match {
          case nr: Node.NodeRollback => go(nr.children ++: toVisit)
          case _: Node.LeafOnlyActionNode => go(toVisit)
          case ne: Node.NodeExercises => go(ne.children ++: toVisit)
        }
    }

    go(roots.toFrontStack)
  }

  /** Traverses the transaction tree in pre-order traversal (i.e. exercise nodes are traversed before their children)
    *
    * Takes constant stack space. Crashes if the transaction is not well formed (see `isWellFormed`)
    */
  final def fold[A](z: A)(f: (A, (NodeId, Node.GenNode)) => A): A = {
    var acc = z
    foreach((nodeId, node) => acc = f(acc, (nodeId, node)))
    acc
  }

  /** A fold over the transaction that maintains global and path-specific state.
    * Takes constant stack space. Returns the global state.
    *
    * Used to for example compute the roots of per-party projections from the
    * transaction.
    */
  final def foldWithPathState[A, B](globalState0: A, pathState0: B)(
      op: (A, B, NodeId, Node.GenNode) => (A, B)
  ): A = {
    var globalState = globalState0

    @tailrec
    def go(toVisit: FrontStack[(NodeId, B)]): Unit = toVisit match {
      case FrontStack() =>
      case FrontStackCons((nodeId, pathState), toVisit) =>
        val node = nodes(nodeId)
        val (globalState1, newPathState) = op(globalState, pathState, nodeId, node)
        globalState = globalState1
        node match {
          case nr: Node.NodeRollback =>
            go(nr.children.map(_ -> newPathState) ++: toVisit)
          case _: Node.LeafOnlyActionNode => go(toVisit)
          case ne: Node.NodeExercises =>
            go(ne.children.map(_ -> newPathState) ++: toVisit)
        }
    }

    go(roots.map(_ -> pathState0).toFrontStack)
    globalState
  }

  final def localContracts[Cid2 >: ContractId]: Map[Cid2, (NodeId, Node.NodeCreate)] =
    fold(Map.empty[Cid2, (NodeId, Node.NodeCreate)]) {
      case (acc, (nid, create: Node.NodeCreate)) =>
        acc.updated(create.coid, nid -> create)
      case (acc, _) => acc
    }

  /** Returns the IDs of all the consumed contracts.
    *  This includes transient contracts but it does not include contracts
    *  consumed in rollback nodes.
    */
  final def consumedContracts[Cid2 >: ContractId]: Set[Cid2] =
    foldInExecutionOrder(Set.empty[Cid2])(
      exerciseBegin = (acc, _, exe) => {
        if (exe.consuming) { (acc + exe.targetCoid, true) }
        else { (acc, true) }
      },
      rollbackBegin = (acc, _, _) => (acc, false),
      leaf = (acc, _, _) => acc,
      exerciseEnd = (acc, _, _) => acc,
      rollbackEnd = (acc, _, _) => acc,
    )

  /** Local and global contracts that are inactive at the end of the transaction.
    * This includes both contracts that have been arachived and local
    * contracts whose create has been rolled back.
    */
  final def inactiveContracts[Cid2 >: ContractId]: Set[Cid2] = {
    final case class LedgerState(
        createdCids: Set[Cid2],
        inactiveCids: Set[Cid2],
    ) {
      def create(cid: Cid2): LedgerState =
        copy(
          createdCids = createdCids + cid
        )
      def archive(cid: Cid2): LedgerState =
        copy(
          inactiveCids = inactiveCids + cid
        )
    }
    final case class State(
        currentState: LedgerState,
        rollbackStack: List[LedgerState],
    ) {
      def create(cid: Cid2) = copy(
        currentState = currentState.create(cid)
      )
      def archive(cid: Cid2) = copy(
        currentState = currentState.archive(cid)
      )
      def beginRollback() = copy(
        rollbackStack = currentState :: rollbackStack
      )
      def endRollback() = {
        // In addition to archives we also need to mark contracts
        // created in the rollback as inactive
        val beginState = rollbackStack.head
        copy(
          currentState = beginState.copy(
            inactiveCids =
              beginState.inactiveCids union (currentState.createdCids diff beginState.createdCids)
          ),
          rollbackStack = rollbackStack.tail,
        )
      }
    }
    foldInExecutionOrder[State](State(LedgerState(Set.empty, Set.empty), Nil))(
      exerciseBegin = (acc, _, exe) =>
        if (exe.consuming) {
          (acc.archive(exe.targetCoid), true)
        } else {
          (acc, true)
        },
      exerciseEnd = (acc, _, _) => acc,
      rollbackBegin = (acc, _, _) => (acc.beginRollback(), true),
      rollbackEnd = (acc, _, _) => acc.endRollback(),
      leaf = (acc, _, leaf) =>
        leaf match {
          case c: Node.NodeCreate => acc.create(c.coid)
          case _ => acc
        },
    ).currentState.inactiveCids
  }

  /** Returns the IDs of all input contracts that are used by this transaction.
    */
  final def inputContracts[Cid2 >: ContractId]: Set[Cid2] =
    fold(Set.empty[Cid2]) {
      case (acc, (_, Node.NodeExercises(coid, _, _, _, _, _, _, _, _, _, _, _, _, _))) =>
        acc + coid
      case (acc, (_, Node.NodeFetch(coid, _, _, _, _, _, _, _))) =>
        acc + coid
      case (acc, (_, Node.NodeLookupByKey(_, _, Some(coid), _))) =>
        acc + coid
      case (acc, _) => acc
    } -- localContracts.keySet

  /** Return all the contract keys referenced by this transaction.
    * This includes the keys created, exercised, fetched, or looked up, even those
    * that refer transient contracts or that appear under a rollback node.
    */
  final def contractKeys: Set[GlobalKey] = {
    fold(Set.empty[GlobalKey]) {
      case (acc, (_, node: Node.NodeCreate)) =>
        node.key.fold(acc)(key => acc + GlobalKey.assertBuild(node.templateId, key.key))
      case (acc, (_, node: Node.NodeExercises)) =>
        node.key.fold(acc)(key => acc + GlobalKey.assertBuild(node.templateId, key.key))
      case (acc, (_, node: Node.NodeFetch)) =>
        node.key.fold(acc)(key => acc + GlobalKey.assertBuild(node.templateId, key.key))
      case (acc, (_, node: Node.NodeLookupByKey)) =>
        acc + GlobalKey.assertBuild(node.templateId, node.key.key)
      case (acc, (_, _: Node.NodeRollback)) =>
        acc
    }
  }

  /** Return the expected contract key inputs (i.e. the state before the transaction)
    * for this transaction or an error if the transaction contains a
    * duplicate key error or has an inconsistent mapping for a key. For
    * KeyCreate and NegativeKeyLookup (both corresponding to the key not being active)
    * the first required input in execution order wins. So if a create comes first
    * the input will be set to KeyCreate, if a negative lookup by key comes first
    * the input will be set to NegativeKeyLookup.
    *
    * Because we do not preserve byKey flags across transaction serialization
    * this method will consider all operations with keys for conflicts
    * rather than just by-key operations.
    */
  @throws[IllegalArgumentException](
    "If a contract key contains a contract id"
  )
  final def contractKeyInputs: Either[KeyInputError, Map[GlobalKey, KeyInput]] = {
    final case class State(
        keys: Map[GlobalKey, Option[Value.ContractId]],
        rollbackStack: List[Map[GlobalKey, Option[Value.ContractId]]],
        keyInputs: Map[GlobalKey, KeyInput],
    ) {
      def setKeyMapping(
          key: GlobalKey,
          value: KeyInput,
      ): Either[KeyInputError, State] = {
        (keyInputs.get(key), value) match {
          case (None, _) =>
            Right(copy(keyInputs = keyInputs.updated(key, value)))
          case (Some(KeyCreate | NegativeKeyLookup), KeyActive(_)) => Left(InconsistentKeys(key))
          case (Some(KeyActive(_)), NegativeKeyLookup) => Left(InconsistentKeys(key))
          case (Some(KeyActive(_)), KeyCreate) => Left(DuplicateKeys(key))
          case _ => Right(this)
        }
      }
      def assertKeyMapping(
          templateId: Identifier,
          cid: Value.ContractId,
          optKey: Option[Node.KeyWithMaintainers[Value]],
      ): Either[KeyInputError, State] =
        optKey.fold[Either[KeyInputError, State]](Right(this)) { key =>
          val gk = GlobalKey.assertBuild(templateId, key.key)
          keys.get(gk) match {
            case Some(keyMapping) if Some(cid) != keyMapping => Left(InconsistentKeys(gk))
            case _ =>
              val r = copy(keys = keys.updated(gk, Some(cid)))
              if (localContracts.contains(cid)) {
                Right(r)
              } else {
                r.setKeyMapping(gk, KeyActive(cid))
              }
          }
        }
      def handleExercise(exe: Node.NodeExercises) =
        assertKeyMapping(exe.templateId, exe.targetCoid, exe.key).map { state =>
          exe.key.fold(state) { key =>
            val gk = GlobalKey.assertBuild(exe.templateId, key.key)
            if (exe.consuming) {
              state.copy(
                keys = keys.updated(gk, None)
              )
            } else {
              state
            }
          }
        }

      def handleCreate(create: Node.NodeCreate) =
        create.key.fold[Either[KeyInputError, State]](Right(this)) { key =>
          val gk = GlobalKey.assertBuild(create.templateId, key.key)
          val next = copy(keys = keys.updated(gk, Some(create.coid)))
          keys.get(gk) match {
            case None =>
              next.setKeyMapping(gk, KeyCreate)
            case Some(None) =>
              Right(next)
            case Some(Some(_)) => Left(DuplicateKeys(gk))
          }
        }

      def handleLookup(
          lookup: Node.NodeLookupByKey
      ): Either[KeyInputError, State] = {
        val gk = GlobalKey.assertBuild(lookup.templateId, lookup.key.key)
        keys.get(gk) match {
          case None =>
            copy(keys = keys.updated(gk, lookup.result))
              .setKeyMapping(gk, lookup.result.fold[KeyInput](NegativeKeyLookup)(KeyActive(_)))
          case Some(optCid) =>
            if (optCid != lookup.result) {
              Left(InconsistentKeys(gk))
            } else {
              // No need to update anything, we updated keyInputs when we updated keys.
              Right(this)
            }
        }
      }

      def handleLeaf(
          leaf: Node.LeafOnlyActionNode
      ): Either[KeyInputError, State] =
        leaf match {
          case create: Node.NodeCreate =>
            handleCreate(create)
          case fetch: Node.NodeFetch =>
            assertKeyMapping(fetch.templateId, fetch.coid, fetch.key)
          case lookup: Node.NodeLookupByKey =>
            handleLookup(lookup)
        }
      def beginRollback: State =
        copy(
          rollbackStack = keys :: rollbackStack
        )
      def endRollback: State =
        copy(
          keys = rollbackStack.head,
          rollbackStack = rollbackStack.tail,
        )
    }
    foldInExecutionOrder[Either[KeyInputError, State]](
      Right(State(Map.empty, List.empty, Map.empty))
    )(
      exerciseBegin = (acc, _, exe) => (acc.flatMap(_.handleExercise(exe)), true),
      exerciseEnd = (acc, _, _) => acc,
      rollbackBegin = (acc, _, _) => (acc.map(_.beginRollback), true),
      rollbackEnd = (acc, _, _) => acc.map(_.endRollback),
      leaf = (acc, _, leaf) => acc.flatMap(_.handleLeaf(leaf)),
    )
      .map(_.keyInputs)
  }

  /** The contract keys created or updated as part of the transaction.
    *  This includes updates to transient contracts (by mapping them to None)
    *  but it does not include any updates under rollback nodes.
    */
  final def updatedContractKeys: Map[GlobalKey, Option[Value.ContractId]] = {
    foldInExecutionOrder(Map.empty[GlobalKey, Option[Value.ContractId]])(
      exerciseBegin = {
        case (acc, _, exec) if exec.consuming =>
          (
            exec.key.fold(acc)(key =>
              acc.updated(GlobalKey.assertBuild(exec.templateId, key.key), None)
            ),
            true,
          )
        case (acc, _, _) => (acc, true)
      },
      rollbackBegin = (acc, _, _) => (acc, false),
      leaf = {
        case (acc, _, create: Node.NodeCreate) =>
          create.key.fold(acc)(key =>
            acc.updated(GlobalKey.assertBuild(create.templateId, key.key), Some(create.coid))
          )
        case (acc, _, _: Node.NodeFetch | _: Node.NodeLookupByKey) => acc
      },
      exerciseEnd = (acc, _, _) => acc,
      rollbackEnd = (acc, _, _) => acc,
    )
  }

  // This method visits to all nodes of the transaction in execution order.
  // Exercise/rollback nodes are visited twice: when execution reaches them and when execution leaves their body.
  // On the first visit of an execution/rollback node, the caller can prevent traversal of the children
  final def foreachInExecutionOrder(
      exerciseBegin: (NodeId, Node.NodeExercises) => Boolean,
      rollbackBegin: (NodeId, Node.NodeRollback) => Boolean,
      leaf: (NodeId, Node.LeafOnlyActionNode) => Unit,
      exerciseEnd: (NodeId, Node.NodeExercises) => Unit,
      rollbackEnd: (NodeId, Node.NodeRollback) => Unit,
  ): Unit = {
    @tailrec
    def loop(
        currNodes: FrontStack[NodeId],
        stack: FrontStack[
          ((NodeId, Either[Node.NodeRollback, Node.NodeExercises]), FrontStack[NodeId])
        ],
    ): Unit =
      currNodes match {
        case FrontStackCons(nid, rest) =>
          nodes(nid) match {
            case rb: Node.NodeRollback =>
              if (rollbackBegin(nid, rb)) {
                loop(rb.children.toFrontStack, ((nid, Left(rb)), rest) +: stack)
              } else {
                loop(rest, stack)
              }
            case exe: Node.NodeExercises =>
              if (exerciseBegin(nid, exe)) {
                loop(exe.children.toFrontStack, ((nid, Right(exe)), rest) +: stack)
              } else {
                loop(rest, stack)
              }
            case node: Node.LeafOnlyActionNode =>
              leaf(nid, node)
              loop(rest, stack)
          }
        case FrontStack() =>
          stack match {
            case FrontStackCons(((nid, either), brothers), rest) =>
              either match {
                case Left(rb) =>
                  rollbackEnd(nid, rb)
                  loop(brothers, rest)
                case Right(exe) =>
                  exerciseEnd(nid, exe)
                  loop(brothers, rest)
              }
            case FrontStack() =>
          }
      }

    loop(roots.toFrontStack, FrontStack.empty)
  }

  // This method visits to all nodes of the transaction in execution order.
  // Exercise nodes are visited twice: when execution reaches them and when execution leaves their body.
  final def foldInExecutionOrder[A](z: A)(
      exerciseBegin: (A, NodeId, Node.NodeExercises) => (A, Boolean),
      rollbackBegin: (A, NodeId, Node.NodeRollback) => (A, Boolean),
      leaf: (A, NodeId, Node.LeafOnlyActionNode) => A,
      exerciseEnd: (A, NodeId, Node.NodeExercises) => A,
      rollbackEnd: (A, NodeId, Node.NodeRollback) => A,
  ): A = {
    var acc = z
    foreachInExecutionOrder(
      (nid, node) => {
        val (acc2, bool) = exerciseBegin(acc, nid, node)
        acc = acc2
        bool
      },
      (nid, node) => {
        val (acc2, bool) = rollbackBegin(acc, nid, node)
        acc = acc2
        bool
      },
      (nid, node) => acc = leaf(acc, nid, node),
      (nid, node) => acc = exerciseEnd(acc, nid, node),
      (nid, node) => acc = rollbackEnd(acc, nid, node),
    )
    acc
  }

  // This method returns all node-ids reachable from the roots of a transaction.
  final def reachableNodeIds: Set[NodeId] = {
    foldInExecutionOrder[Set[NodeId]](Set.empty)(
      (acc, nid, _) => (acc + nid, true),
      (acc, nid, _) => (acc + nid, true),
      (acc, nid, _) => acc + nid,
      (acc, _, _) => acc,
      (acc, _, _) => acc,
    )
  }

  final def guessSubmitter: Either[String, Party] =
    rootNodes.map(_.requiredAuthorizers) match {
      case ImmArray() =>
        Left(s"Empty transaction")
      case ImmArrayCons(head, _) if head.size != 1 =>
        Left(s"Transaction's roots do not have exactly one authorizer: $this")
      case ImmArrayCons(head, tail) if tail.toSeq.exists(_ != head) =>
        Left(s"Transaction's roots have different authorizers: $this")
      case ImmArrayCons(head, _) =>
        Right(head.head)
    }

}

object GenTransaction {

  type WithTxValue = GenTransaction

  private[this] val Empty = GenTransaction(HashMap.empty, ImmArray.Empty)

  private[lf] def empty: GenTransaction = Empty

  private[lf] case class NotWellFormedError(nid: NodeId, reason: NotWellFormedErrorReason)
  private[lf] sealed trait NotWellFormedErrorReason
  private[lf] case object DanglingNodeId extends NotWellFormedErrorReason
  private[lf] case object OrphanedNode extends NotWellFormedErrorReason
  private[lf] case object AliasedNode extends NotWellFormedErrorReason

  // crashes if transaction's keys contain contract Ids.
  @throws[IllegalArgumentException]
  def duplicatedContractKeys(tx: VersionedTransaction): Set[GlobalKey] = {

    import GlobalKey.{assertBuild => globalKey}

    case class State(active: Set[GlobalKey], duplicates: Set[GlobalKey]) {
      def created(key: GlobalKey): State =
        if (active(key)) copy(duplicates = duplicates + key) else copy(active = active + key)
      def consumed(key: GlobalKey): State =
        copy(active = active - key)
      def referenced(key: GlobalKey): State =
        copy(active = active + key)
    }

    tx.fold(State(Set.empty, Set.empty)) { case (state, (_, node)) =>
      node match {
        case Node.NodeCreate(_, tmplId, _, _, _, _, Some(key), _) =>
          state.created(globalKey(tmplId, key.key))
        case Node.NodeExercises(_, tmplId, _, true, _, _, _, _, _, _, _, Some(key), _, _) =>
          state.consumed(globalKey(tmplId, key.key))
        case Node.NodeExercises(_, tmplId, _, false, _, _, _, _, _, _, _, Some(key), _, _) =>
          state.referenced(globalKey(tmplId, key.key))
        case Node.NodeFetch(_, tmplId, _, _, _, Some(key), _, _) =>
          state.referenced(globalKey(tmplId, key.key))
        case Node.NodeLookupByKey(tmplId, key, Some(_), _) =>
          state.referenced(globalKey(tmplId, key.key))
        case _ =>
          state
      }
    }.duplicates
  }
}

object Transaction {

  type Value = Value.VersionedValue

  type ContractInst = Value.ContractInst[Value]

  /** Transaction nodes */
  type Node = Node.GenNode
  type ActionNode = Node.GenActionNode
  type LeafNode = Node.LeafOnlyActionNode

  /** (Complete) transactions, which are the result of interpreting a
    * ledger-update. These transactions are consumed by either the
    * scenario-interpreter or the Daml-engine code. Both of these
    * code-paths share the computations for segregating the
    * transaction into party-specific ledgers and for computing
    * divulgence of contracts.
    */
  type Transaction = VersionedTransaction
  val Transaction: VersionedTransaction.type = VersionedTransaction

  /** Transaction meta data
    *
    * @param submissionSeed : the submission seed used to derive the contract IDs.
    *                       If undefined no seed has been used (the legacy contract ID scheme
    *                       have been used) or it is unknown (output of partial reinterpretation).
    * @param submissionTime : the submission time
    * @param usedPackages   The set of packages used during command processing.
    *                       This is a hint for what packages are required to validate
    *                       the transaction using the current interpreter.
    *                       If set to `empty` the package dependency have not be computed.
    * @param dependsOnTime  : indicate the transaction computation depends on ledger
    *                       time.
    * @param nodeSeeds      : An association list that maps to each ID of create and exercise
    *                       nodes its seeds.
    */
  final case class Metadata(
      submissionSeed: Option[crypto.Hash],
      submissionTime: Time.Timestamp,
      usedPackages: Set[PackageId],
      dependsOnTime: Boolean,
      nodeSeeds: ImmArray[(NodeId, crypto.Hash)],
  )

  def commitTransaction(submittedTransaction: SubmittedTransaction): CommittedTransaction =
    CommittedTransaction(submittedTransaction)

  def commitTransaction(
      submittedTransaction: SubmittedTransaction,
      f: crypto.Hash => Bytes,
  ): Either[String, CommittedTransaction] =
    submittedTransaction.suffixCid(f).map(CommittedTransaction(_))

  /** Errors that can happen during building transactions. */
  sealed abstract class TransactionError extends Product with Serializable

  /** Signals that the contract-id `coid` was expected to be active, but
    * is not.
    */
  final case class ContractNotActive(
      coid: Value.ContractId,
      templateId: TypeConName,
      consumedBy: NodeId,
  ) extends TransactionError

  /** Signals that within the transaction we got to a point where
    * two contracts with the same key were active.
    *
    * Note that speedy only detects duplicate key collisions
    * if both contracts are used in the transaction in by-key operations
    * meaning lookup, fetch or exercise-by-key or local creates.
    *
    * Two notable cases that will never produce duplicate key errors
    * is a standalone create or a create and a fetch (but not fetch-by-key)
    * with the same key.
    *
    * For ledger implementors this means that (for contract key uniqueness)
    * it is sufficient to only look at the inputs and the outputs of the
    * transaction whlie leaving all internal checks within the transaction
    *  to the engine.
    */
  final case class DuplicateContractKey(
      key: GlobalKey
  ) extends TransactionError

  final case class AuthFailureDuringExecution(
      nid: NodeId,
      fa: FailedAuthorization,
  ) extends TransactionError

  /** The state of a key at the beginning of the transaction.
    */
  sealed trait KeyInput extends Product with Serializable

  /** No active contract with the given key.
    */
  sealed trait KeyInactive extends KeyInput

  /** A contract with the key will be created so the key must be inactive.
    */
  final case object KeyCreate extends KeyInactive

  /** Negative key lookup so the key mus tbe inactive.
    */
  final case object NegativeKeyLookup extends KeyInactive

  /** Key must be mapped to this active contract.
    */
  final case class KeyActive(cid: Value.ContractId) extends KeyInput

  /** contractKeyInputs failed to produce an input due to an error for the given key.
    */
  sealed abstract class KeyInputError {
    def key: GlobalKey
  }

  /** A create failed because there was already an active contract with the same key.
    */
  final case class DuplicateKeys(key: GlobalKey) extends KeyInputError

  /** An exercise, fetch or lookupByKey failed because the mapping of key -> contract id
    * was inconsistent with earlier nodes (in execution order).
    */
  final case class InconsistentKeys(key: GlobalKey) extends KeyInputError
}
