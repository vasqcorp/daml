package com.daml.platform.store.appendonlydao.events

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.codahale.metrics.{Counter, Timer}
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent, Transaction => FlatTx}
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.participant.state.v1.{Offset, TransactionId}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.{InstrumentedSource, Metrics, Timed}
import com.daml.platform.ApiOffset
import com.daml.platform.api.v1.event.EventOps.TreeEventOps
import com.daml.platform.participant.util.LfEngineToApi
import com.daml.platform.store.appendonlydao.events
import com.daml.platform.store.cache.EventsBuffer
import com.daml.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId
import com.daml.platform.store.dao.LedgerDaoTransactionsReader
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.google.protobuf.timestamp.Timestamp

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

// TDT Handle verbose lf decode
class BufferedTransactionsReader(
    protected val delegate: LedgerDaoTransactionsReader,
    val lfValueTranslation: LfValueTranslation,
    val transactionsBuffer: EventsBuffer[Offset, TransactionLogUpdate],
    metrics: Metrics,
)(implicit executionContext: ExecutionContext)
    extends LedgerDaoTransactionsReader
    with DelegateTransactionsReader {
  override type LfValueTranslation = events.LfValueTranslation

  private val outputStreamBufferSize = 128

  override def getFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FilterRelation,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] =
    getTransactions(startExclusive, endInclusive, filter, verbose)(
      toApiTx = toFlatTx,
      toApiResponse = (tx: FlatTx) => GetTransactionsResponse(Seq(tx)),
      fetchTransactions = delegate.getFlatTransactions(_, _, _, _)(loggingContext),
      sourceTimer = metrics.daml.index.getFlatTransactionsSource,
      resolvedFromBufferCounter = metrics.daml.index.flatTransactionEventsResolvedFromBuffer,
      totalRetrievedCounter = metrics.daml.index.totalFlatTransactionsRetrieved,
      bufferSizeCounter =
        metrics.daml.index.flatTransactionsBufferSize, // TODO buffer here is ambiguous
    )

  override def getTransactionTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Set[Party],
      verbose: Boolean,
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] =
    getTransactions(startExclusive, endInclusive, requestingParties, verbose)(
      toApiTx = toTxTree,
      toApiResponse = (tx: TransactionTree) => GetTransactionTreesResponse(Seq(tx)),
      fetchTransactions = delegate.getTransactionTrees(_, _, _, _)(loggingContext),
      sourceTimer = metrics.daml.index.getTransactionTreesSource,
      resolvedFromBufferCounter = metrics.daml.index.transactionTreeEventsResolvedFromBuffer,
      totalRetrievedCounter = metrics.daml.index.totalTransactionTreesRetrieved,
      bufferSizeCounter =
        metrics.daml.index.transactionTreesBufferSize, // TODO buffer here is ambiguous
    )

  private def getTransactions[FILTER, API_TX, API_RESPONSE](
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FILTER,
      verbose: Boolean,
  )(
      toApiTx: (TransactionLogUpdate.Transaction, FILTER, Boolean) => Option[API_TX],
      toApiResponse: API_TX => API_RESPONSE,
      fetchTransactions: (
          Offset,
          Offset,
          FILTER,
          Boolean,
      ) => Source[(Offset, API_RESPONSE), NotUsed],
      sourceTimer: Timer,
      resolvedFromBufferCounter: Counter,
      totalRetrievedCounter: Counter,
      bufferSizeCounter: Counter,
  ): Source[(Offset, API_RESPONSE), NotUsed] = {
    val transactionsSource = Timed.source(
      sourceTimer, {
        val buffered =
          transactionsBuffer
            .slice(startExclusive, endInclusive)
            .collect { case (offset, tx: TransactionLogUpdate.Transaction) =>
              offset -> toApiTx(tx, filter, verbose)
            }
            .collect { case (offset, Some(tx)) =>
              resolvedFromBufferCounter.inc()
              offset -> toApiResponse(tx)
            }

        // TODO: Remove the @unchecked once migrated to Scala 2.13.5 where this false positive exhaustivity check is fixed
        (buffered: @unchecked) match {
          case Vector() | Vector(_) =>
            fetchTransactions(startExclusive, endInclusive, filter, verbose)

          // TODO use Offset.prededcessor
          case hd +: tl =>
            fetchTransactions(startExclusive, hd._1, filter, verbose)
              .concat(Source.fromIterator(() => tl.iterator))
              .map(tx => {
                totalRetrievedCounter.inc()
                tx
              })
        }
      },
    )

    InstrumentedSource.bufferedSource(
      original = transactionsSource,
      counter = bufferSizeCounter,
      size = outputStreamBufferSize,
    )
  }

  // TDT return only witnesses from within the requestors
  private def flatTxPredicate(
      event: TransactionLogUpdate.Event,
      filter: FilterRelation,
  ): Boolean =
    if (filter.size == 1) {
      val (party, templateIds) = filter.iterator.next()
      if (templateIds.isEmpty)
        event.flatEventWitnesses.contains(party)
      else
        // Single-party request, restricted to a set of template identifiers
        event.flatEventWitnesses.contains(party) && templateIds.contains(event.templateId)
    } else {
      // Multi-party requests
      // If no party requests specific template identifiers
      val parties = filter.keySet
      if (filter.forall(_._2.isEmpty))
        event.flatEventWitnesses.intersect(parties.map(_.toString)).nonEmpty
      else {
        // If all parties request the same template identifier
        val templateIds = filter.valuesIterator.flatten.toSet
        if (filter.valuesIterator.forall(_ == templateIds)) {
          event.flatEventWitnesses.intersect(parties.map(_.toString)).nonEmpty &&
          templateIds.contains(event.templateId)
        } else {
          // If there are different template identifier but there are no wildcard parties
          val partiesAndTemplateIds = Relation.flatten(filter).toSet
          val wildcardParties = filter.filter(_._2.isEmpty).keySet
          if (wildcardParties.isEmpty) {
            partiesAndTemplateIds.exists { case (party, identifier) =>
              event.flatEventWitnesses.contains(party) && identifier == event.templateId
            }
          } else {
            // If there are wildcard parties and different template identifiers
            partiesAndTemplateIds.exists { case (party, identifier) =>
              event.flatEventWitnesses.contains(party) && identifier == event.templateId
            } || event.flatEventWitnesses.intersect(wildcardParties.map(_.toString)).nonEmpty
          }
        }
      }
    }

  private def permanent(events: Seq[TransactionLogUpdate.Event]): Set[ContractId] =
    events.foldLeft(Set.empty[ContractId]) {
      case (contractIds, event: TransactionLogUpdate.CreatedEvent) =>
        contractIds + event.contractId
      case (contractIds, event) if !contractIds.contains(event.contractId) =>
        contractIds + event.contractId
      case (contractIds, event) => contractIds - event.contractId
    }

  private def toFlatEvent(
      event: TransactionLogUpdate.Event,
      verbose: Boolean,
  )(implicit
      loggingContext: LoggingContext
  ): Option[com.daml.ledger.api.v1.event.Event] =
    event match {
      case createdEvent: TransactionLogUpdate.CreatedEvent =>
        Some(
          com.daml.ledger.api.v1.event.Event(
            event = com.daml.ledger.api.v1.event.Event.Event.Created(
              value = com.daml.ledger.api.v1.event.CreatedEvent(
                eventId = createdEvent.eventId.toLedgerString,
                contractId = createdEvent.contractId.coid,
                templateId = Some(LfEngineToApi.toApiIdentifier(createdEvent.templateId)),
                contractKey = createdEvent.contractKey
                  .map(
                    lfValueTranslation.toApiValue(
                      _,
                      verbose,
                      "create key",
                      value =>
                        lfValueTranslation.enricher
                          .enrichContractKey(createdEvent.templateId, value.value),
                    )
                  )
                  .map(Await.result(_, 1.second)),
                createArguments = Some(
                  Await.result(
                    lfValueTranslation.toApiRecord(
                      createdEvent.createArgument,
                      verbose,
                      "create argument",
                      value =>
                        lfValueTranslation.enricher
                          .enrichContract(createdEvent.templateId, value.value),
                    ),
                    1.second,
                  )
                ),
                witnessParties = createdEvent.treeEventWitnesses.toSeq,
                signatories = createdEvent.createSignatories.toSeq,
                observers = createdEvent.createObservers.toSeq,
                agreementText = createdEvent.createAgreementText.orElse(Some("")),
              )
            )
          )
        )
      case exercisedEvent: TransactionLogUpdate.ExercisedEvent if exercisedEvent.consuming =>
        Some(
          com.daml.ledger.api.v1.event.Event(
            event = com.daml.ledger.api.v1.event.Event.Event.Archived(
              value = com.daml.ledger.api.v1.event.ArchivedEvent(
                eventId = exercisedEvent.eventId.toLedgerString,
                contractId = exercisedEvent.contractId.coid,
                templateId = Some(LfEngineToApi.toApiIdentifier(exercisedEvent.templateId)),
                witnessParties = exercisedEvent.flatEventWitnesses.toSeq,
              )
            )
          )
        )
      case _ => None
    }

  private def toFlatTx(
      tx: TransactionLogUpdate.Transaction,
      filter: FilterRelation,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Option[FlatTx] = {
    val aux = tx.events
      .filter(flatTxPredicate(_, filter))
    val nonTransientIds = permanent(aux)
    val events = aux
      .filter(ev => nonTransientIds(ev.contractId))

    events.headOption.flatMap { first =>
      if (first.commandId.nonEmpty || events.nonEmpty) {
        val flatEvents = events.map(toFlatEvent(_, verbose)).collect { case Some(ev) =>
          ev
        }
        if (flatEvents.isEmpty)
          None
        else
          Some(
            FlatTx(
              transactionId = first.transactionId,
              commandId = first.commandId,
              workflowId = first.workflowId,
              effectiveAt = Some(instantToTimestamp(first.ledgerEffectiveTime)),
              events = flatEvents,
              offset = ApiOffset.toApiString(tx.offset),
              traceContext = None,
            )
          )
      } else None
    }
  }

  private def toTxTree(
      tx: TransactionLogUpdate.Transaction,
      requestingParties: Set[Party],
      verbose: Boolean,
  )(implicit
      loggingContext: LoggingContext
  ): Option[TransactionTree] = {
    val treeEvents = tx.events
      .collect {
        // TDT handle multi-party submissions
        case createdEvent: TransactionLogUpdate.CreatedEvent
            if createdEvent.treeEventWitnesses
              .intersect(requestingParties.asInstanceOf[Set[String]])
              .nonEmpty =>
          TreeEvent(
            TreeEvent.Kind.Created(
              com.daml.ledger.api.v1.event.CreatedEvent(
                eventId = createdEvent.eventId.toLedgerString,
                contractId = createdEvent.contractId.coid,
                templateId = Some(LfEngineToApi.toApiIdentifier(createdEvent.templateId)),
                contractKey = createdEvent.contractKey
                  .map(
                    lfValueTranslation.toApiValue(
                      _,
                      verbose,
                      "create key",
                      value =>
                        lfValueTranslation.enricher
                          .enrichContractKey(createdEvent.templateId, value.value),
                    )
                  )
                  .map(Await.result(_, 1.second)),
                createArguments = Some(
                  Await.result(
                    lfValueTranslation.toApiRecord(
                      createdEvent.createArgument,
                      verbose,
                      "create argument",
                      value =>
                        lfValueTranslation.enricher
                          .enrichContract(createdEvent.templateId, value.value),
                    ),
                    1.second,
                  )
                ),
                witnessParties = createdEvent.treeEventWitnesses.toSeq,
                signatories = createdEvent.createSignatories.toSeq,
                observers = createdEvent.createObservers.toSeq,
                agreementText = createdEvent.createAgreementText.orElse(Some("")),
              )
            )
          )
        case exercisedEvent: TransactionLogUpdate.ExercisedEvent
            if exercisedEvent.treeEventWitnesses
              .intersect(requestingParties.asInstanceOf[Set[String]])
              .nonEmpty =>
          TreeEvent(
            TreeEvent.Kind.Exercised(
              com.daml.ledger.api.v1.event.ExercisedEvent(
                eventId = exercisedEvent.eventId.toLedgerString,
                contractId = exercisedEvent.contractId.coid,
                templateId = Some(LfEngineToApi.toApiIdentifier(exercisedEvent.templateId)),
                choice = exercisedEvent.choice,
                choiceArgument = Some(
                  Await.result(
                    lfValueTranslation.toApiValue(
                      exercisedEvent.exerciseArgument,
                      verbose,
                      "exercise argument",
                      value =>
                        lfValueTranslation.enricher
                          .enrichChoiceArgument(
                            exercisedEvent.templateId,
                            Ref.Name.assertFromString(exercisedEvent.choice),
                            value.value,
                          ),
                    ),
                    1.second,
                  )
                ),
                actingParties = exercisedEvent.actingParties.toSeq,
                consuming = exercisedEvent.consuming,
                witnessParties = exercisedEvent.treeEventWitnesses.toSeq,
                childEventIds = exercisedEvent.children,
                exerciseResult = exercisedEvent.exerciseResult
                  .map(
                    lfValueTranslation.toApiValue(
                      _,
                      verbose,
                      "exercise result",
                      value =>
                        lfValueTranslation.enricher.enrichChoiceResult(
                          exercisedEvent.templateId,
                          Ref.Name.assertFromString(exercisedEvent.choice),
                          value.value,
                        ),
                    )
                  )
                  .map(Await.result(_, 1.second)),
              )
            )
          )
      }

    if (treeEvents.isEmpty)
      Option.empty
    else {

      val visible = treeEvents.map(_.eventId)
      val visibleSet = visible.toSet
      val eventsById = treeEvents.iterator
        .map(e => e.eventId -> e.filterChildEventIds(visibleSet))
        .toMap

      // All event identifiers that appear as a child of another item in this response
      val children = eventsById.valuesIterator.flatMap(_.childEventIds).toSet

      // The roots for this request are all visible items
      // that are not a child of some other visible item
      val rootEventIds = visible.filterNot(children)

      Some(
        TransactionTree(
          transactionId = tx.transactionId,
          commandId = tx.commandId, // TDT use submitters predicate to set commandId
          workflowId = tx.workflowId,
          effectiveAt = Some(instantToTimestamp(tx.effectiveAt)),
          offset = ApiOffset.toApiString(tx.offset),
          eventsById = eventsById,
          rootEventIds = rootEventIds,
          traceContext = None,
        )
      )
    }
  }

  private def instantToTimestamp(t: Instant): Timestamp =
    Timestamp(seconds = t.getEpochSecond, nanos = t.getNano)
}

trait DelegateTransactionsReader extends LedgerDaoTransactionsReader {
  protected def delegate: LedgerDaoTransactionsReader

  override def lookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] =
    delegate.lookupFlatTransactionById(transactionId, requestingParties)

  override def lookupTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] =
    delegate.lookupTransactionTreeById(transactionId, requestingParties)

  override def getActiveContracts(activeAt: Offset, filter: FilterRelation, verbose: Boolean)(
      implicit loggingContext: LoggingContext
  ): Source[GetActiveContractsResponse, NotUsed] =
    delegate.getActiveContracts(activeAt, filter, verbose)

  override def getContractStateEvents(startExclusive: (Offset, Long), endInclusive: (Offset, Long))(
      implicit loggingContext: LoggingContext
  ): Source[((Offset, Long), ContractStateEvent), NotUsed] =
    delegate.getContractStateEvents(startExclusive, endInclusive)

  override def getTransactionLogUpdates(
      startExclusive: (Offset, EventSequentialId),
      endInclusive: (Offset, EventSequentialId),
  )(implicit
      loggingContext: LoggingContext
  ): Source[((Offset, EventSequentialId), TransactionLogUpdate), NotUsed] =
    delegate.getTransactionLogUpdates(startExclusive, endInclusive)
}
