// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.http.util.Logging.{InstanceUUID, RequestID}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api
import com.daml.ledger.api.v1.package_service
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest,
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.google.protobuf
import scalaz.OneAnd

import scala.concurrent.{ExecutionContext, Future}
import com.daml.ledger.api.{domain => LedgerApiDomain}

object LedgerClientJwt {

  private[this] val logger = ContextualizedLogger.get(getClass)

  type SubmitAndWaitForTransaction =
    (Jwt, SubmitAndWaitRequest) => Future[SubmitAndWaitForTransactionResponse]

  type SubmitAndWaitForTransactionTree =
    (Jwt, SubmitAndWaitRequest) => Future[SubmitAndWaitForTransactionTreeResponse]

  type GetTermination =
    (Jwt, LedgerApiDomain.LedgerId) => Future[Option[Terminates.AtAbsolute]]

  type GetActiveContracts =
    (
        Jwt,
        LedgerApiDomain.LedgerId,
        TransactionFilter,
        Boolean,
    ) => Source[GetActiveContractsResponse, NotUsed]

  type GetCreatesAndArchivesSince =
    (
        Jwt,
        LedgerApiDomain.LedgerId,
        TransactionFilter,
        LedgerOffset,
        Terminates,
    ) => Source[Transaction, NotUsed]

  type ListKnownParties =
    Jwt => Future[List[api.domain.PartyDetails]]

  type GetParties =
    (Jwt, OneAnd[Set, Ref.Party]) => Future[List[api.domain.PartyDetails]]

  type AllocateParty =
    (Jwt, Option[Ref.Party], Option[String]) => Future[api.domain.PartyDetails]

  type ListPackages =
    (Jwt, LedgerApiDomain.LedgerId) => LoggingContextOf[InstanceUUID with RequestID] => Future[
      package_service.ListPackagesResponse
    ]

  type GetPackage =
    (
        Jwt,
        LedgerApiDomain.LedgerId,
        String,
    ) => LoggingContextOf[InstanceUUID with RequestID] => Future[
      package_service.GetPackageResponse
    ]

  type UploadDarFile =
    (
        Jwt,
        LedgerApiDomain.LedgerId,
        protobuf.ByteString,
    ) => LoggingContextOf[InstanceUUID with RequestID] => Future[Unit]

  private def bearer(jwt: Jwt): Some[String] = Some(jwt.value: String)

  def submitAndWaitForTransaction(client: DamlLedgerClient): SubmitAndWaitForTransaction =
    (jwt, req) => client.commandServiceClient.submitAndWaitForTransaction(req, bearer(jwt))

  def submitAndWaitForTransactionTree(client: DamlLedgerClient): SubmitAndWaitForTransactionTree =
    (jwt, req) => client.commandServiceClient.submitAndWaitForTransactionTree(req, bearer(jwt))

  def getTermination(client: DamlLedgerClient)(implicit ec: ExecutionContext): GetTermination =
    (jwt, ledgerId) =>
      client.transactionClient.getLedgerEnd(ledgerId, bearer(jwt)).map {
        _.offset flatMap {
          _.value match {
            case off @ LedgerOffset.Value.Absolute(_) => Some(Terminates.AtAbsolute(off))
            case LedgerOffset.Value.Boundary(_) | LedgerOffset.Value.Empty => None // at beginning
          }
        }
      }

  def getActiveContracts(client: DamlLedgerClient): GetActiveContracts =
    (jwt, ledgerId, filter, verbose) =>
      client.activeContractSetClient
        .getActiveContracts(filter, ledgerId, verbose, bearer(jwt))
        .mapMaterializedValue(_ => NotUsed)

  sealed abstract class Terminates extends Product with Serializable {
    import Terminates._
    def toOffset: Option[LedgerOffset] = this match {
      case AtLedgerEnd => Some(ledgerEndOffset)
      case Never => None
      case AtAbsolute(off) => Some(LedgerOffset(off))
    }
  }
  object Terminates {
    case object AtLedgerEnd extends Terminates
    case object Never extends Terminates
    final case class AtAbsolute(off: LedgerOffset.Value.Absolute) extends Terminates {
      def toDomain: domain.Offset = domain.Offset(off.value)
    }
    def fromDomain(o: domain.Offset): AtAbsolute =
      AtAbsolute(
        LedgerOffset.Value.Absolute(domain.Offset unwrap o)
      )
  }

  private val ledgerEndOffset =
    LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))

  def getCreatesAndArchivesSince(client: DamlLedgerClient): GetCreatesAndArchivesSince =
    (jwt, ledgerId, filter, offset, terminates) => {
      val end = terminates.toOffset
      if (skipRequest(offset, end))
        Source.empty[Transaction]
      else
        client.transactionClient
          .getTransactions(
            offset,
            terminates.toOffset,
            filter,
            ledgerId,
            verbose = true,
            token = bearer(jwt),
          )
    }

  private def skipRequest(start: LedgerOffset, end: Option[LedgerOffset]): Boolean = {
    import com.daml.http.util.LedgerOffsetUtil.AbsoluteOffsetOrdering
    (start.value, end.map(_.value)) match {
      case (s: LedgerOffset.Value.Absolute, Some(e: LedgerOffset.Value.Absolute)) =>
        AbsoluteOffsetOrdering.gteq(s, e)
      case _ => false
    }
  }

  def listKnownParties(client: DamlLedgerClient): ListKnownParties =
    jwt => client.partyManagementClient.listKnownParties(bearer(jwt))

  def getParties(client: DamlLedgerClient): GetParties =
    (jwt, partyIds) => client.partyManagementClient.getParties(partyIds, bearer(jwt))

  def allocateParty(client: DamlLedgerClient): AllocateParty =
    (jwt, identifierHint, displayName) =>
      client.partyManagementClient.allocateParty(
        hint = identifierHint,
        displayName = displayName,
        token = bearer(jwt),
      )

  def listPackages(client: DamlLedgerClient): ListPackages =
    (jwt, ledgerId) =>
      implicit lc => {
        logger.trace("sending list packages request to ledger")
        client.packageClient.listPackages(ledgerId, bearer(jwt))
      }

  def getPackage(client: DamlLedgerClient): GetPackage =
    (jwt, ledgerId, packageId) =>
      implicit lc => {
        logger.trace("sending get packages request to ledger")
        client.packageClient.getPackage(packageId, ledgerId, token = bearer(jwt))
      }

  def uploadDar(client: DamlLedgerClient): UploadDarFile =
    (jwt, _, byteString) =>
      implicit lc => {
        logger.trace("sending upload dar request to ledger")
        client.packageManagementClient.uploadDarFile(darFile = byteString, token = bearer(jwt))
      }
}
