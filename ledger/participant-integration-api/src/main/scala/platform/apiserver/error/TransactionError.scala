// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.error

import com.daml.error.ErrorCode.truncateResourceForTransport
import com.daml.error.{BaseError, ErrorCode}
import com.daml.ledger.participant.state.v2.Update.CommandRejected.{
  FinalReason,
  RejectionReasonTemplate,
}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.google.rpc.status.{Status => RpcStatus}
import io.grpc.{Status, StatusRuntimeException}

trait TransactionError extends BaseError {
  @Deprecated
  def createRejectionDeprecated(
      rewrite: Map[ErrorCode, Status.Code]
  )(implicit
      logger: ContextualizedLogger,
      loggingContext: LoggingContext,
      correlationId: Option[String],
  ): RejectionReasonTemplate = {
    FinalReason(_rpcStatus(rewrite.get(this.code), correlationId))
  }

  def createRejection(
      correlationId: Option[String]
  )(implicit
      logger: ContextualizedLogger,
      loggingContext: LoggingContext,
  ): RejectionReasonTemplate = {
    FinalReason(rpcStatus(correlationId))
  }

  // Determines the value of the `definite_answer` key in the error details
  def definiteAnswer: Boolean = false

  final override def definiteAnswerO: Option[Boolean] = Some(definiteAnswer)

  def rpcStatus(
      correlationId: Option[String]
  )(implicit logger: ContextualizedLogger, loggingContext: LoggingContext): RpcStatus =
    _rpcStatus(None, correlationId)

  def _rpcStatus(
      overrideCode: Option[Status.Code],
      correlationId: Option[String],
  )(implicit logger: ContextualizedLogger, loggingContext: LoggingContext): RpcStatus = {

    // yes, this is a horrible duplication of ErrorCode.asGrpcError. why? because
    // scalapb does not really support grpc rich errors. there is literally no method
    // that supports turning scala com.google.rpc.status.Status into java com.google.rpc.Status
    // objects. however, the sync-api uses the scala variant whereas we have to return StatusRuntimeExceptions.
    // therefore, we have to compose the status code a second time here ...
    // the ideal fix would be to extend scalapb accordingly ...
    val ErrorCode.StatusInfo(codeGrpc, message, contextMap, _) =
      code.getStatusInfo(this, correlationId, logger)(loggingContext)

    val definiteAnswerKey = com.daml.ledger.grpc.GrpcStatuses.DefiniteAnswerKey

    val metadata = if (code.category.securitySensitive) Map.empty[String, String] else contextMap
    val errorInfo = com.google.rpc.error_details.ErrorInfo(
      reason = code.id,
      metadata = metadata.updated(definiteAnswerKey, definiteAnswer.toString),
    )

    val retryInfoO = retryable.map { ri =>
      val dr = com.google.protobuf.duration.Duration(
        java.time.Duration.ofMillis(ri.duration.toMillis)
      )
      com.google.protobuf.any.Any.pack(com.google.rpc.error_details.RetryInfo(Some(dr)))
    }

    val requestInfoO = correlationId.map { ci =>
      com.google.protobuf.any.Any.pack(com.google.rpc.error_details.RequestInfo(requestId = ci))
    }

    val resourceInfos =
      if (code.category.securitySensitive) Seq()
      else
        truncateResourceForTransport(resources).map { case (rs, item) =>
          com.google.protobuf.any.Any
            .pack(
              com.google.rpc.error_details
                .ResourceInfo(resourceType = rs.asString, resourceName = item)
            )
        }

    val details = Seq[com.google.protobuf.any.Any](
      com.google.protobuf.any.Any.pack(errorInfo)
    ) ++ retryInfoO.toList ++ requestInfoO.toList ++ resourceInfos

    com.google.rpc.status.Status(
      overrideCode.getOrElse(codeGrpc).value(),
      message,
      details,
    )
  }
}

abstract class TransactionErrorImpl(
    override val cause: String,
    override val throwableO: Option[Throwable] = None,
    override val definiteAnswer: Boolean = false,
)(implicit override val code: ErrorCode)
    extends TransactionError

abstract class LoggingTransactionErrorImpl(
    cause: String,
    throwableO: Option[Throwable] = None,
    definiteAnswer: Boolean = false,
)(implicit
    code: ErrorCode,
    loggingContext: LoggingContext,
    logger: ContextualizedLogger,
    correlationId: CorrelationId,
) extends TransactionErrorImpl(cause, throwableO, definiteAnswer)(code) {

  def asGrpcError: StatusRuntimeException = asGrpcErrorFromContext(
    correlationId = correlationId.id,
    logger = logger,
  )(loggingContext)

  def log(): Unit = logWithContext(logger, correlationId.id)(loggingContext)

  // Automatically log the error on generation
  log()
}
