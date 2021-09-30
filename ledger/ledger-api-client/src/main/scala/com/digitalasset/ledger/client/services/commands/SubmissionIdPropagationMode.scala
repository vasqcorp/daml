// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands

import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.services.commands.tracker.CommandTracker.ContextualizedCompletionResponse
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.NotOkResponse
import com.daml.ledger.client.services.commands.tracker.TrackingData
import com.daml.util.Ctx
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status
import org.slf4j.LoggerFactory

sealed trait SubmissionIdPropagationMode {
  def handleCompletion[Context](
      completion: Completion,
      trackingDataForCompletion: Seq[TrackingData[Context]],
      maybeSubmissionId: Option[String],
  ): Seq[ContextualizedCompletionResponse[Context]]
}

object SubmissionIdPropagationMode {
  case object Supported extends SubmissionIdPropagationMode {
    private val logger = LoggerFactory.getLogger(this.getClass.getName)

    def handleCompletion[Context](
        completion: Completion,
        trackingDataForCompletion: Seq[TrackingData[Context]],
        maybeSubmissionId: Option[String],
    ): Seq[ContextualizedCompletionResponse[Context]] =
      if (maybeSubmissionId.isEmpty) {
        logger.trace(
          "Ignoring a completion with an empty submission ID for a submission from the CommandSubmissionService."
        )
        Seq.empty
      } else {
        trackingDataForCompletion.map(trackingData =>
          Ctx(trackingData.context, tracker.CompletionResponse(completion))
        )
      }
  }

  case object NotSupported extends SubmissionIdPropagationMode {
    def handleCompletion[Context](
        completion: Completion,
        trackingDataForCompletion: Seq[TrackingData[Context]],
        maybeSubmissionId: Option[String],
    ): Seq[ContextualizedCompletionResponse[Context]] =
      if (trackingDataForCompletion.size > 1) {
        trackingDataForCompletion.map { trackingData =>
          val commandId = completion.commandId
          Ctx(
            trackingData.context,
            Left(
              NotOkResponse(
                Completion(
                  commandId,
                  Some(
                    StatusProto.of(
                      Status.Code.INTERNAL.value(),
                      s"There are multiple pending commands with ID: $commandId for submission ID: $maybeSubmissionId.",
                      Seq.empty,
                    )
                  ),
                )
              )
            ),
          )
        }
      } else {
        trackingDataForCompletion.map(trackingData =>
          Ctx(trackingData.context, tracker.CompletionResponse(completion))
        )
      }
  }
}
