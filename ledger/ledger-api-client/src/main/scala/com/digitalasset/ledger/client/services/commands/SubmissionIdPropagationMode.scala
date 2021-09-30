// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands

import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.services.commands.tracker.CommandTracker.ContextualizedCompletionResponse
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.NotOkResponse
import com.daml.ledger.client.services.commands.tracker.{TrackedCommandKey, TrackingData}
import com.daml.util.Ctx
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status
import org.slf4j.LoggerFactory

sealed trait SubmissionIdPropagationMode {
  def handleCompletion[Context](
      completion: Completion,
      trackedCommands: collection.Map[TrackedCommandKey, TrackingData[Context]],
      maybeSubmissionId: Option[String],
  ): collection.Map[TrackedCommandKey, ContextualizedCompletionResponse[Context]]
}

object SubmissionIdPropagationMode {
  case object Supported extends SubmissionIdPropagationMode {
    private val logger = LoggerFactory.getLogger(this.getClass.getName)

    def handleCompletion[Context](
        completion: Completion,
        trackedCommands: collection.Map[TrackedCommandKey, TrackingData[Context]],
        maybeSubmissionId: Option[String],
    ): collection.Map[TrackedCommandKey, ContextualizedCompletionResponse[Context]] =
      maybeSubmissionId
        .map { submissionId =>
          val key = TrackedCommandKey(submissionId, completion.commandId)
          val trackedCommandForCompletion = trackedCommands.get(key)

          trackedCommandForCompletion
            .map { trackingData =>
              Map(key -> Ctx(trackingData.context, tracker.CompletionResponse(completion)))
            }
            .getOrElse(Map.empty)
        }
        .getOrElse {
          logger.trace(
            "Ignoring a completion with an empty submission ID for a submission from the CommandSubmissionService."
          )
          Map.empty
        }
  }

  case object NotSupported extends SubmissionIdPropagationMode {
    def handleCompletion[Context](
        completion: Completion,
        trackedCommands: collection.Map[TrackedCommandKey, TrackingData[Context]],
        maybeSubmissionId: Option[String],
    ): collection.Map[TrackedCommandKey, ContextualizedCompletionResponse[Context]] = {
      val commandId = completion.commandId

      val trackedCommandsForCompletion = maybeSubmissionId
        .map { submissionId =>
          val key = TrackedCommandKey(submissionId, commandId)
          trackedCommands
            .get(key)
            .map(trackingData => Map(key -> trackingData))
            .getOrElse(Map.empty)
        }
        .getOrElse {
          trackedCommands.filter { case (key, _) => key.commandId == commandId }
        }

      if (trackedCommandsForCompletion.size > 1) {
        trackedCommandsForCompletion.map { case (key, trackingData) =>
          key -> Ctx(
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
        trackedCommandsForCompletion.map { case (key, trackingData) =>
          key -> Ctx(trackingData.context, tracker.CompletionResponse(completion))
        }
      }
    }
  }
}
