// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands

import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.NotOkResponse
import com.daml.ledger.client.services.commands.tracker.{TrackedCommandKey, TrackingData}
import com.daml.util.Ctx
import com.google.rpc.status.{Status => StatusProto}
import io.grpc.Status
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class SubmissionIdPropagationModeTest extends AnyWordSpec with Matchers {
  import SubmissionIdPropagationModeTest._

  "handleCompletion" should {
    "ignore a completion without a submission id (if the propagation is supported)" in {
      SubmissionIdPropagationMode.Supported.handleCompletion(
        aCompletion(maybeSubmissionId = None),
        trackedCommands = Map.empty,
        maybeSubmissionId = None,
      ) shouldBe Map.empty
    }

    "return a completion response (if the propagation is supported)" in {
      val submissionId = Some(aSubmissionId)
      val completion = aCompletion(submissionId)
      SubmissionIdPropagationMode.Supported.handleCompletion(
        completion = completion,
        trackedCommands = Map(aTrackedCommandKey -> trackingData),
        maybeSubmissionId = submissionId,
      ) shouldBe Map(aTrackedCommandKey -> Ctx(context, tracker.CompletionResponse(completion)))
    }

    "return a failed completion if there are multiple tracked submissions with the same command id (if the propagation is not supported)" in {
      val expectedResponse = Ctx(
        trackingData.context,
        Left(
          NotOkResponse(
            Completion(
              aCommandId,
              Some(
                StatusProto.of(
                  Status.Code.INTERNAL.value(),
                  s"There are multiple pending commands with ID: $aCommandId for submission ID: None.",
                  Seq.empty,
                )
              ),
            )
          )
        ),
      )
      val key1 = TrackedCommandKey("submissionId1", aCommandId)
      val key2 = TrackedCommandKey("submissionId2", aCommandId)
      SubmissionIdPropagationMode.NotSupported.handleCompletion(
        completion = aCompletion(maybeSubmissionId = None),
        trackedCommands = Map(key1 -> trackingData, key2 -> trackingData),
        maybeSubmissionId = None,
      ) shouldBe Map(key1 -> expectedResponse, key2 -> expectedResponse)
    }

    "return a completion response (if the propagation is not supported)" in {
      val submissionId = Some(aSubmissionId)
      val completion = aCompletion(submissionId)
      SubmissionIdPropagationMode.NotSupported.handleCompletion(
        completion,
        trackedCommands = Map(aTrackedCommandKey -> trackingData),
        submissionId,
      ) shouldBe Map(aTrackedCommandKey -> Ctx(context, tracker.CompletionResponse(completion)))
    }
  }
}

object SubmissionIdPropagationModeTest {
  private val aSubmissionId = "aSubmissionId"
  private val aCommandId = "aCommandId"
  private val aTrackedCommandKey = TrackedCommandKey(aSubmissionId, aCommandId)
  private val context = "context"
  private val trackingData = TrackingData(aCommandId, Instant.MAX, context)

  private def aCompletion(maybeSubmissionId: Option[String]) = Completion.of(
    commandId = aCommandId,
    status = Some(StatusProto.of(Status.Code.OK.value(), "", Seq.empty)),
    transactionId = "aTransactionId",
    applicationId = "anApplicationId",
    actAs = Seq.empty,
    submissionId = maybeSubmissionId.getOrElse(""),
    deduplicationPeriod = Completion.DeduplicationPeriod.Empty,
  )
}
