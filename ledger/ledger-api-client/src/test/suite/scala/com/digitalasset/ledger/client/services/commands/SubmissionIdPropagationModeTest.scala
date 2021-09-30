// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands

import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.NotOkResponse
import com.daml.ledger.client.services.commands.tracker.TrackingData
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
        trackingDataForCompletion = Seq.empty,
        maybeSubmissionId = None,
      ) shouldBe Seq.empty
    }

    "return a completion response (if the propagation is supported)" in {
      val submissionId = Some("aSubmissionId")
      val completion = aCompletion(submissionId)
      SubmissionIdPropagationMode.Supported.handleCompletion(
        completion = completion,
        trackingDataForCompletion = Seq(trackingData),
        maybeSubmissionId = submissionId,
      ) shouldBe Seq(Ctx("context", tracker.CompletionResponse(completion)))
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
      SubmissionIdPropagationMode.NotSupported.handleCompletion(
        completion = aCompletion(maybeSubmissionId = None),
        trackingDataForCompletion = Seq(trackingData, trackingData),
        maybeSubmissionId = None,
      ) shouldBe Seq(expectedResponse, expectedResponse)
    }

    "return a completion response (if the propagation is not supported)" in {
      val submissionId = Some("aSubmissionId")
      val completion = aCompletion(submissionId)
      SubmissionIdPropagationMode.NotSupported.handleCompletion(
        completion,
        Seq(trackingData),
        submissionId,
      ) shouldBe Seq(Ctx(context, tracker.CompletionResponse(completion)))
    }
  }
}

object SubmissionIdPropagationModeTest {
  private val aCommandId = "aCommandId"

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
