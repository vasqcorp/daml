// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.configuration

import java.time.Duration

/** @param maxCommandsInFlight The maximum number of unconfirmed commands the client may track.
  *                            The client will backpressure when this number is reached.
  * @param maxParallelSubmissions The maximum number of parallel command submissions at a given time.
  *                               The client will backpressure when this number is reached.
  * @param defaultDeduplicationTime The deduplication time to use for commands that do not have
  *                                 a deduplication time set. The deduplication time is also used
  *                                 as the time after which commands time out in the command client.
  * @param submissionIdPropagationSupported Tells whether submission IDs are being returned in Completions.
  *                                         Should be false for integrations where the mutating schema is enabled.
  */
final case class CommandClientConfiguration(
    maxCommandsInFlight: Int,
    maxParallelSubmissions: Int,
    defaultDeduplicationTime: Duration,
    submissionIdPropagationSupported: Boolean,
)

object CommandClientConfiguration {
  def default: CommandClientConfiguration = CommandClientConfiguration(
    maxCommandsInFlight = 1,
    maxParallelSubmissions = 1,
    defaultDeduplicationTime = Duration.ofSeconds(30L),
    submissionIdPropagationSupported = false,
  )
}
