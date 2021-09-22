// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.value

import com.daml.lf.data.ScalazEqual
import com.daml.lf.transaction.TransactionVersion
import scalaz.Equal
import scalaz.syntax.order._

final case class Versioned[X](version: TransactionVersion, unversioned: X) {
  def map(f: X => X): Versioned[X] = Versioned(version, f(unversioned))
}

object Versioned {
  implicit def `Versioned Equal instance`[X](implicit xEqual: Equal[X]): Equal[Versioned[X]] =
    ScalazEqual.withNatural(xEqual.equalIsNatural) { (a, b) =>
      a.version == b.version && a.unversioned === b.unversioned
    }
}
