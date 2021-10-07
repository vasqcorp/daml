// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import com.daml.platform.store.backend.common.Field

private[postgresql] trait PGStringArrayBase[FROM, TO] extends Field[FROM, TO, String] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"string_to_array($inputFieldName, '|')"

  protected def convertBase: Iterable[String] => String = { in =>
    assert(
      in.forall(!_.contains("|")),
      s"The following input string(s) contain the character '|', which is not expected: ${in.filter(_.contains("|")).mkString(", ")}",
    )
    in.mkString("|")
  }
}

private[postgresql] case class PGStringArray[FROM](
    extract: (String => Int) => FROM => Iterable[String]
) extends PGStringArrayBase[FROM, Iterable[String]] {
  override def convert: Iterable[String] => String = convertBase
}

private[postgresql] case class PGStringArrayOptional[FROM](
    extract: (String => Int) => FROM => Option[Iterable[String]]
) extends PGStringArrayBase[FROM, Option[Iterable[String]]] {
  override def convert: Option[Iterable[String]] => String = _.map(convertBase).orNull
}

private[postgresql] trait PGIntArrayBase[FROM, TO] extends Field[FROM, TO, String] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"string_to_array($inputFieldName, '|')::integer[]" // TODO consider doing some hex magic here to compress the transport data more

  protected def convertBase: Iterable[Int] => String = { in =>
    in.mkString("|")
  }
}

private[postgresql] case class PGIntArray[FROM](extract: (String => Int) => FROM => Iterable[Int])
    extends PGIntArrayBase[FROM, Iterable[Int]] {
  override def convert: Iterable[Int] => String = convertBase
}

private[postgresql] case class PGIntArrayOptional[FROM](
    extract: (String => Int) => FROM => Option[Iterable[Int]]
) extends PGIntArrayBase[FROM, Option[Iterable[Int]]] {
  override def convert: Option[Iterable[Int]] => String = _.map(convertBase).orNull
}

private[postgresql] case class PGSmallintOptional[FROM](
    extract: (String => Int) => FROM => Option[Int]
) extends Field[FROM, Option[Int], java.lang.Integer] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"$inputFieldName::smallint"

  override def convert: Option[Int] => Integer = _.map(Int.box).orNull
}
