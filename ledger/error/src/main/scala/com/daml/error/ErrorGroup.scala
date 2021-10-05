// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

abstract class ErrorGroup()(implicit parent: ErrorClass) {
  private val fullClassName: String = getClass.getName

  // Hit https://github.com/scala/bug/issues/5425?orig=1 here: we cannot use .getSimpleName in deeply nested objects
  // TODO error codes: Switch to using .getSimpleName when switching to JDK 9+
  implicit val errorClass: ErrorClass = doIt()


  private def doIt(): ErrorClass = {
    val name = fullClassName
      .replace("$", ".")
      .split("\\.")
      .view
      .reverse
      .find(segment => segment.trim.nonEmpty)
      .getOrElse(
        throw new IllegalStateException(
          s"Could not parse full class name: '${fullClassName}' for the error class name"
        )
      )
    //    val i = fullClassName.lastIndexOf("$")
    //    val name = if (i == fullClassName.length - 1){
    //      fullClassName.substring(fullClassName.lastIndexOf("."), i - 1)
    //    } else if (i == -1) {
    //      fullClassName.substring(fullClassName.lastIndexOf("."))
    //    } else {
    //      fullClassName.substring(fullClassName.lastIndexOf("$"))
    //    }
    parent.extend(name)
  }
}
