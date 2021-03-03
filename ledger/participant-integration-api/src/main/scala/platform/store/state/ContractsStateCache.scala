package com.daml.platform.store.state

import com.daml.caching.{Cache, SizedCache}
import com.daml.lf.data.Ref.Party
import com.daml.metrics.Metrics
import com.daml.platform.store.dao.events.{Contract, ContractId}
import com.daml.platform.store.state.ContractsStateCache.ContractCacheValue

import scala.concurrent.ExecutionContext

case class ContractsStateCache(cache: Cache[ContractId, ContractCacheValue], ec: ExecutionContext)
    extends StateCache[ContractId, ContractCacheValue, ContractCacheValue] {
  override protected def toUpdateValue(u: ContractCacheValue): ContractCacheValue = u
}

object ContractsStateCache {
  // Caching possible lifecycle states of a contract, together with its stakeholders
  sealed trait ContractCacheValue
  // Inexistent contracts (e.g. coming from malformed submissions)
  // -- This situation is outside the happy flow, but it may raise an error
  final case object NotFound extends ContractCacheValue

  sealed trait ExistingContractValue extends ContractCacheValue {
    def contract: Contract
    def stakeholders: Set[Party]
  }
  final case class Active(contract: Contract, stakeholders: Set[Party])
      extends ExistingContractValue
  // For archivals we need the contract still so it can be served to divulgees that did not observe the archival
  final case class Archived(contract: Contract, stakeholders: Set[Party])
      extends ExistingContractValue

  def apply(metrics: Metrics)(implicit
      ec: ExecutionContext
  ): ContractsStateCache = new ContractsStateCache(
    SizedCache.from[ContractId, ContractCacheValue](
      SizedCache.Configuration(10000L),
      metrics.daml.execution.contractsStateCache,
    ),
    ec,
  )
}
