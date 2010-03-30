package com.twitter.rowz

import com.twitter.gizzard.nameserver
import com.twitter.querulous.evaluator.QueryEvaluator


class ForwardingManager(mappingFunction: Long => Long, protected val queryEvaluator: QueryEvaluator)
  extends nameserver.ForwardingManager[Shard] {
  
}