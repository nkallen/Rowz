package com.twitter.rowz

import com.twitter.gizzard.nameserver.{Forwarding, NameServer}
import com.twitter.gizzard.shards.ShardException


class ForwardingManager(nameServer: NameServer[Shard]) extends (Long => Shard) {
  def apply(id: Long) = nameServer.findCurrentForwarding(0, id)
}
