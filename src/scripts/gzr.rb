#!/usr/bin/env ruby

# drop database rowz_nameserver; create database rowz_nameserver;

$:.push(File.dirname($0))
require 'socket'
require 'simple_thrift'

HOST = "localhost"
ROWZ_PORT = 7919
SHARD_PORT = 7920

SHARD_CLASS = {
  :read_only => "ReadOnlyShard",
  :blocked => "BlockedShard",
  :write_only => "WriteOnlyShard",
  :replica => "ReplicatingShard"
}

SHARD_NAME = Hash[*SHARD_CLASS.map { |a, b| [ b, a ] }.flatten]

Row = SimpleThrift.make_struct(:Row,
                               SimpleThrift::Field.new(:id, SimpleThrift::I64, 1),
                               SimpleThrift::Field.new(:name, SimpleThrift::STRING, 2),
                               SimpleThrift::Field.new(:created_at, SimpleThrift::I32, 3),
                               SimpleThrift::Field.new(:updated_at, SimpleThrift::I32, 4),
                               SimpleThrift::Field.new(:state, SimpleThrift::I32, 5))

ShardInfo = SimpleThrift.make_struct(:ShardInfo,
                                     SimpleThrift::Field.new(:class_name, SimpleThrift::STRING, 1),
                                     SimpleThrift::Field.new(:table_prefix, SimpleThrift::STRING, 2),
                                     SimpleThrift::Field.new(:hostname, SimpleThrift::STRING, 3),
                                     SimpleThrift::Field.new(:source_type, SimpleThrift::STRING, 4),
                                     SimpleThrift::Field.new(:destination_type, SimpleThrift::STRING, 5),
                                     SimpleThrift::Field.new(:busy, SimpleThrift::I32, 6),
                                     SimpleThrift::Field.new(:shard_id, SimpleThrift::I32, 7))

ShardChild = SimpleThrift.make_struct(:ShardChild,
                                      SimpleThrift::Field.new(:shard_id, SimpleThrift::I32, 1),
                                      SimpleThrift::Field.new(:weight, SimpleThrift::I32, 2))

Forwarding = SimpleThrift.make_struct(:Forwarding,
                                      SimpleThrift::Field.new(:table_id, SimpleThrift::ListType.new(SimpleThrift::I32), 1),
                                      SimpleThrift::Field.new(:base_id, SimpleThrift::I64, 2),
                                      SimpleThrift::Field.new(:shard_id, SimpleThrift::I32, 3))

ShardMigration = SimpleThrift.make_struct(:ShardMigration,
                                          SimpleThrift::Field.new(:source_shard_id, SimpleThrift::I32, 1),
                                          SimpleThrift::Field.new(:destination_shard_id, SimpleThrift::I32, 2),
                                          SimpleThrift::Field.new(:replicating_shard_id, SimpleThrift::I32, 3),
                                          SimpleThrift::Field.new(:write_only_shard_id, SimpleThrift::I32, 4))

class ShardManager < SimpleThrift::ThriftService
  thrift_method :create_shard, i32, field(:shard, struct(ShardInfo), 1)
  thrift_method :find_shard, i32, field(:shard, struct(ShardInfo), 1)
  thrift_method :get_shard, struct(ShardInfo), field(:shard_id, i32, 1)
  thrift_method :update_shard, void, field(:shard, struct(ShardInfo), 1)
  thrift_method :delete_shard, void, field(:shard_id, i32, 1)

  thrift_method :add_child_shard, void, field(:parent_shard_id, i32, 1), field(:child_shard_id, i32, 2), field(:weight, i32, 3)
  thrift_method :remove_child_shard, void, field(:parent_shard_id, i32, 1), field(:child_shard_id, i32, 2)
  thrift_method :replace_child_shard, void, field(:old_child_shard_id, i32, 1), field(:new_child_shard_id, i32, 2)
  thrift_method :list_shard_children, list(struct(ShardChild)), field(:shard_id, i32, 1)

  thrift_method :mark_shard_busy, void, field(:shard_id, i32, 1), field(:busy, i32, 2)
  thrift_method :copy_shard, void, field(:source_shard_id, i32, 1), field(:destination_shard_id, i32, 2)
  thrift_method :setup_migration, struct(ShardMigration), field(:source_shard_info, struct(ShardInfo), 1), field(:destination_shard_info, struct(ShardInfo), 2)
  thrift_method :migrate_shard, void, field(:migration, struct(ShardMigration), 1)

  thrift_method :set_forwarding, void, field(:forwarding, struct(Forwarding), 1)
  thrift_method :replace_forwarding, void, field(:old_shard_id, i32, 1), field(:new_shard_id, i32, 2)
  thrift_method :get_forwarding, struct(ShardInfo), field(:table_id, list(i32), 1), field(:base_id, i64, 2)
  thrift_method :get_forwarding_for_shard, struct(Forwarding), field(:shard_id, i32, 1)
  thrift_method :get_forwardings, list(struct(Forwarding))
  thrift_method :reload_forwardings, void
  thrift_method :find_current_forwarding, struct(ShardInfo), field(:table_id, list(i32), 1), field(:id, i64, 2)

  thrift_method :shard_ids_for_hostname, list(i32), field(:hostname, string, 1), field(:class_name, string, 2)
  thrift_method :shards_for_hostname, list(struct(ShardInfo)), field(:hostname, string, 1), field(:class_name, string, 2)
  thrift_method :get_busy_shards, list(struct(ShardInfo))
  thrift_method :get_parent_shard, struct(ShardInfo), field(:shard_id, i32, 1)
  thrift_method :get_root_shard, struct(ShardInfo), field(:shard_id, i32, 1)
  thrift_method :get_child_shards_of_class, list(struct(ShardInfo)), field(:parent_shard_id, i32, 1), field(:class_name, string, 2)

  thrift_method :rebuild_schema, void
end

class JobManager < SimpleThrift::ThriftService
  thrift_method :retry_errors, void
  thrift_method :stop_writes, void
  thrift_method :resume_writes, void
  thrift_method :retry_errors_for, void, field(:priority, i32, 1)
  thrift_method :stop_writes_for, void, field(:priority, i32, 1)
  thrift_method :resume_writes_for, void, field(:priority, i32, 1)
  thrift_method :is_writing, bool, field(:priority, i32, 1)
  thrift_method :inject_job, void, field(:priority, i32, 1), field(:job, string, 2)
end

class Rowz < SimpleThrift::ThriftService
  thrift_method :create, i64, field(:name, string, 1), field(:at, i32, 2)
  thrift_method :read, struct(Row), field(:id, i64, 1)
  thrift_method :destroy, void, field(:id, i64, 1)
end

$service = ShardManager.new(TCPSocket.new(HOST, SHARD_PORT))
$rowz = Rowz.new(TCPSocket.new(HOST, ROWZ_PORT))
$service.rebuild_schema

partitions = 10
keyspace = 2**64

partitions.times do |i|
  shard_info_a = ShardInfo.new("com.twitter.rowz.SqlShard", "data_a" + i.to_s, HOST, "", "", 0, 0)
  shard_id_a = $service.create_shard(shard_info_a)
  shard_info_b = ShardInfo.new("com.twitter.rowz.SqlShard", "data_b" + i.to_s, HOST, "", "", 0, 0)
  shard_id_b = $service.create_shard(shard_info_b)
  replicating_shard_info = ShardInfo.new("com.twitter.gizzard.shards.ReplicatingShard", "replicating_" + i.to_s, HOST, "", "", 0, 0)
  replicating_shard_id = $service.create_shard(replicating_shard_info)

  $service.add_child_shard(replicating_shard_id, shard_id_a, 1)
  $service.add_child_shard(replicating_shard_id, shard_id_b, 1)

  lower_bound = keyspace / partitions * i
  $service.set_forwarding(Forwarding.new([], lower_bound, replicating_shard_id))
end
$service.reload_forwardings

10.times do |i|
  id = $rowz.create("row #{i}", Time.now.to_i)
  p id
  sleep 0.1
  p $rowz.read(id)
end