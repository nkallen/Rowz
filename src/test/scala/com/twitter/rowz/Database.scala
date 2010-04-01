package com.twitter.rowz

import net.lag.configgy.Configgy
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.database.{ApachePoolingDatabaseFactory, MemoizingDatabaseFactory}
import com.twitter.querulous.evaluator.StandardQueryEvaluatorFactory
import com.twitter.xrayspecs.TimeConversions._


object Database {
  val config = Configgy.config
  val databaseFactory = new MemoizingDatabaseFactory(new ApachePoolingDatabaseFactory(
    config("rowz.db.connection_pool.size_min").toInt,
    config("rowz.db.connection_pool.size_max").toInt,
    config("rowz.db.connection_pool.test_idle_msec").toLong.millis,
    config("rowz.db.connection_pool.max_wait").toLong.millis,
    config("rowz.db.connection_pool.test_on_borrow").toBoolean,
    config("rowz.db.connection_pool.min_evictable_idle_msec").toLong.millis))
  val queryEvaluatorFactory = new StandardQueryEvaluatorFactory(databaseFactory, new SqlQueryFactory)
}