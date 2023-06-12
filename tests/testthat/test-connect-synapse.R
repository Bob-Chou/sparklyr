message("testing synapse connection")

skip_on_livy()
skip_on_arrow_devel()
skip_unless_synapse_connect()

test_that("spark connection method for Synapse should be configured", {
  sc <- testthat_spark_connection()
  expect_equal(sc$method, "synapse")
})

test_that("sparklyr gateway for Synapse should be configured", {
  sc <- testthat_spark_connection()
  connector <- invoke_static(sc, "org.apache.spark.sparklyr.SparkConnector.getOrCreate")
  expect_false(is.null(connector))
  gateway_url <- invoke_method(sc, NULL, connector, "getUri")
  expect_true(spark_master_is_gateway(gateway_url))
})

test_that("spark session should be identical", {
  call_sparkr <- function(method, ...) {
    get(method, asNamespace("SparkR"), ...)
  }
  expect_equal(
    call_sparkr("callJMethod", call_sparkr("session"), "hashCode"),
    invoke_method(sc, NULL, sc$state$hive_context, "hashCode")
  )
})
