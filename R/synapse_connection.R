synapse_connection <- function(config, extensions) {
    message("
   _____
  / ___/__  ______  ____ _____  ________
  \\__ \\/ / / / __ \\/ __ `/ __ \\/ ___/ _ \\
 ___/ / /_/ / / / / /_/ / /_/ (__  )  __/
/____/\\__, /_/ /_/\\__,_/ .___/____/\\___/
     /____/           /_/
    ")
    gatewayPort <- as.integer(spark_config_value(config, "sparklyr.gateway.gatewayPort", "8880"))
    sessionId <- as.integer(spark_session_random())
    jarPath <- file.path(system.file("java", package = "sparklyr"), "sparklyr-master-2.12.jar")
    message(sprintf("[Synapse] Start sparklyr.Backend gatewayPort=%d sessionId=%d", gatewayPort, sessionId))
    tryCatch(
        {
            callSparkRStatic <- get("callJStatic", envir = asNamespace("SparkR"))
            callSparkRMethod <- get("callJMethod", envir = asNamespace("SparkR"))
            # Start `sparklyr` backend daemon
            callSparkRStatic(
                "org.apache.spark.sparklyr.SparklyrDriver",
                "start",
                jarPath,
                gatewayPort,
                sessionId
            )
        },
        error = function(err) {
            stop("Failed to start sparklyr backend: ", err$message)
        }
    )
    message("[Synapse] Successfully launched sparkr.Backend")

    sc <- new_spark_gateway_connection(
        gateway_connection(
            paste("sparklyr://localhost:", gatewayPort, "/", sessionId, sep = ""),
            config = config
        ),
        class = "synapse_connection"
    )
    message("[Synapse] Successfully connected to gateway")

    sc$stat$hive_context <- invoke(
        invoke_static(sc, "org.apache.spark.sql.SparkSession", "builder"),
        "getOrCreate"
    )
    message("[Synapse] Sucessfully connect to default SparkSession")

    sc
}

#' @export
spark_version.synapse_connection <- function(sc) {
  if (!is.null(sc$state$spark_version)) {
    return(sc$state$spark_version)
  }

  version <- eval(rlang::parse_expr("SparkR::sparkR.version()"))

  sc$state$spark_version <- numeric_version(version)

  # return to caller
  sc$state$spark_version
}
