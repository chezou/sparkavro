spark_dependencies <- function(spark_version, scala_version, ...) {
    spark_avro_version = ""
    if (spark_version < "2.0.0") {
      spark_avro_version = "2.0.1"
    } else {
      spark_avro_version = "3.2.0"
    }

    sparklyr::spark_dependency(
    jars = c(
    ),
    packages = c(
      sprintf("com.databricks:spark-avro_%s:%s", scala_version, spark_avro_version)
    )
  )
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}
