spark_dependencies <- function(spark_version, scala_version, ...) {
    spark_avro_version = ""
    spark_avro_lib = "com.databricks"
    if (spark_version < "2.0.0") {
      spark_avro_version = "2.0.1"
    } else if (spark_version < "2.2.0") {
      spark_avro_version = "3.2.0"
    } else if (spark_version < "2.4.0") {
      spark_avro_version = "4.0.0"
    } else {
      spark_avro_lib = "org.apache.spark"
      # Full semantic version is needed, so 2.4 -> 2.4.0
      if (nchar(as.character(spark_version)) == 3) {
        spark_avro_version = numeric_version(paste0(spark_version, ".0"))
      } else spark_avro_version = spark_version
    }

    sparklyr::spark_dependency(
    jars = c(
    ),
    packages = c(
      sprintf("%s:spark-avro_%s:%s", spark_avro_lib, scala_version, spark_avro_version)
    )
  )
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}
