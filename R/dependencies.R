spark_dependencies <- function(spark_version, scala_version, ...) {
  spark_avro_version = ""
  if (spark_version < "2.0.0") {
    spark_avro_version = "2.0.1"
  } else {
    spark_avro_version = "3.2.0"
  }
  
  jar <- system.file(
    sprintf("spark-avro_%s-%s.jar", scala_version, spark_avro_version), 
    package = "sparkavro"
  )
  
  if (jar=="") { 
    # dependency not included in package dist
    sparklyr::spark_dependency(packages = c(
      sprintf("com.databricks:spark-avro_%s:%s", scala_version, spark_avro_version)
    ))
  } else { 
    # dependency is included, use jar from package install path
    sparklyr::spark_dependency(jars = jar)
  }
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}
