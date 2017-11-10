#' Reads a Avro File into Apache Spark
#'
#' Reads a Avro file into Apache Spark using sparklyr.
#'
#' @param sc An active \code{spark_connection}.
#' @param name The name to assign to the newly generated table.
#' @param path The path to the file. Needs to be accessible from the cluster.
#'   Supports the \samp{"hdfs://"}, \samp{"s3n://"} and \samp{"file://"} protocols.
#' @param readOptions A list of strings with additional options.
#' @param repartition The number of partitions used to distribute the
#'   generated table. Use 0 (the default) to avoid partitioning.
#' @param memory Boolean; should the data be loaded eagerly into memory? (That
#'   is, should the table be cached?)
#' @param overwrite Boolean; overwrite the table with the given name if it
#'   already exists?
#'
#' @examples
#' \dontrun{
#' ## If you haven't got a Spark cluster, you can install Spark locally like this
#' library(sparklyr)
#' spark_install(version = "2.0.1")
#'
#' sc <- spark_connect(master = "local")
#' df <- spark_read_avro(
#'   sc,
#'   "twitter",
#'   system.file("extdata/twitter.avro", package = "sparkavro"),
#'   repartition = FALSE,
#'   memory = FALSE,
#'   overwrite = FALSE
#' )
#'
#' spark_disconnect(sc)
#' }
#' @import sparklyr
#' @import DBI
#' @export
spark_read_avro <- function(sc,
                            name,
                            path,
                            readOptions = list(),
                            repartition = 0L,
                            memory = TRUE,
                            overwrite = TRUE) {
  if (overwrite && name %in% dbListTables(sc)) {
    dbRemoveTable(sc, name)
  }

  options <- sparklyr::hive_context(sc) %>%
    sparklyr::invoke("read") %>%
    sparklyr::invoke("format", "com.databricks.spark.avro")


  lapply(names(readOptions), function(optionName) {
    options <<- invoke(options, "option", optionName, readOptions[[optionName]])
  })

  df <- sparklyr::invoke(options, "load", list(spark_normalize_path(path)))

  sparklyr::invoke(df, "registerTempTable", name)

  if (memory) {
    DBI::dbGetQuery(sc, paste("CACHE TABLE", DBI::dbQuoteIdentifier(sc, name)))
    DBI::dbGetQuery(sc, paste("SELECT count(*) FROM", DBI::dbQuoteIdentifier(sc, name)))
  }

  dplyr::tbl(sc, name)
}

#' Write a Spark DataFrame to a Avro file
#'
#' Serialize a Spark DataFrame to the
#' \href{https://parquet.apache.org/}{Parquet} format.
#'
#' @param x A Spark DataFrame or dplyr operation
#' @param path The path to the file. Needs to be accessible from the cluster.
#'   Supports the \samp{"hdfs://"}, \samp{"s3n://"} and \samp{"file://"} protocols.
#' @param mode Specifies the behavior when data or table already exists.
#' @param options A list of strings with additional options. See \url{http://spark.apache.org/docs/latest/sql-programming-guide.html#configuration}.
#'
#' @family Spark serialization routines
#'
#' @export
spark_write_avro <- function(x, path, mode = NULL, options = list()) {
  UseMethod("spark_write_avro")
}

#' @export
spark_write_avro.tbl_spark <- function(x, path, mode = NULL, options = list()) {
  sqlResult <- spark_sqlresult_from_dplyr(x)
  spark_data_write_avro(sqlResult, spark_normalize_path(path), mode, options)
}

#' @export
spark_write_avro.spark_jobj <- function(x, path, mode = NULL, options = list()) {
  spark_expect_jobj_class(x, "org.apache.spark.sql.DataFrame")
  spark_data_write_avro(x, normalizePath(path), mode, options)
}

#' @import sparklyr
spark_data_write_avro <- function(df, path, mode = NULL, writeOptions = list()) {
  options <- sparklyr::invoke(df, "write")

  if (!is.null(mode)) {
    if (is.list(mode)) {
      lapply(mode, function(m) {
        options <<- sparklyr::invoke(options, "mode", m)
      })
    }
    else if (is.character(mode)) {
      options <- sparklyr::invoke(options, "mode", mode)
    }
    else {
      stop("Unsupported type ", typeof(mode), " for mode parameter.")
    }
  }

  lapply(names(writeOptions), function(writeOptionName) {
    options <<- sparklyr::invoke(options, "option", writeOptionName, writeOptions[[writeOptionName]])
  })

  sparklyr::invoke(options, "format", "com.databricks.spark.avro") %>% sparklyr::invoke("save", path)
  invisible(TRUE)
}
