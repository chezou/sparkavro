#' Reads a Avro File into Apache Spark
#'
#' Reads a Avro file into Apache Spark using sparklyr.
#'
#' @param sc An active \code{spark_connection}.
#' @param name The name to assign to the newly generated table.
#' @param path The path to the file. Needs to be accessible from the cluster.
#'   Supports the \samp{"hdfs://"}, \samp{"s3n://"} and \samp{"file://"} protocols.
#' @param repartition The number of partitions used to distribute the
#'   generated table. Use 0 (the default) to avoid partitioning.
#' @param memory Boolean; should the data be loaded eagerly into memory? (That
#'   is, should the table be cached?)
#' @param overwrite Boolean; overwrite the table with the given name if it
#'   already exists?
#'
#' @examples
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "spark://HOST:PORT")
#' df <- spark_read_avro(
#'   sc,
#'   "samples/sample.avro",
#'   repartition = FALSE,
#'   memory = FALSE,
#'   overwrite = FALSE
#' )
#'
#' spark_disconnect(sc)
#'
#' @export
#' @import DBI
spark_read_avro <- function(sc,
                            name,
                            path,
                            repartition = 0L,
                            memory = TRUE,
                            overwrite = TRUE,
                            group = FALSE,
                            parse = FALSE,
                            ...) {
  if (overwrite && name %in% dbListTables(sc)) {
    dbRemoveTable(sc, name)
  }

  df <- hive_context(sc) %>%
    invoke("read") %>%
    invoke("format", "com.databricks.spark.avro") %>%
    invoke("load", list(spark_normalize_path(path)))

  invoke(df, "registerTempTable", name)

  if (memory) {
    dbGetQuery(sc, paste("CACHE TABLE", DBI::dbQuoteIdentifier(sc, name)))
    dbGetQuery(sc, paste("SELECT count(*) FROM", DBI::dbQuoteIdentifier(sc, name)))
  }

  tbl(sc, name)
  invisible(NULL)
}

#' Write a Spark DataFrame to a Avro file
#'
#' Serialize a Spark DataFrame to the
#' \href{https://parquet.apache.org/}{Parquet} format.
#'
#' @param sc An active \code{spark_connection}.
#' @param name The name to assign to the newly generated table.
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

spark_data_write_avro <- function(df, path, mode = NULL, csvOptions = list()) {
  options <- invoke(df, "write")

  if (!is.null(mode)) {
    if (is.list(mode)) {
      lapply(mode, function(m) {
        options <<- invoke(options, "mode", m)
      })
    }
    else if (is.character(mode)) {
      options <- invoke(options, "mode", mode)
    }
    else {
      stop("Unsupported type ", typeof(mode), " for mode parameter.")
    }
  }

  if(!is.null(options)) {
    options <<- invoke(options, "options", csvOptions)
  }

  lapply(names(csvOptions), function(csvOptionName) {
    options <<- invoke(options, "option", csvOptionName, csvOptions[[csvOptionName]])
  })

  invoke(options, "format", "com.databricks.spark.avro") %>% invoke("save", path)
  invisible(TRUE)
}


# normalizes a path that we are going to send to spark but avoids
# normalizing remote identifiers like hdfs:// or s3n://. note
# that this will take care of path.expand ("~") as well as converting
# relative paths to absolute (necessary since the path will be read by
# another process that has a different current working directory)
spark_normalize_path <- function(path) {
  # don't normalize paths that are urls
  if (grepl("[a-zA-Z]+://", path)) {
    path
  }
  else {
    normalizePath(path, mustWork = FALSE)
  }
}
