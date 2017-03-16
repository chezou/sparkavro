library(sparklyr)

library(sparkavro)
library(dplyr)
if(!exists("sc")){
  sc <- spark_connect(master="local")
}

test_that("read existing avro", {
  skip_on_cran()

  df <- spark_read_avro(sc, "twitter", "./inst/extdata/twitter.avro")
  expect_equal(df %>% collect() %>% length(), 3)
})

test_that("write Spark DataFrame into avro", {
  skip_on_cran()

  df2 <- df %>% filter(username == "miguno")
  filename <- tempfile("test", fileext=".avro")
  spark_write_avro(df2, filename)
  expect_true(file.exists(filename))
})
