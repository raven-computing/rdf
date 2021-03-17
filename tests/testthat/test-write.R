# Copyright (C) 2021 Raven Computing
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

column_names <- c(
  "byteCol",    # 0
  "shortCol",   # 1
  "intCol",     # 2
  "longCol",    # 3
  "stringCol",  # 4
  "charCol",    # 5
  "floatCol",   # 6
  "doubleCol",  # 7
  "booleanCol", # 8
  "binaryCol"   # 9
)

df_default <- data.frame(
  c(10L, 20L, 30L, 40L, 50L),
  c(11L, 21L, 31L, 41L, 51L),
  c(12L, 22L, 32L, 42L, 52L),
  c(13L, 23L, 33L, 43L, 53L),
  c("10", "20", "30", "40", "50"),
  c("a", "b", "c", "d", "e"),
  c(10.1, 20.2, 30.3, 40.4, 50.5),
  c(11.1, 21.2, 31.3, 41.4, 51.5),
  c(TRUE, FALSE, TRUE, FALSE, TRUE),
  I(list(
    as.raw(c(0x01, 0x02, 0x03, 0x04, 0x05)),
    as.raw(c(0x05, 0x04, 0x03, 0x02, 0x01)),
    as.raw(c(0x05, 0x02, 0x01, 0x02, 0x03)),
    as.raw(c(0x02, 0x01, 0x04, 0x05, 0x03)),
    as.raw(c(0x03, 0x01, 0x02, 0x05, 0x04))
  )),
  stringsAsFactors = FALSE
)

df_nullable <- data.frame(
  c(10L, NA, NA, 0L, 50L),
  c(11L, 21L, NA, 0L, NA),
  c(12L, NA, 32L, 0L, NA),
  c(NA, NA, 33L, 0L, 53L),
  c("ABCD", "2!\"0,.", NA, "", "#5{=0>}"),
  c(",", "b", NA, "d", "?"),
  c(10.1, NA, 0.0, NA, 50.5),
  c(NA, 0.0, 0.0, NA, 51.5),
  c(TRUE, NA, FALSE, NA, TRUE),
  I(list(
    as.raw(c(0x00)),
    as.raw(c(0x05, 0x04, 0x03, 0x02, 0x01)),
    NA,
    as.raw(c(0x02, 0x01, 0x04, 0x05, 0x4a, 0x05, 0x03)),
    NA
  )),
  stringsAsFactors = FALSE
)

colnames(df_default) <- column_names
colnames(df_nullable) <- column_names

test_that("data.frame is serialized correctly (default, no types)", {
  bytes <- serializeDataFrame(df_default, compress = FALSE)
  filepath <- "truth_default_notypes"
  f <- file(filepath, "rb")
  on.exit(close(f))
  truth <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  expect_equal(bytes, truth)
})

test_that("data.frame is serialized correctly (nullable, no types)", {
  bytes <- serializeDataFrame(df_nullable, compress = FALSE)
  filepath <- "truth_nullable_notypes"
  f <- file(filepath, "rb")
  on.exit(close(f))
  truth <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  expect_equal(bytes, truth)
})

test_that("data.frame is serialized correctly with column types (default)", {
  t <- c("byte", "short", "int", "long", "string", "char",
         "float", "double", "boolean", "binary")

  bytes <- serializeDataFrame(df_default, types = t, compress = FALSE)
  filepath <- "truth_default"
  f <- file(filepath, "rb")
  on.exit(close(f))
  truth <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  expect_equal(bytes, truth)
})

test_that("data.frame is serialized correctly with column types (nullable)", {
  t <- c("byte", "short", "int", "long", "string", "char",
         "float", "double", "boolean", "binary")

  bytes <- serializeDataFrame(df_nullable, types = t, compress = FALSE)
  filepath <- "truth_nullable"
  f <- file(filepath, "rb")
  on.exit(close(f))
  truth <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  expect_equal(bytes, truth)
})

test_that("data.frame is serialized correctly (default, compressed)", {
  t <- c("byte", "short", "int", "long", "string", "char",
         "float", "double", "boolean", "binary")

  bytes <- serializeDataFrame(df_default, types = t, compress = TRUE)
  filepath <- "truth_default_compressed"
  f <- file(filepath, "rb")
  on.exit(close(f))
  truth <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  expect_equal(bytes, truth)
})

test_that("data.frame is serialized correctly (nullable, compressed)", {
  t <- c("byte", "short", "int", "long", "string", "char",
         "float", "double", "boolean", "binary")

  bytes <- serializeDataFrame(df_nullable, types = t, compress = TRUE)
  filepath <- "truth_nullable_compressed"
  f <- file(filepath, "rb")
  on.exit(close(f))
  truth <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  expect_equal(bytes, truth)
})

test_that("uninitialized data.frame is serialized correctly (default)", {
  df <- data.frame()
  bytes <- serializeDataFrame(df, compress = TRUE)
  filepath <- "truth_default_uninitialized.df"
  f <- file(filepath, "rb")
  on.exit(close(f))
  truth <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  expect_equal(bytes, truth)
})

test_that("uninitialized data.frame is serialized correctly (nullable)", {
  df <- data.frame()
  bytes <- serializeDataFrame(df, as.nullable = TRUE, compress = TRUE)
  filepath <- "truth_nullable_uninitialized.df"
  f <- file(filepath, "rb")
  on.exit(close(f))
  truth <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  expect_equal(bytes, truth)
})

test_that("empty data.frame is serialized correctly (default)", {
  t <- c("byte", "short", "int", "long", "string", "char",
         "float", "double", "boolean", "binary")

  df <- df_default[0, ]
  bytes <- serializeDataFrame(df, types = t, compress = TRUE)
  filepath <- "truth_default_empty.df"
  f <- file(filepath, "rb")
  on.exit(close(f))
  truth <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  expect_equal(bytes, truth)
})

test_that("empty data.frame is serialized correctly (nullable)", {
  t <- c("byte", "short", "int", "long", "string", "char",
         "float", "double", "boolean", "binary")

  df <- df_nullable[0, ]
  bytes <- serializeDataFrame(df, types = t,
                              compress = TRUE,
                              as.nullable = TRUE)

  filepath <- "truth_nullable_empty.df"
  f <- file(filepath, "rb")
  on.exit(close(f))
  truth <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  expect_equal(bytes, truth)
})

test_that("data.frame with one column is serialized correctly (default)", {
  t <- c("int")
  df <- df_default["intCol"]
  bytes <- serializeDataFrame(df, types = t, compress = TRUE)
  filepath <- "truth_default_onecolumn.df"
  f <- file(filepath, "rb")
  on.exit(close(f))
  truth <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  expect_equal(bytes, truth)
})

test_that("data.frame with one column is serialized correctly (nullable)", {
  t <- c("int")
  df <- df_nullable["intCol"]
  bytes <- serializeDataFrame(df, types = t, compress = TRUE)
  filepath <- "truth_nullable_onecolumn.df"
  f <- file(filepath, "rb")
  on.exit(close(f))
  truth <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  expect_equal(bytes, truth)
})

test_that("continuous typed serialization works (default)", {
  df <- data.frame(df_default)
  types <- c("byte", "short", "int", "long", "string",
             "char", "float", "double", "boolean", "binary")

  for (i in 1:ncol(df)){
    df <- df[, c(2:ncol(df),1)]
    types <- c(types[2:length(types)], types[1])
    b <- serializeDataFrame(df, types = types)
    df <- deserializeDataFrame(b)
  }
  expect_equal(df, df_default, tolerance = 5)
})

test_that("continuous typed serialization works (nullable)", {
  df <- data.frame(df_nullable)
  types <- c("byte", "short", "int", "long", "string",
             "char", "float", "double", "boolean", "binary")

  for (i in 1:ncol(df)){
    df <- df[, c(2:ncol(df),1)]
    types <- c(types[2:length(types)], types[1])
    b <- serializeDataFrame(df, types = types)
    df <- deserializeDataFrame(b)
  }
  expect_equal(df, df_nullable, tolerance = 5)
})
