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

test_that("data.frame is initialized correctly (default)", {
  dftypes <- unname(sapply(df_default, typeof))
  expect_equal(dftypes, c("integer", "integer", "integer", "integer",
                          "character", "character", "double", "double",
                          "logical", "list"))
})

test_that("data.frame is initialized correctly (nullable)", {
  dftypes <- unname(sapply(df_nullable, typeof))
  expect_equal(dftypes, c("integer", "integer", "integer", "integer",
                          "character", "character", "double", "double",
                          "logical", "list"))
})

test_that("data.frame is deserialized correctly (default)", {
  filepath <- "truth_default_notypes"
  f <- file(filepath, "rb")
  on.exit(close(f))
  bytes <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  df <- deserializeDataFrame(bytes)
  expect_equal(df, df_default, tolerance = 5)
})

test_that("data.frame is deserialized correctly (nullable)", {
  filepath <- "truth_nullable_notypes"
  f <- file(filepath, "rb")
  on.exit(close(f))
  bytes <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  df <- deserializeDataFrame(bytes)
  expect_equal(df, df_nullable, tolerance = 5)
})

test_that("data.frame is deserialized correctly with column types (default)", {
  filepath <- "truth_default"
  f <- file(filepath, "rb")
  on.exit(close(f))
  bytes <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  df <- deserializeDataFrame(bytes)
  expect_equal(df, df_default, tolerance = 5)
})

test_that("data.frame is deserialized correctly with column types (nullable)", {
  filepath <- "truth_nullable"
  f <- file(filepath, "rb")
  on.exit(close(f))
  bytes <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  df <- deserializeDataFrame(bytes)
  expect_equal(df, df_nullable, tolerance = 5)
})

test_that("data.frame is deserialized (default, compressed) correctly", {
  filepath <- "truth_default_compressed"
  f <- file(filepath, "rb")
  on.exit(close(f))
  bytes <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  df <- deserializeDataFrame(bytes)
  expect_equal(df, df_default, tolerance = 5)
})

test_that("data.frame is deserialized (nullable, compressed) correctly", {
  filepath <- "truth_nullable_compressed"
  f <- file(filepath, "rb")
  on.exit(close(f))
  bytes <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  df <- deserializeDataFrame(bytes)
  expect_equal(df, df_nullable, tolerance = 5)
})

test_that("df file is read correctly (default)", {
  df <- readDataFrame("truth_default_compressed")
  expect_equal(df, df_default, tolerance = 5)
})

test_that("df file is read correctly (nullable)", {
  df <- readDataFrame("truth_nullable_compressed")
  expect_equal(df, df_nullable, tolerance = 5)
})

test_that("df file with uninitialized DataFrame is read correctly (default)", {
  df <- readDataFrame("truth_default_uninitialized.df")
  truth <- data.frame()
  expect_equal(df, truth, tolerance = 5)
})

test_that("df file with uninitialized DataFrame is read correctly (nullable)", {
  df <- readDataFrame("truth_nullable_uninitialized.df")
  truth <- data.frame()
  expect_equal(df, truth, tolerance = 5)
})

test_that("df file with empty DataFrame is read correctly (default)", {
  df <- readDataFrame("truth_default_empty.df")
  truth <- df_default[0, ]
  expect_equal(df, truth, tolerance = 5)
})

test_that("df file with empty DataFrame is read correctly (nullable)", {
  df <- readDataFrame("truth_nullable_empty.df")
  truth <- df_nullable[0, ]
  expect_equal(df, truth, tolerance = 5)
})

test_that("df file with a one-column DataFrame is read correctly (default)", {
  df <- readDataFrame("truth_default_onecolumn.df")
  truth <- df_default["intCol"]
  expect_equal(df, truth, tolerance = 5)
})

test_that("df file with a one-column DataFrame is read correctly (nullable)", {
  df <- readDataFrame("truth_nullable_onecolumn.df")
  truth <- df_nullable["intCol"]
  expect_equal(df, truth, tolerance = 5)
})

test_that("df file containing only nulls is read correctly", {
  df <- readDataFrame("truth_nullable_onlynulls.df")
  truth <- data.frame(df_nullable)
  truth[, "byteCol"] <- NA_integer_
  truth[, "shortCol"] <- NA_integer_
  truth[, "intCol"] <- NA_integer_
  truth[, "longCol"] <- NA_integer_
  truth[, "stringCol"] <- NA_character_
  truth[, "charCol"] <- NA_character_
  truth[, "floatCol"] <- NA_real_
  truth[, "doubleCol"] <- NA_real_
  truth[, "booleanCol"] <- NA
  for (i in 1:nrow(truth)) {
    truth[i, "binaryCol"] <- list(NA)
  }

  expect_equal(df, truth, tolerance = 5)
})

test_that("continuous serialization works (default)", {
  df <- data.frame(df_default)
  for (i in 1:ncol(df)){
    df <- df[, c(2:ncol(df),1)]
    b <- serializeDataFrame(df)
    df <- deserializeDataFrame(b)
  }
  expect_equal(df, df_default, tolerance = 5)
})

test_that("continuous serialization works (nullable)", {
  df <- data.frame(df_nullable)
  for (i in 1:ncol(df)){
    df <- df[, c(2:ncol(df),1)]
    b <- serializeDataFrame(df)
    df <- deserializeDataFrame(b)
  }
  expect_equal(df, df_nullable, tolerance = 5)
})
