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

#' Reads a DataFrame from the specified file.
#'
#' The file to be read must be a DataFrame (.df) file. The content of the file
#' is returned as an R data.frame object.
#'
#' @param filepath The path to the file to read
#'
#' @return A data.frame object
#'
#' @details
#' The column types from Raven DataFrames are mapped to the
#' corresponding R types. More specifically, all integer types
#' (byte, short, int, long) are mapped to the R 'integer' type. The floating
#' point types (float, double) are mapped to the R 'double' type. Both string
#' and char types are mapped to the R 'character' type. Booleans are mapped to
#' the R 'logical' type. Binary columns are represented as R 'list' types
#' containing raw vectors.
#'
#' @seealso
#' [raven.rdf::deserializeDataFrame()] for deserializing vectors
#' of raw bytes. \cr
#' [raven.rdf::writeDataFrame()] for writing DataFrame files which can be read
#' by this function.
#'
#' @examples
#' \dontrun{
#' # read a .df file into memory
#' df <- readDataFrame("/path/to/my/file.df")
#'
#' # get the types for all columns
#' types <- sapply(df, typeof)
#' }
#'
#' @export
#'
readDataFrame <- function(filepath) {
  return(deserialize_v2(readFile(filepath)))
}

#' Deserializes the given vector of raw bytes and returns a data.frame object.
#'
#' The raw vector to be deserialized must represent a Raven DataFrame. That
#' DataFrame is returned as an R data.frame object.
#'
#' @param bytes The vector of raw bytes to deserialize
#'
#' @return A data.frame object from the specified raw vector
#'
#' @details
#' The column types from Raven DataFrames are mapped to the
#' corresponding R types. More specifically, all integer types
#' (byte, short, int, long) are mapped to the R 'integer' type. The floating
#' point types (float, double) are mapped to the R 'double' type. Both string
#' and char types are mapped to the R 'character' type. Booleans are mapped to
#' the R 'logical' type. Binary columns are represented as R 'list' types
#' containing raw vectors.
#'
#' @seealso [raven.rdf::readDataFrame()] for reading DataFrame (.df)
#' files directly.
#'
#' @examples
#' \dontrun{
#' # deserialize a raw vector representing a DataFrame
#' df <- deserializeDataFrame(my.raw.vector)
#'
#' # get the types for all columns
#' types <- sapply(df, typeof)
#' }
#'
#' @export
#'
deserializeDataFrame <- function(bytes) {
  if ((bytes[1] == 0x64) && (bytes[2] == 0x66)) {
    bytes[1] <- as.raw(0x78)
    bytes[2] <- as.raw(0x9c)
    bytes <- memDecompress(bytes, type = "gzip")
  }
  return(deserialize_v2(bytes))
}

readFile <- function(filepath) {
  f <- file(filepath, "rb")
  on.exit(close(f))
  bytes <- readBin(f, what = "raw",
                   n = file.info(filepath)$size, endian = "big")

  bytes[1] <- as.raw(0x78)
  bytes[2] <- as.raw(0x9c)
  return(memDecompress(bytes, type = "gzip"))
}

deserialize_v2 <- function(buffer) {
  #HEADER
  ptr <- 6 # index pointer
  dftype <- buffer[ptr]
  # check implementation encoding number
  if (!is.element(as.integer(dftype), c(100, 110))) {
    stop("Unsupported DataFrame implementation")
  }

  # code of the DataFrame implementation
  impl_default = as.logical(dftype == 0x64)
  ptr <- ptr + 1
  # number of rows
  rows <- readBin(
    buffer[ptr:(ptr+3)],
    what = "integer", n = 4, signed = TRUE, endian = "big")

  ptr <- ptr + 4
  # number of columns
  cols <- readBin(
    buffer[ptr:(ptr+3)],
    what = "integer", n = 4, signed = TRUE, endian = "big")

  ptr <- ptr + 4
  # column labels
  names = character(cols)
  for (i in 1:cols) {
    c0 <- ptr # first char
    while (buffer[ptr] != 0x00) {
      ptr <- ptr + 1
    }
    ptr <- ptr + 1
    names[i] <- rawToChar(buffer[c0:(ptr-2)], multiple = FALSE)
  }

  # column types
  types = integer(cols)
  for (i in 1:cols) {
    types[i] <- as.integer(buffer[ptr])
    ptr <- ptr + 1
  }

  if (cols == 0) { # uninitialized instance
    df <- data.frame()
    return(df)
  }

  if (rows == 0) { # empty instance
    if (impl_default) { # DefaultDataFrame
      return(deserialize_default_empty(buffer, ptr, names, types, cols, rows))
    } else { # NullableDataFrame
      return(deserialize_nullable_empty(buffer, ptr, names, types, cols, rows))
    }
  }

  df <- NULL
  if (impl_default) { # DefaultDataFrame
    if (buffer[ptr] != 0x7d) { # header closing brace '}'
      stop("Invalid format")
    }
    df <- deserialize_default(buffer, ptr, names, types, cols, rows)
  } else {  # NullableDataFrame
    df <- deserialize_nullable(buffer, ptr, names, types, cols, rows)
  }
  return(df)
}

deserialize_default_empty <- function(buffer, ptr, names, types, cols, rows) {
  df <- data.frame()
  for (i in 1:cols) {
    if (types[i] == 1) { # ByteColumn
      val <- integer(rows)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 2) { # ShortColumn
      val <- integer(rows)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 3) { # IntColumn
      val <- integer(rows)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 4) { # LongColumn
      val <- integer(rows)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 5) { # StringColumn
      val <- character(rows)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 6) { # FloatColumn
      val <- double(rows)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 7) { # DoubleColumn
      val <- double(rows)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 8) { # CharColumn
      val <- character(rows)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 9) { # BooleanColumn
      val <- logical(rows)
      logicLength <- as.integer(ifelse(rows %% 8 == 0, rows/8, (rows/8) + 1))
      ptr <- ptr + 1
      bits <- buffer[ptr:(ptr+logicLength-1)]
      ptr <- ptr + (logicLength-1)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 19) { # BinaryColumn
      val <- list()
      df <- addColumn(df, names[i], I(val))
    } else {
      stop("Unknown column type code")
    }
  }
  return(df)
}

deserialize_nullable_empty <- function(buffer, ptr, names, types, cols, rows) {
  # first read the entire lookup list into memory
  lookupLength <- readBin(
    buffer[ptr:(ptr+3)],
    what = "integer", size = 4, n = 4, signed = TRUE, endian = "big")

  ptr <- ptr + 4
  lookupBits <- buffer[ptr:(ptr+lookupLength-1)]

  # list index pointing to the next readable bit
  # within the lookup list (zero indexed)
  li <- 0
  ptr <- ptr + lookupLength
  if (buffer[ptr] != 0x7d) { # header closing brace '}' missing
    stop("Invalid format")
  }

  df <- data.frame()
  for (i in 1:cols) {
    if (types[i] == 10) { # NullableByteColumn
      val <- integer(rows)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 11) { # NullableShortColumn
      val <- integer(rows)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 12) { # NullableIntColumn
      val <- integer(rows)
      df <- addColumn(df, names[i], val)
    } else if(types[i] == 13) { # NullableLongColumn
      val <- integer(rows)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 14) { # NullableStringColumn
      val <- character(rows)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 15) { # NullableFloatColumn
      val <- double(rows)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 16) { # NullableDoubleColumn
      val <- double(rows)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 17) { # NullableCharColumn
      val <- character(rows)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 18) { # NullableBooleanColumn
      val <- logical(rows)
      logicLength <- as.integer(ifelse(rows %% 8 == 0, rows/8, (rows/8) + 1))
      ptr <- ptr + 1
      bits <- buffer[ptr:(ptr+logicLength-1)]
      ptr <- ptr + (logicLength-1)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 20) { # NullableBinaryColumn
      val <- list()
      df <- addColumn(df, names[i], I(val))
    } else {
      stop("Unknown column type code")
    }
  }
  return(df)
}

deserialize_default <- function(buffer, ptr, names, types, cols, rows) {
  df <- data.frame()
  for (i in 1:cols) {
    if (types[i] == 1) { # ByteColumn
      val <- integer(rows)
      for (j in 1:rows) {
        ptr <- ptr + 1
        val[j] <- readBin(
          buffer[ptr:(ptr+1)],
          what = "integer", size = 1, n = 1, signed = TRUE, endian = "big")
      }
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 2) { # ShortColumn
      val <- integer(rows)
      for (j in 1:rows) {
        ptr <- ptr + 2
        val[j] <- readBin(
          buffer[(ptr-1):(ptr+1)],
          what = "integer", size = 2, n = 2, signed = TRUE, endian = "big")
      }
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 3) { # IntColumn
      val <- integer(rows)
      for (j in 1:rows) {
        ptr <- ptr + 4
        val[j] <- readBin(
          buffer[(ptr-3):(ptr+1)],
          what = "integer", size = 4, n = 4, signed = TRUE, endian = "big")
      }
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 4) { # LongColumn
      val <- integer(rows)
      for (j in 1:rows) {
        ptr <- ptr + 8
        val[j] <- readBin(
          buffer[(ptr-7):ptr],
          what = "integer", size = 8, n = 8, signed = TRUE, endian = "big")
      }
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 5) { # StringColumn
      val <- character(rows)
      for (j in 1:rows) {
        ptr <- ptr + 1
        c0 <- ptr
        while (buffer[ptr] != 0x00) {
          ptr <- ptr + 1
        }
        if ((ptr - c0) == 0) {
          val[j] <- "N/A"
        } else {
          val[j] <- rawToChar(buffer[c0:ptr], multiple = FALSE)
        }
      }
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 6) { # FloatColumn
      val <- double(rows)
      for (j in 1:rows) {
        ptr <- ptr + 4
        val[j] <- readBin(
          buffer[(ptr-3):(ptr+1)],
          what = "double", size = 4, n = 4, endian = "big")
      }
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 7) { # DoubleColumn
      val <- double(rows)
      for (j in 1:rows) {
        ptr <- ptr + 8
        val[j] <- readBin(
          buffer[(ptr-7):(ptr+1)],
          what = "double", size = 8, n = 8, endian = "big")
      }
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 8) { # CharColumn
      val <- character(rows)
      for (j in 1:rows) {
        ptr <- ptr + 1
        val[j] <- rawToChar(buffer[ptr], multiple = FALSE)
      }
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 9) { # BooleanColumn
      val <- logical(rows)
      logicLength <- as.integer(ifelse(rows %% 8 == 0, rows/8, (rows/8) + 1))
      ptr <- ptr + 1
      bits <- buffer[ptr:(ptr+logicLength-1)]
      for (j in 1:rows) {
        val[j] <- as.integer(
          bits[((j-1) %/% 8) + 1]
            & as.raw((bitwShiftL(1, (7-((j-1) %% 8)))))) != 0
      }
      ptr <- ptr + (logicLength-1)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 19) { # BinaryColumn
      val <- list(raw(rows))
      for (j in 1:rows) {
        ptr <- ptr + 1
        binLength <- readBin(
          buffer[ptr:(ptr+3)],
          what = "integer", size = 4, n = 4, endian = "big")

        ptr <- ptr + 3
        val[j] <- list(writeBin(
          buffer[(ptr+1):(ptr+binLength)], raw(), endian = "big"))

        ptr <- ptr + binLength
      }
      df <- addColumn(df, names[i], I(val))
    } else {
      stop("Unknown column type code")
    }
  }
  return(df)
}

deserialize_nullable <- function(buffer, ptr, names, types, cols, rows) {
  # first read the entire lookup list into memory
  lookupLength <- readBin(
    buffer[ptr:(ptr+3)],
    what = "integer", size = 4, n = 4, signed = TRUE, endian = "big")

  ptr <- ptr + 4
  lookupBits <- buffer[ptr:(ptr+lookupLength-1)]

  # list index pointing to the next readable bit
  # within the lookup list (zero indexed)
  li <- 0
  ptr <- ptr + lookupLength
  if (buffer[ptr] != 0x7d) { # header closing brace '}' missing
    stop("Invalid format")
  }

  df <- data.frame()
  for (i in 1:cols) {
    if (types[i] == 10) { # NullableByteColumn
      val <- integer(rows)
      for (j in 1:rows) {
        ptr <- ptr + 1
        entry <- readBin(
          buffer[ptr],
          what = "integer", size = 1, n = 1, signed = TRUE, endian = "big")

        if (entry == 0) {
          if (!(lookupBits[as.integer((li %/% 8) + 1)]
                & as.raw(bitwShiftL(1, (7-(li %% 8))))) != 0) {

            val[j] <- 0L
          } else {
            val[j] <- NA
          }
          li <- li + 1
        } else {
          val[j] <- entry
        }
      }
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 11) { # NullableShortColumn
      val <- integer(rows)
      for (j in 1:rows) {
        ptr <- ptr + 2
        entry <- readBin(
          buffer[(ptr-1):ptr],
          what = "integer", size = 2, n = 2, signed = TRUE, endian = "big")

        if (entry == 0) {
          if (!(lookupBits[as.integer((li %/% 8) + 1)]
                & as.raw(bitwShiftL(1, (7-(li %% 8))))) != 0) {

            val[j] <- 0L
          } else {
            val[j] <- NA
          }
          li <- li + 1
        } else {
          val[j] <- entry
        }
      }
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 12) { # NullableIntColumn
      val <- integer(rows)
      for (j in 1:rows) {
        ptr <- ptr + 4
        entry <- readBin(
          buffer[(ptr-3):ptr],
          what = "integer", size = 4, n = 4, signed = TRUE, endian = "big")

        if (entry == 0) {
          if (!(lookupBits[as.integer((li %/% 8) + 1)]
                & as.raw(bitwShiftL(1, (7-(li %% 8))))) != 0) {

            val[j] <- 0L
          } else {
            val[j] <- NA
          }
          li <- li + 1
        } else {
          val[j] <- entry
        }
      }
      df <- addColumn(df, names[i], val)
    } else if(types[i] == 13) { # NullableLongColumn
      val <- integer(rows)
      for (j in 1:rows) {
        ptr <- ptr + 8
        entry <- readBin(
          buffer[(ptr-7):ptr],
          what = "integer", size = 8, n = 8, signed = TRUE, endian = "big")

        if (entry == 0) {
          if (!(lookupBits[as.integer((li %/% 8) + 1)]
                & as.raw(bitwShiftL(1, (7-(li %% 8))))) != 0) {

            val[j] <- 0L
          } else {
            val[j] <- NA
          }
          li <- li + 1
        } else {
          val[j] <- entry
        }
      }
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 14) { # NullableStringColumn
      val <- character(rows)
      for (j in 1:rows) {
        ptr <- ptr + 1
        c0 <- ptr
        while (buffer[ptr] != 0x00) {
          ptr <- ptr + 1
        }
        if ((ptr - c0) == 0) {
          if (!(lookupBits[as.integer((li %/% 8) + 1)]
                & as.raw(bitwShiftL(1, (7-(li %% 8))))) != 0) {

            val[j] <- ""
          } else {
            val[j] <- NA
          }
          li <- li + 1
        } else {
          val[j] <- rawToChar(buffer[c0:ptr], multiple = FALSE)
        }
      }
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 15) { # NullableFloatColumn
      val <- double(rows)
      for (j in 1:rows) {
        ptr <- ptr + 4
        entry <- readBin(
          buffer[(ptr-3):ptr],
          what = "double", size = 4, n = 4, endian = "big")

        if (entry == 0.0) {
          if (!(lookupBits[as.integer((li %/% 8) + 1)]
                & as.raw(bitwShiftL(1, (7-(li %% 8))))) != 0) {

            val[j] <- 0.0
          } else {
            val[j] <- NA
          }
          li <- li + 1
        } else {
          val[j] <- entry
        }
      }
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 16) { # NullableDoubleColumn
      val <- double(rows)
      for (j in 1:rows) {
        ptr <- ptr + 8
        entry <- readBin(
          buffer[(ptr-7):ptr],
          what = "double", size = 8, n = 8, endian = "big")

        if (entry == 0.0) {
          if (!(lookupBits[as.integer((li %/% 8) + 1)]
                & as.raw(bitwShiftL(1, (7-(li %% 8))))) != 0) {

            val[j] <- 0.0
          } else {
            val[j] <- NA
          }
          li <- li + 1
        } else {
          val[j] <- entry
        }
      }
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 17) { # NullableCharColumn
      val <- character(rows)
      for (j in 1:rows) {
        ptr <- ptr + 1
        entry <- buffer[ptr]
        if (entry == 0) {
          val[j] <- NA
        } else {
          val[j] <- rawToChar(entry, multiple = FALSE)
        }
      }
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 18) { # NullableBooleanColumn
      val <- logical(rows)
      logicLength <- as.integer(ifelse(rows %% 8 == 0, rows/8, (rows/8) + 1))
      ptr <- ptr + 1
      bits <- buffer[ptr:(ptr+logicLength-1)]
      for (j in 1:rows) {
        entry <- as.integer(bits[((j-1) %/% 8) + 1]
                            & as.raw((bitwShiftL(1, (7-((j-1) %% 8)))))) != 0

        if (!entry) {
          if (!(lookupBits[as.integer((li %/% 8) + 1)]
                & as.raw(bitwShiftL(1, (7-(li %% 8))))) != 0) {

            val[j] <- FALSE
          } else {
            val[j] <- NA
          }
          li <- li + 1
        } else {
          val[j] <- entry
        }
      }
      ptr <- ptr + (logicLength-1)
      df <- addColumn(df, names[i], val)
    } else if (types[i] == 20) { # NullableBinaryColumn
      val <- list(raw(rows))
      for (j in 1:rows) {
        ptr <- ptr + 1
        binLength <- readBin(
          buffer[ptr:(ptr+3)],
          what = "integer", size = 4, n = 4, endian = "big")

        ptr <- ptr + 3
        if (binLength != 0) {
          val[j] <- list(writeBin(
            buffer[(ptr+1):(ptr+binLength)], raw(), endian = "big"))

        } else {
          val[j] <- NA
        }
        ptr <- ptr + binLength
      }
      df <- addColumn(df, names[i], I(val))
    } else {
      stop("Unknown column type code")
    }
  }
  return(df)
}

addColumn <- function(df, name, values) {
  if (ncol(df) == 0) { # is first column
    df <- data.frame(values, stringsAsFactors = FALSE)
    colnames(df) <- c(name)
  } else { # add column to the end
    names <- colnames(df)
    df[[ncol(df) + 1]] <- values
    colnames(df) <- c(names, name)
  }
  return(df)
}
