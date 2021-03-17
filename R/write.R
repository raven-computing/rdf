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

#' Writes the specified data.frame to the specified file.
#'
#' The R data.frame is persisted as a DataFrame (.df) file. The concrete column
#' types to use for each individual data.frame column can be specified by
#' the 'types' argument.
#'
#' @param filepath The path to the file to write
#' @param df The data.frame object to write
#' @param types The type names for all column types. Must be a vector of
#'              character values. May be NULL
#' @param as.nullable A logical indicating whether the data.frame should
#' be persisted as a NullableDataFrame, even if it contains no NA values
#'
#' @return The number of bytes written to the specified file
#'
#' @details
#' The column types of the R data.frame object are mapped to the corresponding
#' Raven DataFrame column types. The following types exist:
#'
#' \tabular{ll}{
#'   \strong{Type name} \tab \strong{Description} \cr
#'   byte \tab int8 \cr
#'   short \tab int16 \cr
#'   int \tab int32 \cr
#'   long \tab int64 \cr
#'   float \tab float32 \cr
#'   double \tab float64 \cr
#'   string \tab UTF-8 encoded unicode string \cr
#'   char \tab single printable ASCII character \cr
#'   boolean \tab logical value TRUE or FALSE \cr
#'   binary \tab arbitrary length byte array \cr
#' }
#'
#' By default, if the 'types' argument is not explicitly specified, all values
#' are mapped to the corresponding largest possible type in order to avoid
#' possible loss of information. However, users can specify the concrete type
#' for each column in the DataFrame file to be written. This is done by
#' providing a vector of character values denoting the type name of each
#' corresponding data.frame column. The index of each entry corresponds to the
#' index of the column in the underlying data.frame to persist.
#'
#' If the specified data.frame object contains at least one NA value, then the
#' DataFrame file to be persisted will represent a NullableDataFrame. If the
#' data.frame contains no NA values, then the DataFrame file to be persisted
#' will represent a DefaultDataFrame, unless the 'as.nullable' argument is
#' set to TRUE.
#'
#' @seealso
#' [raven.rdf::serializeDataFrame()] for serializing data.frame objects
#' to vectors of raw bytes. \cr
#' [raven.rdf::readDataFrame()] for reading DataFrame files which have been
#' previously persisted by this function.
#'
#' @examples
#' \dontrun{
#' # get a data.frame
#' df <- cars
#' # write the data.frame to a .df file
#' writeDataFrame("cars.df", df)
#'
#' # specify the concrete types of all columns
#' coltypes <- c("float", "double")
#' # write the data.frame to a .df file with concrete types
#' writeDataFrame("cars.df", df, types = coltypes)
#' }
#'
#' @export
#'
writeDataFrame <- function(filepath, df, types = NULL, as.nullable = FALSE) {
  if (is.null(types)) {
    typecodes <- mapTypeCodesR(sapply(df, typeof))
  } else {
    if (length(types) != ncol(df)) {
      stop(paste("length of specified type name vector does not",
                 "match number of DataFrame columns"))
    }
    typecodes <- mapTypeNamesToTypeCodes(types)
  }
  return(writeFile(filepath, serialize_v2(df, typecodes, as.nullable)))
}

#' Serializes the specified data.frame object to a vector of raw bytes.
#'
#' The R data.frame is serialized as a Raven DataFrame. The concrete column
#' types to use for each individual data.frame column can be specified by
#' the 'types' argument.
#'
#' @param df The data.frame object to serialize
#' @param types The type names for all column types. Must be a vector of
#'              character values. May be NULL
#' @param compress A logical indicating whether to compress the content
#' of the returned raw vector
#' @param as.nullable A logical indicating whether the data.frame should
#' be serialized as a NullableDataFrame, even if it contains no NA values
#'
#' @return A raw vector representing the serialized date.frame object
#'
#' @details
#' The column types of the R data.frame object are mapped to the corresponding
#' Raven DataFrame column types. The following types exist:
#'
#' \tabular{ll}{
#'   \strong{Type name} \tab \strong{Description} \cr
#'   byte \tab int8 \cr
#'   short \tab int16 \cr
#'   int \tab int32 \cr
#'   long \tab int64 \cr
#'   float \tab float32 \cr
#'   double \tab float64 \cr
#'   string \tab UTF-8 encoded unicode string \cr
#'   char \tab single printable ASCII character \cr
#'   boolean \tab logical value TRUE or FALSE \cr
#'   binary \tab arbitrary length byte array \cr
#' }
#'
#' By default, if the 'types' argument is not explicitly specified, all values
#' are mapped to the corresponding largest possible type in order to avoid
#' possible loss of information. However, users can specify the concrete type
#' for each column in the DataFrame file to be written. This is done by
#' providing a vector of character values denoting the type name of each
#' corresponding data.frame column. The index of each entry corresponds to the
#' index of the column in the underlying data.frame to persist.
#'
#' If the specified data.frame object contains at least one NA value, then the
#' serialized DataFrame will represent a NullableDataFrame. If the data.frame
#' contains no NA values, then the serialized DataFrame will represent a
#' DefaultDataFrame, unless the 'as.nullable' argument is set to TRUE.
#'
#' The logical 'compress' argument specifies whether the serialized DataFrame
#' is compressed.
#'
#' @seealso
#' [raven.rdf::writeDataFrame()] for directly persisting data.frame objects
#' to the file system
#'
#' @examples
#' \dontrun{
#' # get a data.frame
#' df <- cars
#' # serialize the data.frame to a raw vector
#' vec <- serializeDataFrame(df)
#'
#' # specify the concrete types of all columns
#' coltypes <- c("float", "double")
#' # serialize the data.frame to a raw vector with concrete types
#' serializeDataFrame(df, types = coltypes)
#' }
#'
#' @export
#'
serializeDataFrame <- function(df, types = NULL, compress = FALSE,
                               as.nullable = FALSE) {

  if (ncol(df) == 0) {
    return(createUninitialized(as.nullable, compress))
  }
  if (is.null(types)) {
    typecodes <- mapTypeCodesR(sapply(df, typeof))
  } else {
    if (length(types) != ncol(df)) {
      stop(paste("length of specified type name vector does not match",
                 "number of DataFrame columns"))

    }
    typecodes <- mapTypeNamesToTypeCodes(types)
  }
  bytes <- NULL
  if (nrow(df) == 0) {
    bytes <- createEmpty(df, typecodes, as.nullable)
  } else {
    bytes <- serialize_v2(df, typecodes, as.nullable)
  }
  if (compress) {
    bytes <- memCompress(bytes, type = "gzip")
    bytes[1] <- as.raw(0x64)
    bytes[2] <- as.raw(0x66)
  }
  return(bytes)
}

writeFile <- function(filepath, bytes) {
  bytes <- memCompress(bytes, type = "gzip")
  bytes[1] <- as.raw(0x64)
  bytes[2] <- as.raw(0x66)
  f <- file(filepath, "wb")
  on.exit(close(f))
  writeBin(bytes, f, endian = "big")
  flush(f)
  return(length(bytes))
}

mapTypeCodesR <- function(typenames) {
  len <- length(typenames)
  typecodes <- vector("integer", len)
  for (i in 1:len) {
    if (typenames[i] == "integer") {
      typecodes[i] <- 4
    } else if (typenames[i] == "double") {
      typecodes[i] <- 7
    } else if (typenames[i] == "character") {
      typecodes[i] <- 5
    } else if (typenames[i] == "logical") {
      typecodes[i] <- 9
    } else if (typenames[i] == "list") {
      typecodes[i] <- 19
    } else {
      stop("Invalid column type")
    }
  }
  return(typecodes)
}

mapTypeNamesToTypeCodes <- function(typenames) {
  len <- length(typenames)
  typecodes <- vector("integer", len)
  for (i in 1:len) {
    if (typenames[i] == "byte") {
      typecodes[i] <- 1
    } else if (typenames[i] == "short") {
      typecodes[i] <- 2
    } else if (typenames[i] == "int") {
      typecodes[i] <- 3
    } else if (typenames[i] == "long") {
      typecodes[i] <- 4
    } else if (typenames[i] == "string") {
      typecodes[i] <- 5
    } else if (typenames[i] == "float") {
      typecodes[i] <- 6
    } else if (typenames[i] == "double") {
      typecodes[i] <- 7
    } else if (typenames[i] == "char") {
      typecodes[i] <- 8
    } else if (typenames[i] == "boolean") {
      typecodes[i] <- 9
    } else if (typenames[i] == "binary") {
      typecodes[i] <- 19
    } else {
      stop("Invalid column type")
    }
  }
  return(typecodes)
}

mapTypeCodesToNullable <- function(typecodes) {
  len <- length(typecodes)
  nullableCodes <- vector("integer", len)
  for (i in 1:len) {
    if (typecodes[i] == 1) {
      nullableCodes[i] <- 10
    } else if (typecodes[i] == 2) {
      nullableCodes[i] <- 11
    } else if (typecodes[i] == 3) {
      nullableCodes[i] <- 12
    } else if (typecodes[i] == 4) {
      nullableCodes[i] <- 13
    } else if (typecodes[i] == 5) {
      nullableCodes[i] <- 14
    } else if (typecodes[i] == 6) {
      nullableCodes[i] <- 15
    } else if (typecodes[i] == 7) {
      nullableCodes[i] <- 16
    } else if (typecodes[i] == 8) {
      nullableCodes[i] <- 17
    } else if (typecodes[i] == 9) {
      nullableCodes[i] <- 18
    } else if (typecodes[i] == 19) {
      nullableCodes[i] <- 20
    } else {
      stop("Unknown column type code")
    }
  }
  return(as.raw(nullableCodes))
}

createUninitialized <- function(as.nullable, compress) {
  cols <- 0L
  rows <- 0L
  buffer <- as.raw(c(0x7b, 0x76, 0x3a, 0x32, 0x3b, 0x64,
                     writeBin(
                       as.integer(rows), raw(), size = 4, endian = "big"),
                     writeBin(
                       as.integer(cols), raw(), size = 4, endian = "big")))

  if (as.nullable) {
    buffer <- as.raw(buffer)
    # set DataFrame type to 'nullable'
    buffer[6] <- as.raw(0x6e)
    # set lookup list length to zero
    buffer <- c(buffer, writeBin(0L, raw(), size = 4, endian = "big"))
  }
  buffer <- c(buffer, as.raw(0x7d))

  if (compress) {
    buffer <- memCompress(buffer, type = "gzip")
    buffer[1] <- as.raw(0x64)
    buffer[2] <- as.raw(0x66)
  }
  return(as.raw(buffer))
}

createEmpty <- function(df, typecodes, as.nullable) {
  cols <- ncol(df)
  rows <- 0L
  buffer <- as.raw(c(0x7b, 0x76, 0x3a, 0x32, 0x3b, 0x64,
                     writeBin(
                       as.integer(rows), raw(), size = 4, endian = "big"),
                     writeBin(
                       as.integer(cols), raw(), size = 4, endian = "big")))

  names <- colnames(df)
  for (i in 1:length(names)) {
    buffer <- c(buffer, c(charToRaw(enc2utf8(names[i])), 0x00))
  }

  posTypecodes <- length(buffer) + 1
  buffer <- c(buffer, typecodes)

  if (as.nullable) {
    buffer <- as.raw(buffer)
    # set DataFrame type to 'nullable'
    buffer[6] <- as.raw(0x6e)
    # get the current (default) typecodes
    typecodes <- buffer[posTypecodes:(posTypecodes+length(typecodes)-1)]
    # map the default column typecodes to the corresponding nullable variants
    typecodes <- mapTypeCodesToNullable(typecodes)
    # override the typecodes in the header with the nullable typecodes
    buffer[posTypecodes:(posTypecodes+length(typecodes)-1)] <- typecodes
    # set lookup list length to zero
    buffer <- c(buffer, writeBin(0L, raw(), size = 4, endian = "big"))
  }
  buffer <- c(buffer, as.raw(0x7d))
  return(as.raw(buffer))
}

serialize_v2 <- function(df, typecodes, as.nullable = FALSE) {
  cols <- ncol(df)
  rows <- nrow(df)
  # check for max int for rows and cols
  if (cols > 4294967295) {
    stop("Failed to serialize DataFrame. Column count exceeds supported size")
  }
  if (rows > 4294967295) {
    stop("Failed to serialize DataFrame. Row count exceeds supported size")
  }

  is.nullable <- FALSE
  buffer <- as.raw(c(0x7b, 0x76, 0x3a, 0x32, 0x3b, 0x64,
                     writeBin(
                       as.integer(rows), raw(), size = 4, endian = "big"),
                     writeBin(
                       as.integer(cols), raw(), size = 4, endian = "big")))

  # create a buffer for the lookup list with an initial capacity
  lookupBits <- vector("raw", 256)
  # zero indexed
  lookupBlock <- 0
  lookupIndex <- 0

  names <- colnames(df)
  for (i in 1:length(names)) {
    buffer <- c(buffer, c(charToRaw(enc2utf8(names[i])), 0x00))
  }

  posTypecodes <- length(buffer) + 1
  buffer <- c(buffer, typecodes)

  data <- vector("raw", 0)
  for (i in 1:cols) {
    colbuffer <- vector("raw", 0)
    if (typecodes[i] == 1) { # ByteColumn
      for (j in 1:rows) {
        val <- df[j, i]
        if (is.na(val)) {
          is.nullable <- TRUE
          colbuffer <- c(colbuffer, as.raw(0x00))
          lookupBits[lookupBlock + 1] <- lookupBits[lookupBlock + 1] |
              as.raw((bitwShiftL(1, (7-(lookupIndex %% 8)))))

          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else if (val == 0) {
          colbuffer <- c(colbuffer, as.raw(0x00))
          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else {
          colbuffer <- c(colbuffer,
                         writeBin(as.integer(val),
                                  raw(), size = 1, endian = "big"))

        }
      }
    } else if (typecodes[i] == 2) { # ShortColumn
      for (j in 1:rows) {
        val <- df[j, i]
        if (is.na(val)) {
          is.nullable <- TRUE
          colbuffer <- c(colbuffer, as.raw(c(0x00, 0x00)))
          lookupBits[lookupBlock + 1] <- lookupBits[lookupBlock + 1] |
              as.raw((bitwShiftL(1, (7-(lookupIndex %% 8)))))

          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else if (val == 0) {
          colbuffer <- c(colbuffer, as.raw(c(0x00, 0x00)))
          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else {
          colbuffer <- c(colbuffer,
                         writeBin(as.integer(val),
                                  raw(), size = 2, endian = "big"))

        }
      }
    } else if (typecodes[i] == 3) { # IntColumn
      for (j in 1:rows) {
        val <- df[j, i]
        if (is.na(val)) {
          is.nullable <- TRUE
          colbuffer <- c(colbuffer, as.raw(c(0x00, 0x00, 0x00, 0x00)))
          lookupBits[lookupBlock + 1] <- lookupBits[lookupBlock + 1] |
              as.raw((bitwShiftL(1, (7-(lookupIndex %% 8)))))

          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else if (val == 0) {
          colbuffer <- c(colbuffer, as.raw(c(0x00, 0x00, 0x00, 0x00)))
          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else {
          colbuffer <- c(colbuffer,
                         writeBin(as.integer(val),
                                  raw(), size = 4, endian = "big"))

        }
      }
    } else if (typecodes[i] == 4) { # LongColumn
      for (j in 1:rows) {
        val <- df[j, i]
        if (is.na(val)) {
          is.nullable <- TRUE
          colbuffer <- c(colbuffer, as.raw(c(0x00, 0x00, 0x00, 0x00,
                                             0x00, 0x00, 0x00, 0x00)))

          lookupBits[lookupBlock + 1] <- lookupBits[lookupBlock + 1] |
              as.raw((bitwShiftL(1, (7-(lookupIndex %% 8)))))

          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else if (val == 0) {
          colbuffer <- c(colbuffer, as.raw(c(0x00, 0x00, 0x00, 0x00,
                                             0x00, 0x00, 0x00, 0x00)))

          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else {
          colbuffer <- c(colbuffer,
                         writeBin(as.integer(val),
                                  raw(), size = 8, endian = "big"))

        }
      }
    } else if (typecodes[i] == 5) { # StringColumn
      for (j in 1:rows) {
        val <- df[j, i]
        if (is.na(val)) {
          is.nullable <- TRUE
          lookupBits[lookupBlock + 1] <- lookupBits[lookupBlock + 1] |
              as.raw((bitwShiftL(1, (7-(lookupIndex %% 8)))))

          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else if (nchar(val) == 0) {
          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else {
          colbuffer <- c(colbuffer, charToRaw(enc2utf8(val)))
        }
        # add null character
        colbuffer <- c(colbuffer, as.raw(0x00))
      }
    } else if (typecodes[i] == 6) { # FloatColumn
      for (j in 1:rows) {
        val <- df[j, i]
        if (is.na(val)) {
          is.nullable <- TRUE
          colbuffer <- c(colbuffer, as.raw(c(0x00, 0x00, 0x00, 0x00)))
          lookupBits[lookupBlock + 1] <- lookupBits[lookupBlock + 1] |
              as.raw((bitwShiftL(1, (7-(lookupIndex %% 8)))))

          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else if (val == 0) {
          colbuffer <- c(colbuffer, as.raw(c(0x00, 0x00, 0x00, 0x00)))
          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else {
          colbuffer <- c(colbuffer,
                         writeBin(as.double(val),
                                  raw(), size = 4, endian = "big"))

        }
      }
    } else if (typecodes[i] == 7) { # DoubleColumn
      for (j in 1:rows) {
        val <- df[j, i]
        if (is.na(val)) {
          is.nullable <- TRUE
          colbuffer <- c(colbuffer, as.raw(c(0x00, 0x00, 0x00, 0x00,
                                             0x00, 0x00, 0x00, 0x00)))

          lookupBits[lookupBlock + 1] <- lookupBits[lookupBlock + 1] |
              as.raw((bitwShiftL(1, (7-(lookupIndex %% 8)))))

          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else if (val == 0) {
          colbuffer <- c(colbuffer, as.raw(c(0x00, 0x00, 0x00, 0x00,
                                             0x00, 0x00, 0x00, 0x00)))

          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else {
          colbuffer <- c(colbuffer,
                         writeBin(as.double(val),
                                  raw(), size = 8, endian = "big"))

        }
      }
    } else if (typecodes[i] == 8) { # CharColumn
      for (j in 1:rows) {
        val <- df[j, i]
        if (is.na(val)) {
          is.nullable <- TRUE
          colbuffer <- c(colbuffer, as.raw(0x00))
        } else {
          val <- charToRaw(val)
          if ((val < 32) || (val > 126)) {
            stop("Invalid character value. Only printable ASCII is permitted")
          }
          colbuffer <- c(colbuffer, val)
        }
      }
    } else if (typecodes[i] == 9) { # BooleanColumn
      logicLength <- as.integer(ifelse(rows %% 8 == 0, rows/8, (rows/8) + 1))
      colbuffer <- vector("raw", logicLength)
      bufferIndex <- 0
      bufferBlock <- 0
      for (j in 1:rows) {
        val <- df[j, i]
        if (is.na(val)) {
          is.nullable <- TRUE
          bufferIndex <- bufferIndex + 1
          bufferBlock <- (bufferIndex %/% 8)
          lookupBits[lookupBlock + 1] <- lookupBits[lookupBlock + 1] |
              as.raw((bitwShiftL(1, (7-(lookupIndex %% 8)))))

          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else if (val == FALSE) {
          bufferIndex <- bufferIndex + 1
          bufferBlock <- (bufferIndex %/% 8)
          lookupIndex <- lookupIndex + 1
          lookupBlock <- (lookupIndex %/% 8)
        } else {
          colbuffer[bufferBlock + 1] <- colbuffer[bufferBlock + 1] |
              as.raw((bitwShiftL(1, (7-(bufferIndex %% 8)))))

          bufferIndex <- bufferIndex + 1
          bufferBlock <- (bufferIndex %/% 8)
        }
      }
    } else if (typecodes[i] == 19) { # BinaryColumn
      for (j in 1:rows) {
        val <- df[[j, i]]
        if (!is.raw(val) && is.na(val)) {
          is.nullable <- TRUE
          colbuffer <- c(colbuffer,
                         writeBin(as.integer(0),
                                  raw(), size = 4, endian = "big"))

        } else {
          dataLength <- length(val)
          colbuffer <- c(colbuffer,
                         writeBin(as.integer(dataLength),
                                  raw(), size = 4, endian = "big"), val)

        }
      }
    } else {
      stop("Unknown column type code")
    }
    data <- c(data, colbuffer)
  }

  if (is.nullable || as.nullable) {
    buffer <- as.raw(buffer)
    # set DataFrame type to 'nullable'
    buffer[6] <- as.raw(0x6e)
    # get the current (default) typecodes
    typecodes <- buffer[posTypecodes:(posTypecodes+length(typecodes)-1)]
    # map the default column typecodes to the corresponding nullable variants
    typecodes <- mapTypeCodesToNullable(typecodes)
    # override the typecodes in the header with the nullable typecodes
    buffer[posTypecodes:(posTypecodes+length(typecodes)-1)] <- typecodes
    # add the lookup list bytes to the buffer
    blength = as.integer(((lookupIndex - 1) / 8) + 1)
    ll <- lookupBits[1:blength]
    buffer <- c(buffer, writeBin(blength, raw(), size = 4, endian = "big"))
    buffer <- c(buffer, ll)
  }
  # add a closing brace to the header
  buffer <- c(buffer, as.raw(0x7d))
  # add the payload data
  buffer <- c(buffer, data)
  return(as.raw(buffer))
}
