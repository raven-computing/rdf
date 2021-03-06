
<!-- README.md is generated from README.Rmd. Please edit that file -->

# Rdf: An R Interface for Raven DataFrames

<!-- badges: start -->

[![cran](https://www.r-pkg.org/badges/version/raven.rdf)](https://cran.r-project.org/package=raven.rdf)
[![release](https://img.shields.io/badge/Release-Beta-blue.svg)](https://cran.r-project.org/package=raven.rdf)
<!-- badges: end -->

This is an R package which provides an I/O interface between R
data.frames and Raven DataFrames. The functions provided by this package
allow you to both read and write DataFrame files (.df), as well as
serialize/deserialize DataFrames to/from raw vectors.

## Getting Started

You can install the released version of rdf from
[CRAN](https://cran.r-project.org/package=raven.rdf) with:

``` r
install.packages("raven.rdf")
```

After installation you can easily read DataFrame files (.df) into R
data.frames and persist R data.frame instances to DataFrame files.

Make sure that the library package is loaded:

``` r
library("raven.rdf")
```

Then read a DataFrame file into memory with:

``` r
df <- readDataFrame("path/to/my/file.df")
```

The *df* variable is a regular R data.frame so you can use it like any
other such instance. You could then do some computations, change values
inside the data.frame and then write it back to the file as a Raven
DataFrame with:

``` r
writeDataFrame("path/to/my/file.df", df)
```

For more options, please see the documentation section below.

## Compatibility

The rdf package requires **R 3.5.0** or higher.

This library does not have any external dependencies.

## Documentation

See the [developer
documentation](https://github.com/raven-computing/rdf/wiki) for API
usage instructions and examples.

See the R [reference
manual](https://cran.r-project.org/web/packages/raven.rdf/raven.rdf.pdf)
for the API documentation.

For more information on DataFrames as used by Raven Computing, please
see the corresponding [developer
documentation](https://www.raven-computing.com/docs/dataframe/).

## Development

If you want to change code of this library or install it manually
without downloading from CRAN, you can do so by cloning this repository.

### Setup

The R *devtools* are required in a development environment. The below
instructions assume that devtools are loaded within the underlying
development environment. We are using *RStudio* as our standard IDE for
R projects. The following instructions show how to achieve things on a
command line.

### Loading Library Functions

When in the project root directory, you may load the library code with:

``` r
load_all()
```

This will temporarily install the rdf library in a non-global workspace.
You can then call all functions provided by the rdf library directly.

### Running Tests

Execute all unit tests with:

``` r
test()
```

### Build Documentation

Build the documentation with:

``` r
document()
```

The above command will build the documentation in the *man* directory
with *roxygen2*.

### Perform Checks

If you want to install rdf from source, it is highly recommended to run
the automated checks before installing:

``` r
check()
```

### Install from Source

If all checks have passed, you can install rdf from source with:

``` r
install()
```

You can then attach the rdf package in your projects like any other
package.

### Build

You can build a distribution file with:

``` r
build()
```

The built file can then be distributed manually.

## License

This library is licensed under the Apache License Version 2 - see the
[LICENSE](LICENSE) for details.
