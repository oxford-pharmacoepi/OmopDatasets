
<!-- README.md is generated from README.Rmd. Please edit that file -->

# OmopDatasets

<!-- badges: start -->

[![R-CMD-check](https://github.com/oxford-pharmacoepi/OmopDatasets/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/oxford-pharmacoepi/OmopDatasets/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

The goal of OmopDatasets is to …

## Installation

You can install `OmopDatasets` from cran:

``` r
install.packages("OmopDatasets")
```

You can install the development version of `OmopDatasets` from
[GitHub](https://github.com/oxford-pharmacoepi/OmopDatasets) with:

``` r
# install.packages("pak")
pak::pkg_install("oxford-pharmacoepi/OmopDatasets")
```

## Example

This is a simple example how to start working with one of our synthetic
OMOP CDM Datasets:

1.  Choose the dataset you want to use from the list of
    `availableDatasets()`:

``` r
availableDatasets()
```

2.  Download the dataset that you are interested on:

``` r
downloadDataset(datasetName = "GiBleed")
```

### cdm_reference object

3.  Get your cdm_reference object (see
    [omopgenerics](https://darwin-eu.github.io/omopgenerics/)) from the
    dataset:

``` r
cdm <- cdmFromDataset(datasetName = "GiBleed")
cdm
```

### connectionDetails

3.  Get you connectionDetails object (see
    [DatabaseConenctor](https://ohdsi.github.io/DatabaseConnector/))
    from the dataset:

``` r
connectionDetails <- connectionDetailsFromDataset(datasetName = "GiBleed")
connectionDetails
```
