
<!-- README.md is generated from README.Rmd. Please edit that file -->

# OmopDatasets

<!-- badges: start -->

[![R-CMD-check](https://github.com/oxford-pharmacoepi/OmopDatasets/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/oxford-pharmacoepi/OmopDatasets/actions/workflows/R-CMD-check.yaml)
[![CRAN
status](https://www.r-pkg.org/badges/version/OmopDatasets)](https://CRAN.R-project.org/package=OmopDatasets)
<!-- badges: end -->

The goal of OmopDatasets is an R packages that allowa you to
**download**, **export** and **connect** to synthetic sample datasets
mapped to the Observational Medical Outcomes Partnership (OMOP) Common
Data Model (CDM).

## Installation

You can install `OmopDatasets` from cran:

``` r
install.packages("OmopDatasets")
```

Or you can install the development version of `OmopDatasets` from
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
library(OmopDatasets)
availableDatasets()
#>  [1] "GiBleed"                             "empty_cdm"                          
#>  [3] "synpuf-1k_5.3"                       "synpuf-1k_5.4"                      
#>  [5] "synthea-allergies-10k"               "synthea-anemia-10k"                 
#>  [7] "synthea-breast_cancer-10k"           "synthea-contraceptives-10k"         
#>  [9] "synthea-covid19-10k"                 "synthea-covid19-200k"               
#> [11] "synthea-dermatitis-10k"              "synthea-heart-10k"                  
#> [13] "synthea-hiv-10k"                     "synthea-lung_cancer-10k"            
#> [15] "synthea-medications-10k"             "synthea-metabolic_syndrome-10k"     
#> [17] "synthea-opioid_addiction-10k"        "synthea-rheumatoid_arthritis-10k"   
#> [19] "synthea-snf-10k"                     "synthea-surgery-10k"                
#> [21] "synthea-total_joint_replacement-10k" "synthea-veteran_prostate_cancer-10k"
#> [23] "synthea-veterans-10k"                "synthea-weight_loss-10k"
```

2.  Download the dataset that you are interested on:

``` r
downloadDataset(datasetName = "GiBleed")
```

3.  Get your cdm_reference object (see
    [omopgenerics](https://darwin-eu.github.io/omopgenerics/)) from the
    dataset:

``` r
cdm <- cdmFromDataset(datasetName = "GiBleed")
#> ℹ Deleting prior `METADATA` file.
#> ℹ Deleting prior `duckdb` file.
#> Note: method with signature 'DBIConnection#Id' chosen for function 'dbExistsTable',
#>  target signature 'duckdb_connection#Id'.
#>  "duckdb_connection#ANY" would also be valid
cdm
#> 
#> ── # OMOP CDM reference (duckdb) of GiBleed ────────────────────────────────────
#> • omop tables: person, observation_period, visit_occurrence, visit_detail,
#> condition_occurrence, drug_exposure, procedure_occurrence, device_exposure,
#> measurement, observation, death, note, note_nlp, specimen, fact_relationship,
#> location, care_site, provider, payer_plan_period, cost, drug_era, dose_era,
#> condition_era, metadata, cdm_source, concept, vocabulary, domain,
#> concept_class, concept_relationship, relationship, concept_synonym,
#> concept_ancestor, source_to_concept_map, drug_strength
#> • cohort tables: -
#> • achilles tables: -
#> • other tables: -
```

This function creates a *temporary* cdm_reference, if you want to export
the database to a duckdb container you can use instead:

``` r
pathToExport <- file.path(tempdir(), "my_db")
dir.create(pathToExport)
exportDatasetToDuckdb(path = pathToExport, datasetName = "GiBleed")
```

You can then create a cdm_reference object using
[CDMConnector](https://darwin-eu.github.io/CDMConnector/):

``` r
library(duckdb)
#> Loading required package: DBI
library(CDMConnector)

con <- dbConnect(drv = duckdb(dbdir = file.path(pathToExport, "GiBleed.duckdb")))
cdm <- cdmFromCon(con = con, cdmSchema = "main", writeSchema = "results")
```
