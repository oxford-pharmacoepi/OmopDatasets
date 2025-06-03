test_that("test export functions", {

  # download GiBleed
  dbName <- "GiBleed"
  datasetsFolder(path = tempdir())
  downloadDataset(datasetName = dbName)

  expect_no_error(cdm <- cdmFromDataset(datasetName = dbName))
  expect_no_error(omopgenerics::validateCdmArgument(cdm))

  expect_no_error(cdm <- cdmFromDataset(
    datasetName = dbName, cdmSchema = "test", writeSchema = "results",
    writePrefix = "mc_"
  ))
  expect_no_error(src <- omopgenerics::cdmSource(cdm))
  expect_true("db_cdm" %in% class(src))
  expect_identical(attr(src, "write_schema"), c(schema = "results", prefix = "mc_"))
  schemas <- dplyr::tbl(CDMConnector::cdmCon(cdm), I("information_schema.schemata")) |>
    dplyr::collect()
  expect_true(all(c("results", "test") %in% schemas$schema_name))

  td <- file.path(tempdir(), "export")
  dir.create(td, showWarnings = FALSE)
  expect_no_error(exportDatasetToFiles(path = td, datasetName = dbName, format = "csv"))
  expect_true("person.csv" %in% list.files(td))
  expect_true("METADATA" %in% list.files(td))
  expect_no_error(md <- readMetadata(file.path(td, "METADATA")))
  expect_identical(md$type, "files")
  expect_identical(md$format, "csv")

  exportDatasetToDuckdb

  cdmFromMetadata

})
