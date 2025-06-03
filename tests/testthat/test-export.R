test_that("test export functions", {

  # download GiBleed
  dbName <- "GiBleed"
  datasetsFolder(path = tempdir())
  downloadDataset(datasetName = dbName)

  expect_no_error(cdm <- cdmFromDataset(datasetName = dbName))
  expect_no_error(omopgenerics::validateCdmArgument(cdm))

  expect_no_error(cdm <- cdmFromDataset(datasetName = dbName))

  tdf <- file.path(tempdir(), "export")
  dir.create(tdf, showWarnings = FALSE)
  expect_no_error(exportDatasetToFiles(path = tdf, datasetName = dbName, format = "csv"))
  expect_true("person.csv" %in% list.files(tdf))
  expect_true("METADATA" %in% list.files(tdf))
  expect_no_error(md <- readMetadata(file.path(tdf, "METADATA")))
  expect_identical(md$type, "files")
  expect_identical(md$format, "csv")

  expect_no_error(cdm <- cdmFromMetadata(path = tdf))
  expect_no_error(omopgenerics::validateCdmArgument(cdm))

  tdd <- file.path(tempdir(), "export_db")
  dir.create(tdd, showWarnings = FALSE)
  expect_no_error(exportDatasetToDuckdb(path = tdd, datasetName = dbName, cdmSchema = "test", writeSchema = "results"))

  expect_no_error(cdm <- cdmFromMetadata(path = tdd))
  expect_no_error(omopgenerics::validateCdmArgument(cdm))

  expect_no_error(src <- omopgenerics::cdmSource(cdm))
  expect_true("db_cdm" %in% class(src))
  expect_identical(attr(src, "write_schema"), c(schema = "results"))
  schemas <- dplyr::tbl(CDMConnector::cdmCon(cdm), I("information_schema.schemata")) |>
    dplyr::collect()
  expect_true(all(c("results", "test") %in% schemas$schema_name))

})
