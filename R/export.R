
cdmFromDataset <- function(datasetName = "GiBleed",
                           cdmSchema = "main",
                           cdmPrefix = "",
                           writeSchema = "main",
                           writePrefix = "") {

}

connectionDetailsFromDataset <- function(datasetName = "GiBleed",
                                         format = "duckdb",
                                         cdmSchema = NULL,
                                         writeSchema = NULL) {

}

#' Export a dataset
#'
#' @param path
#' @param datasetName
#' @param format
#'
#' @return
#' @export
#'
#' @examples
exportDatasetToFiles <- function(path,
                                 datasetName = "GiBleed",
                                 format = "csv") {
  # initial checks
  path <- validatePath(path)
  datasetName <- validateDatasetName(datasetName)
  omopgenerics::assertChoice(format, c("csv", "parquet"), length = 1)

  # check dataset availability
  datasetPath <- datasetAvailable(datasetName)

  # metadata
  nm <- OmopDatasets::omopDatasets$cdm_name[OmopDatasets::omopDatasets$dataset_name == datasetName]
  vr <- OmopDatasets::omopDatasets$cdm_versionwrite[OmopDatasets::omopDatasets$dataset_name == datasetName]
  metadata <- list(datasetName = datasetName, cdmName = nm, cdmVersion = vr)
  metadataFile <- file.path(path, "METADATA")
  if (file.exists(metadataFile)) {
    cli::cli_inform(c("i" = "Deleting prior `METADATA` file."))
    file.remove(metadataFile)
  }
  write.dcf(x = metadata, file = metadataFile)

  # files
  uniqueName <- omopgenerics::uniqueId(exclude = list.files(tempdir()))
  tempFolder <- file.path(tempdir(), uniqueName)
  dir.create(tempFolder)
  utils::unzip(zipfile = datasetPath, exdir = tempFolder)
  if (format == "parquet") {
    list.files(path = tempFolder, full.names = TRUE) |>
      purrr::keep(\(x) endsWith(x = x, suffix = ".parquet")) |>
      purrr::map(\(x) file.copy(from = x, to = file.path(path, basename(x)))) |>
      invisible()
  } else if (format == "csv") {
    if (rlang::is_installed("arrow") & !rlang::is_installed("duckdb")) {
      mode <- "arrow"
    } else {
      rlang::check_installed("duckdb")
      mode <- "duckdb"
    }
    if (mode == "duckdb") {
      con <- duckdb::dbConnect(duckdb::duckdb())
      on.exit(duckdb::dbDisconnect(conn = con))
    }
    csvFiles <- list.files(path = tempFolder, full.names = TRUE) |>
      purrr::keep(\(x) endsWith(x = x, suffix = ".parquet")) |>
      purrr::map(\(x) {
        nm <- paste0(substr(basename(x), 1, nchar(x) - 8), ".csv")
        csvFile <- file.path(path, nm)
        if (mode == "arrow") {
          x <- arrow::read_parquet(x)
        } else if (mode == "duckdb") {
          x <- DBI::dbGetQuery(con, glue::glue("SELECT * FROM 'x'")) |>
            dplyr::collect()
        }
        write.csv(x = x, file = csvFile, row.names = FALSE)
        csvFile
      }) |>
      purrr::flatten_chr()
  }
  unlink(tempFolder, recursive = TRUE)

  invisible(c(metadataFile, csvFiles))
}

#' Title
#'
#' @param path
#' @param datasetName
#' @param cdmSchema
#' @param cdmPrefix
#' @param writeSchema
#' @param writePrefix
#'
#' @return
#' @export
#'
#' @examples
exportDatasetToDuckdb <- function(path,
                                  datasetName = "GiBleed",
                                  cdmSchema = "main",
                                  cdmPrefix = "",
                                  writeSchema = "main",
                                  writePrefix = "") {
  rlang::check_installed("duckdb")

  # initial checks
  path <- validatePath(path)
  datasetName <- validateDatasetName(datasetName)
  omopgenerics::assertCharacter(cdmSchema, length = 1)
  omopgenerics::assertCharacter(cdmPrefix, length = 1)
  omopgenerics::assertCharacter(writeSchema, length = 1)
  omopgenerics::assertCharacter(writePrefix, length = 1)

  # check dataset availability
  datasetPath <- datasetAvailable(datasetName)

  # metadata
  nm <- OmopDatasets::omopDatasets$cdm_name[OmopDatasets::omopDatasets$dataset_name == datasetName]
  vr <- OmopDatasets::omopDatasets$cdm_versionwrite[OmopDatasets::omopDatasets$dataset_name == datasetName]
  metadata <- list(
    datasetName = datasetName,
    dbms = "duckdb",
    cdmName = nm,
    cdmVersion = vr,
    cdmSchema = cdmSchema,
    cdmPrefix = cdmPrefix,
    writeSchema = writeSchema,
    writePrefix = writePrefix
  )
  metadataFile <- file.path(path, "METADATA")
  if (file.exists(metadataFile)) {
    cli::cli_inform(c("i" = "Deleting prior `METADATA` file."))
    file.remove(metadataFile)
  }
  write.dcf(x = metadata, file = metadataFile)

  # empty db
  dbPath <- file.path(path, paste0(datasetName, ".duckdb"))
  con <- duckdb::dbConnect(duckdb::duckdb(dbdir = dbPath))
  on.exit(DBI::dbDisconnect(conn = con))

  # schemas
  createSchema(con = con, schema = cdmSchema)
  createSchema(con = con, schema = writeSchema)

  # copy tables
  uniqueName <- omopgenerics::uniqueId(exclude = list.files(tempdir()))
  tempFolder <- file.path(tempdir(), uniqueName)
  dir.create(tempFolder)
  utils::unzip(zipfile = datasetPath, exdir = tempFolder)
  list.files(path = tempFolder, full.names = TRUE) |>
    purrr::keep(\(x) endsWith(x = x, suffix = ".parquet")) |>
    purrr::map(\(x) {
      nm <- substr(basename(x), 1, nchar(x) - 8)
      sql <- "CREATE TABLE {cdmSchema}.{cdmPrefix}{nm} AS SELECT * FROM read_parquet('{x}')" |>
        glue::glue()
      DBI::dbExecute(conn = con, statment = sql)
    }) |>
    invisible()
  unlink(tempFolder, recursive = TRUE)

  invisible(c(metadataFile, dbPath))
}
createSchema <- function(con, schema) {
  query <- glue::glue("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema}'")
  result <- DBI::dbGetQuery(conn = con, statement = query)
  if (nrow(result) == 0) {
    DBI::dbExecute(conn = con, statement = glue::glue("CREATE SCHEMA {schema}"))
  }
  invisible(con)
}

cdmFromMetadata <- function(path) {

}

connectionDetailsFromMetadata <- function(path) {

}

datasetAvailable <- function(datasetName, call = parent.frame()) {
  folder <- datasetsFolder()
  if (!isDatasetDownloaded(datasetName = datasetName, path = folder)) {
    if (question(paste0("`", datasetName, "` is not downloaded, do you wnat to download it?"))) {
      downloadDataset(datasetName = datasetName, path = folder)
    } else {
      cli::cli_abort(c(x = "`{datasetName}` is not downloaded."), call = call)
    }
  }
  file.path(folder, datasetName, paste0(datasetName, ".zip"))
}
