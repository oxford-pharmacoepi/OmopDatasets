
#' Title
#'
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
cdmFromDataset <- function(datasetName = "GiBleed",
                           cdmSchema = "main",
                           cdmPrefix = "",
                           writeSchema = "main",
                           writePrefix = "") {
  rlang::check_installed("duckdb")
  rlang::check_installed("CDMConnector")

  # initial check
  datasetName <- validateDatasetName(datasetName)
  omopgenerics::assertCharacter(cdmSchema, length = 1)
  omopgenerics::assertCharacter(cdmPrefix, length = 1)
  omopgenerics::assertCharacter(writeSchema, length = 1)
  omopgenerics::assertCharacter(writePrefix, length = 1)

  # make dataset avialable
  datasetPath <- dirname(datasetAvailable(datasetName))

  # create duckdb
  files <- exportDatasetToDuckdb(
    path = datasetPath,
    datasetName = datasetName,
    cdmSchema = cdmSchema,
    cdmPrefix = cdmPrefix,
    writeSchema = writeSchema,
    writePrefix = writePrefix
  )

  # metadata
  metadata <- as.list(read.dcf(files[1])[1,])

  # create cdm object
  con <- duckdb::dbConnect(drv = duckdb::duckdb(dbdir = metadata$dbDir))
  wp <- metadata$writePrefix
  if (wp == "") wp <- NULL
  CDMConnector::cdmFromCon(
    con = con,
    cdmSchema = metadata$cdmSchema,
    writeSchema = metadata$writeSchema,
    cdmVersion = metadata$cdmVersion,
    cdmName = metadata$cdmName,
    writePrefix = wp
  )
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
  metadata <- list(
    datasetName = datasetName,
    format = format,
    cdmName = nm,
    cdmVersion = vr
  )
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
    list.files(path = tempFolder, full.names = TRUE, recursive = TRUE) |>
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
    csvFiles <- list.files(path = tempFolder, full.names = TRUE, recursive = TRUE) |>
      purrr::keep(\(x) endsWith(x = x, suffix = ".parquet")) |>
      purrr::map(\(x) {
        nm <- paste0(substr(basename(x), 1, nchar(basename(x)) - 8), ".csv")
        csvFile <- file.path(path, nm)
        if (mode == "arrow") {
          x <- arrow::read_parquet(x)
        } else if (mode == "duckdb") {
          x <- DBI::dbGetQuery(con, glue::glue("SELECT * FROM '{x}'")) |>
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
  dbPath <- file.path(path, paste0(datasetName, ".duckdb"))
  nm <- OmopDatasets::omopDatasets$cdm_name[OmopDatasets::omopDatasets$dataset_name == datasetName]
  vr <- OmopDatasets::omopDatasets$cdm_version[OmopDatasets::omopDatasets$dataset_name == datasetName]
  metadata <- list(
    datasetName = datasetName,
    format = "database",
    dbDir = dbPath,
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
  walPath <- paste0(dbPath, ".wal")
  if (file.exists(dbPath) | file.exists(walPath)) {
    cli::cli_inform(c("i" = "Deleting prior `duckdb` file."))
    if (file.exists(dbPath)) file.remove(dbPath)
    if (file.exists(walPath)) file.remove(walPath)
  }
  duckdb::duckdb_shutdown(duckdb::duckdb(dbdir = dbPath))
  con <- duckdb::dbConnect(duckdb::duckdb(dbdir = dbPath))
  on.exit(DBI::dbDisconnect(conn = con, shutdown = TRUE))

  # schemas
  createSchema(con = con, schema = cdmSchema)
  createSchema(con = con, schema = writeSchema)

  # copy tables
  uniqueName <- omopgenerics::uniqueId(exclude = list.files(tempdir()))
  tempFolder <- file.path(tempdir(), uniqueName)
  dir.create(tempFolder)
  utils::unzip(zipfile = datasetPath, exdir = tempFolder)
  on.exit(unlink(tempFolder, recursive = TRUE))
  list.files(path = tempFolder, full.names = TRUE, recursive = TRUE) |>
    purrr::keep(\(x) endsWith(x = x, suffix = ".parquet")) |>
    purrr::map(\(x) {
      nm <- substr(basename(x), 1, nchar(basename(x)) - 8)
      sql <- "CREATE TABLE {cdmSchema}.{cdmPrefix}{nm} AS SELECT * FROM read_parquet('{x}')" |>
        glue::glue()
      DBI::dbExecute(conn = con, statement = sql)
    }) |>
    invisible()

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

#' Title
#'
#' @param path
#'
#' @return
#' @export
#'
#' @examples
cdmFromMetadata <- function(path) {
  # initial checks
  path <- validatePath(path)

  # read metadata
  if (endsWith(x = path, suffix = "DESCRIPTION")) {
    path <- file.path(path, "DESCRIPTION")
  }
  if (!file.exists(path)) {
    cli::cli_abort(c(x = "`METADATA` file does not exist in {.path {path}}"))
  }
  metadata <- readMetadata(path)

  if (!"format" %in% names(metadata)) {
    cli::cli_abort(c(x = "`METADATA` file is not properly formated."))
  }
  omopgenerics::assertChoice(metadata$format, c("csv", "duckdb"))

  if (metadata$format == "csv") {
    tables <- list.files(path = dirname(path), pattern = "\\.csv$", full.names = TRUE) |>
      purrr::map(\(x) read.csv(file = x))
    names(tables) <- purrr::map_chr(tables,\(x) substr(basename(x), 1, nchar(basename(x)) - 4))
    cdm <- omopgenerics::cdmFromTables(
      tables = tables,
      cdmName = metadata$cdmName,
      cdmVersion = metadata$cdmVersion
    )
  } else if (metadata$format == "duckdb") {
    rlang::check_installed("duckdb")
    rlang::check_installed("CDMConnector")
    con <- duckdb::dbConnect(drv = duckdb::duckdb(dbdir = metadata$dbDir))
    wp <- metadata$writePrefix
    if (wp == "") wp <- NULL
    cdm <- CDMConnector::cdmFromCon(
      con = con,
      cdmSchema = metadata$cdmSchema,
      writeSchema = metadata$writeSchema,
      cdmVersion = metadata$cdmVersion,
      cdmName = metadata$cdmName,
      writePrefix = wp
    )
  }

  return(cdm)
}

# connectionDetailsFromDataset <- function(datasetName = "GiBleed",
#                                          format = "duckdb",
#                                          cdmSchema = NULL,
#                                          writeSchema = NULL) {
#
# }

# connectionDetailsFromMetadata <- function(path) {
#
# }

datasetAvailable <- function(datasetName, call = parent.frame()) {
  folder <- datasetsFolder()
  if (!isDatasetDownloaded(datasetName = datasetName, path = folder)) {
    if (question(paste0("`", datasetName, "` is not downloaded, do you want to download it? Y/n"))) {
      downloadDataset(datasetName = datasetName, path = folder)
    } else {
      cli::cli_abort(c(x = "`{datasetName}` is not downloaded."), call = call)
    }
  }
  file.path(folder, datasetName, paste0(datasetName, ".zip"))
}
readMetadata <- function(metadataFile) {
  as.list(read.dcf(metadataFile)[1,])
}
