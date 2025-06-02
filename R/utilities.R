
#' Available OMOP CDM Synthetic Datasets
#'
#' There are the OMOP CDM Synthetic datasets that are available to download
#' using the `OmopDatasets` package.
#'
#' @format A data frame with 4 variables:
#' \describe{
#'   \item{dataset_name}{Name of the dataset.}
#'   \item{url}{url to download the dataset.}
#'   \item{cdm_name}{Name of the cdm reference created.}
#'   \item{cdm_version}{OMOP CDM version of the dataset.}
#' }
#'
#' @examples
#' omopDatasets
#'
"omopDatasets"

#' Download an OMOP Synthetic dataset.
#'
#' @inheritParams datasetNameDoc
#' @param path Path where to download the dataset.
#' @param overwrite Whether to overwrite the dataset if it is already
#' downloaded.
#'
#' @return The path to the downloaded dataset.
#' @export
#'
#' @examples
#' \donttest{
#' library(OmopDatasets)
#'
#' isDatasetDownloaded("GiBleed")
#' downloadDataset("GiBleed")
#' isDatasetDownloaded("GiBleed")
#' }
downloadDataset <- function(datasetName = "GiBleed",
                            path = datasetsFolder(),
                            overwrite = FALSE) {
  # initial checks
  datasetName <- validateDatasetName(datasetName)
  path <- validatePath(path)
  omopgenerics::assertLogical(overwrite, length = TRUE)

  datasetPath <- file.path(path, datasetName)
  datasetFile <- file.path(datasetPath, paste0(datasetPath, ".zip"))
  # is available
  if (dir.exists(datasetPath)) {
    if (file.exists(datasetFile)) {
      if (overwrite) {
        unlink(datasetPath, recursive = TRUE)
      } else {
        if (question("Do you want to overwrite prior existing dataset?")) {
          unlink(datasetPath, recursive = TRUE)
        } else {
          return(invisible(datasetPath))
        }
      }
    }
  } else {
    dir.create(datasetPath)
  }

  # download dataset
  url <- OmopDatasets::omopDatasets$url[OmopDatasets::omopDatasets$dataset_name == datasetName]
  download.file(url = url, destfile = datasetFile)

  invisible(datasetFile)
}

#' Check if a certain dataset is downloaded.
#'
#' @inheritParams datasetNameDoc
#' @param path Path where to search for the dataset.
#'
#' @return Whether the dataset is available or not.
#' @export
#'
#' @examples
isDatasetDownloaded <- function(datasetName = "GiBleed",
                                path = datasetsFolder()) {
  # initial checks
  datasetName <- validateDatasetName(datasetName)
  path <- validatePath(path)

  file.exists(file.path(path, datasetPath, paste0(datasetPath, ".zip")))
}

#' Title
#'
#' @return
#' @export
#'
#' @examples
availableDatasets <- function() {
  OmopDatasets::omopDatasets$dataset_name
}

#' Check the availability of the OMOP CDM datasets.
#'
#' @return A message with the availability of the datasets.
#' @export
#'
#' @examples
#' datasetStatus()
datasetStatus <- function() {
  x <- OmopDatasets::omopDatasets |>
    dplyr::select("dataset_name") |>
    dplyr::mutate(exists = dplyr::if_else(file.exists(file.path(
      datasetsFolder(), .data$dataset_name, paste0(.data$dataset_name, ".zip")
    )), 1, 0)) |>
    dplyr::arrange(dplyr::desc(.data$exists), .data$dataset_name) |>
    dplyr::mutate(
      status = dplyr::if_else(.data$exists == 1, "v", "x"),
      message = rlang::set_names(list(.data$dataset_name), .data$status)
    )
  cli::cli_inform(x$message)
  x <- x |>
    dplyr::select("status", "exists", "dataset_name")
  invisible(x)
}

#' Check or set the datasets Folder
#'
#' @param path Path to a folder to store the synthetic datasets. If NULL the
#' current OMOP_DATASETS_FOLDER is returned.
#'
#' @return The dataset folder.
#' @export
#'
#' @examples
#' \donttest{
#' datasetsFolder()
#' datasetsFolder(file.path(getwd(), "OMOP_DATASETS"))
#' datasetsFolder()
#' }
#'
datasetsFolder <- function(path = NULL) {
  if (is.null(path)) {
    if (Sys.getenv(omopDatasetsKey) == "") {
      cli::cli_inform(c("!" = "`{omopDatasetsKey}` is currently not set."))
      tempOmopDatasetsFolder <- file.path(tempdir(), omopDatasetsKey)
      dir.create(tempOmopDatasetsFolder, showWarnings = FALSE)
      cli::cli_inform(c("i" = "`{omopDatasetsKey}` temporarily set to {.path {tempOmopDatasetsFolder}}."))
      cli::cli_inform(c("!" = "Please consider creating a permanent `{omopDatasetsKey}` location."))
      arg <- rlang::set_names(x = tempOmopDatasetsFolder, nm = omopDatasetsKey)
      do.call(what = Sys.setenv, args = as.list(arg))
    }
  } else {
    omopgenerics::assertCharacter(x = path, length = 1)
    if (!dir.exists(path)) {
      cli::cli_inform(c("i" = "Creating {.path {path}}."))
      dir.create(path)
    }
    arg <- rlang::set_names(x = path, nm = omopDatasetsKey)
    do.call(what = Sys.setenv, args = as.list(arg))
    c("i" = "If you want to create a permanent `{omopDatasetsKey}` write the following in your `.Renviron` file:",
      "", " " = "{.pkg {omopDatasetsKey}}=\"{path}\"", "") |>
      cli::cli_inform()
  }
  return(Sys.getenv(omopDatasetsKey))
}

question <- function(message) {
  if (rlang::is_interactive()) {
    x <- ""
    while(!x %in% c("yes", "no")) {
      cli::cli_inform(message = message)
      x <- tolower(readline())
      x[x == "y"] <- "yes"
      x[x == "n"] <- "no"
    }
  } else {
    TRUE
  }
}

