
validateDatasetName <- function(datasetName, call = parent.frame()) {
  omopgenerics::assertChoice(datasetName, choices = availableDatasets(), length = 1, call = call)
  invisible(datasetName)
}
validatePath <- function(path, call = parent.frame()) {
  omopgenerics::assertCharacter(x = path, length = 1, call = call)
  if (dir.exists(path)) {
    cli::cli_abort(c(x = "Path {.path {path}} does not exist."), call = call)
  }
  invisible(path)
}
