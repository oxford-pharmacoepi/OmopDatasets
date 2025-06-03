test_that("test utility functions", {
  # availableDatasets
  expect_identical(availableDatasets(), OmopDatasets::omopDatasets$dataset_name)

  expect_no_error(datasetsFolder())
  myFolder <- file.path(tempdir(), "DATASETS")
  expect_message(expect_no_error(datasetsFolder(myFolder)))
  expect_identical(datasetsFolder(), myFolder)

  expect_false(isDatasetDownloaded("GiBleed"))
  expect_no_error(downloadDataset("GiBleed"))
  expect_true(isDatasetDownloaded("GiBleed"))
  expect_no_error(downloadDataset("GiBleed"))
  expect_no_error(downloadDataset("GiBleed", overwrite = TRUE))

  expect_message(x <- datasetStatus())
  expect_identical(
    OmopDatasets::omopDatasets |>
      dplyr::select("dataset_name") |>
      dplyr::mutate(
        exists = dplyr::if_else(.data$dataset_name == "GiBleed", 1, 0),
        status = dplyr::if_else(.data$dataset_name == "GiBleed", "v", "x")
      ) |>
      dplyr::arrange(dplyr::desc(.data$exists), .data$dataset_name),
    x
  )

  expect_error(validatePath("path_do_not_exist"))
})
