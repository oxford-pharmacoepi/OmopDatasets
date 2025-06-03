test_that("test available datasets", {
  expect_identical(availableDatasets(), omopDatasets$dataset_name)
})
