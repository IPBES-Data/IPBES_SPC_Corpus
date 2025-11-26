fetch_corpus <- function(
  project_dir,
  search_terms,
  st_spcc = st$spcc,
  publication_date = params$publication_date,
  types_filter = params$types_filter,
  workers = 2
) {
  queries <- lapply(
    search_terms,
    function(st_c) {
      openalexPro::pro_query(
        title_and_abstract.search = paste0(
          "(",
          st_spcc,
          ") AND (",
          st_compact(st_c),
          ")"
        ),
        # type = params$types_filter,
        from_publication_date = publication_date$from,
        to_publication_date = publication_date$to
      )
    }
  )

  message("Downloading raw corporas...")

  openalexPro::pro_fetch(
    queries,
    project_folder = file.path(project_dir),
    verbose = FALSE,
    mailto = "rainer@krugs.de",
    api_key = Sys.getenv("API_openalex", ""),
    progress = TRUE,
    workers = workers,
  )

  message("Harmonizing raw corporas ...")

  openalexPro::harmonize_parquet_schemata(
    root_dir = file.path(project_dir, "parquet")
  )

  message("Filtering raw corporas by types ...")

  files <- list.files(
    path = file.path(project_dir, "parquet"),
    pattern = "*.parquet",
    recursive = TRUE,
    full.names = TRUE
  )

  for (f in files) {
    # build destination path mirroring hive subdirs

    f_out <- gsub(
      pattern = "/parquet/",
      replacement = "/corpus/",
      x = f
    )

    dir.create(
      dirname(f_out),
      showWarnings = FALSE,
      recursive = TRUE
    )

    # read lazily
    arrow::open_dataset(
      f,
      format = "parquet"
    ) |>
      dplyr::filter(
        type %in% types_filter
      ) |>
      arrow::write_dataset(
        path = dirname(f_out),
        format = "parquet",
        basename_template = paste0(basename(f_out), "-{i}.parquet")
      )
  }

  return(file.path(project_dir, "corpus"))
}
