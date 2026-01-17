fetch_corpus <- function(
  project_dir,
  search_terms,
  st_spcc = st$spcc,
  publication_date = params$publication_date,
  types_filter = params$types_filter,
  sustainable_development_goals.idAND = NULL,
  workers = 8
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
        sustainable_development_goals.id = sustainable_development_goals.idAND,
        from_publication_date = publication_date$from #,
        # to_publication_date = publication_date$to
      )
    }
  )

  message("####################")
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

  message("####################")
  message(
    "The Harmonizing is not done automatically. Please do it by hand if necessary!"
  )

  # openalexPro::harmonize_parquet_schemata(
  #   root_dir = file.path(project_dir, "parquet")
  # )

  message("####################")
  message("Filtering raw corporas by types and end data...")

  files <- list.files(
    path = file.path(project_dir, "parquet"),
    pattern = "*.parquet",
    recursive = TRUE,
    full.names = TRUE
  )

  # parallel filtering of raw corpora
  pdt <- publication_date$to

  old_plan <- future::plan(future::multisession, workers = workers)
  on.exit(
    future::plan(old_plan),
    add = TRUE
  )

  progressr::with_progress({
    p <- progressr::progressor(along = files)

    future.apply::future_lapply(
      files,
      function(f) {
        p(sprintf("Processing %s", dirname(f)))

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

        arrow::open_dataset(
          f,
          format = "parquet"
        ) |>
          dplyr::filter(
            type %in% types_filter,
            publication_date <= pdt
          ) |>
          arrow::write_dataset(
            path = dirname(f_out),
            format = "parquet",
            basename_template = paste0(basename(f_out), "-{i}.parquet")
          )

        NULL
      }
    )
  })

  return(file.path(project_dir, "corpus"))
}
