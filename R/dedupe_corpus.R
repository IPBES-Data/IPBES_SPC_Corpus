#' Dedupe a Parquet Corpus by ID
#'
#' Scans a Parquet dataset on disk, removes duplicate rows by `id`, and writes
#' a deduplicated copy to a sibling folder.
#'
#' @param project_dir Character path to project root.
#' @param corpus_name Character name of the corpus folder. Default `"corpus"`.
#' @param corpus_deduped_name Character name of the deduplicated output folder.
#'   Default `"corpus_deduped"`.
#'
#' @return Invisibly returns `TRUE` on success.
#'
#' @importFrom arrow open_dataset read_parquet write_dataset
#' @importFrom dplyr filter summarize mutate select left_join n coalesce
#' @importFrom progressr with_progress progressor handlers handler_progress
#' @export
dedupe_corpus <- function(
  project_dir,
  corpus_name = "corpus",
  corpus_deduped_name = "corpus_deduped"
) {
  corpus <- file.path(project_dir, corpus_name) |>
    arrow::open_dataset()

  duplicates <- corpus |>
    dplyr::summarize(n = n(), .by = id) |>
    dplyr::filter(n > 1) |>
    collect()

  files <- list.files(
    path = file.path(project_dir, corpus_name),
    pattern = "*.parquet",
    recursive = TRUE,
    full.names = TRUE
  )

  old_handlers <- progressr::handlers()
  on.exit(progressr::handlers(old_handlers), add = TRUE)

  progressr::handlers(
    progressr::handler_progress(
      format = ":current/:total [:bar] :percent eta: :eta | :message"
    )
  )

  root_dir <- normalizePath(file.path(project_dir, corpus_name))

  progressr::with_progress(
    {
      p <- progressr::progressor(along = files)

      for (f in files) {
        rel_dir <- gsub(
          paste0("^", root_dir, "/?"),
          "",
          dirname(normalizePath(f))
        )
        p(rel_dir)

        # build destination path mirroring hive subdirs
        f_out <- gsub(
          pattern = paste0("/", corpus_name, "/"),
          replacement = paste0("/", corpus_deduped_name, "/"),
          x = f
        )

        dir.create(
          dirname(f_out),
          showWarnings = FALSE,
          recursive = TRUE
        )

        df <- arrow::read_parquet(
          f
        )

        df |>
          dplyr::filter(
            !(id %in% duplicates[["id"]])
          ) |>
          arrow::write_dataset(
            path = dirname(f_out),
            format = "parquet",
            basename_template = paste0(basename(f_out), "-{i}.parquet")
          )

        in_file <- df |>
          filter(
            id %in% duplicates[["id"]]
          ) |>
          dplyr::summarize(n_in_file = n(), .by = id)

        if (nrow(in_file) > 0) {
          duplicates <- duplicates |>
            dplyr::left_join(in_file, by = "id") |>
            dplyr::mutate(
              n_in_file = dplyr::coalesce(n_in_file, 0L),
              remaining = n - n_in_file
            ) |>
            dplyr::filter(
              remaining > 1
            ) |>
            dplyr::select(
              id,
              n
            )
        }
      }
    }
  )
}
