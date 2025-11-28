export_sets <- function(
  project_dir,
  select = c(
    "id",
    "doi",
    "title",
    "publication_year",
    "citation",
    "abstract",
    "is_retracted",
    "language",
    "type",
    "cited_by_count",
    "primary_topic$id",
    "primary_topic$subfield$id",
    "primary_topic$display_name",
    "sustainable_development_goals"
  )
) {
  # Helper functions sdg ---------------------------------------------------

  safe_sdg_value <- function(x, i, col) {
    if (
      is.null(x) || !is.data.frame(x) || nrow(x) < i || !(col %in% names(x))
    ) {
      return(NA)
    }
    x[[col]][[i]]
  }

  sdg_chr <- function(sdg_list, i, col) {
    purrr::map_chr(
      sdg_list,
      ~ {
        v <- safe_sdg_value(.x, i, col)
        if (is.na(v)) NA_character_ else as.character(v)
      }
    )
  }

  sdg_dbl <- function(sdg_list, i, col) {
    purrr::map_dbl(
      sdg_list,
      ~ {
        v <- safe_sdg_value(.x, i, col)
        if (is.na(v)) NA_real_ else as.numeric(v)
      },
      .default = NA_real_
    )
  }

  # Identify nested and base columns ---------------------------------------

  base_cols <- select[!grepl("\\$", select)]
  nested_specs <- select[grepl("\\$", select)]

  # Turn e.g. "primary_topic$id" into an expression primary_topic$id -------

  nested_exprs <- rlang::parse_exprs(nested_specs)
  names(nested_exprs) <- gsub("\\$", ".", nested_specs) # e.g. "primary_topic_id"

  sets <- list.files(
    file.path(
      project_dir,
      "corpus"
    ),
    full.names = TRUE
  ) |>
    grepv(
      pattern = "/page="
    )

  names(sets) <- sets |>
    basename() |>
    utils::URLdecode() |>
    gsub(
      pattern = "page=",
      replacement = ""
    )

  log <- lapply(
    seq_along(sets),
    function(i) {
      works <- sets[[i]] |>
        arrow::open_dataset() |>
        dplyr::mutate(
          abstract = substr(abstract, 1, 5000),
          !!!nested_exprs # creates primary_topic_id, etc.
        ) |>
        dplyr::select(
          dplyr::all_of(base_cols),
          dplyr::all_of(names(nested_exprs))
        ) |>
        dplyr::collect()

      if ("sustainable_development_goals" %in% select) {
        works <- works |>
          dplyr::mutate(
            sdg.1.id = sdg_chr(sustainable_development_goals, 1, "id"),
            sdg.1.score = sdg_dbl(sustainable_development_goals, 1, "score"),
            sdg.1.display_name = sdg_chr(
              sustainable_development_goals,
              1,
              "display_name"
            ),
            sdg.2.id = sdg_chr(sustainable_development_goals, 2, "id"),
            sdg.2.score = sdg_dbl(sustainable_development_goals, 2, "score"),
            sdg.2.display_name = sdg_chr(
              sustainable_development_goals,
              2,
              "display_name"
            ),
            sdg.3.id = sdg_chr(sustainable_development_goals, 3, "id"),
            sdg.3.score = sdg_dbl(sustainable_development_goals, 3, "score"),
            sdg.3.display_name = sdg_chr(
              sustainable_development_goals,
              3,
              "display_name"
            ),
            sustainable_development_goals = NULL
          )
      }

      fn_xlsx <- file.path(
        project_dir,
        paste0(
          names(sets)[[i]],
          ".xlsx"
        )
      )
      writexl::write_xlsx(
        works,
        path = fn_xlsx
      )

      fn_csv <- file.path(
        project_dir,
        paste0(
          names(sets)[[i]],
          ".csv"
        )
      )
      write.csv(
        works,
        file = fn_csv,
        row.names = FALSE
      )

      return(c(csv = fn_csv, xlsx = fn_xlsx))
    }
  ) |>
    dplyr::bind_rows() |>
    dplyr::mutate(
      set = names(sets),
      .before = 1
    )

  write.csv(
    log,
    file = file.path(
      project_dir,
      "export_sets.csv"
    )
  )
  return(
    file.path(
      project_dir,
      "log_export_sets.csv"
    )
  )
}
