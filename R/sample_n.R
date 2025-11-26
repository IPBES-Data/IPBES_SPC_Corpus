#' Sample n random rows per partition without loading full partitions
#'
#' Streams each Hive partition of a Parquet dataset and performs reservoir
#' sampling so only `n` rows per partition are kept in memory. This avoids
#' `collect()` on the whole dataset and is safe for very large partitions.
#'
#' @details Reservoir sampling logic:
#'   1) Fill the reservoir with the first `n` rows.
#'   2) For the k-th row (k > n), draw `j` uniformly from 1..k; if `j <= n`,
#'      replace reservoir slot `j` with the new row.
#'   3) Repeat until the end of the partition stream.
#'   Each row has equal chance of being kept, while memory stays bounded by `n`.
#'
#' @param corpus Path to the root of the Hive-partitioned dataset
#'   (e.g. a folder containing `page=...` subdirectories).
#' @param n Integer number of rows to sample per partition.
#' @param partition_col Name of the partition column (default `"page"`).
#' @param seed Optional integer seed for reproducible sampling.
#' @param batch_size Optional batch size (rows) to use when scanning Parquet
#'   fragments. Lower this if you want smaller in-memory chunks than the Arrow
#'   default (~64K rows).
#' @param workers Number of workers for `future.apply::future_lapply()`.
#'   A temporary future plan is set inside the function: `multisession` when
#'   `workers > 1`, otherwise `sequential`. The prior plan is restored on exit.
#'
#' @return A named list of tibbles (one per partition), each with up to `n`
#'   rows. If a partition has fewer than `n` rows, all of its rows are returned.
#'
#' @examples
#' sample_n(
#'   corpus = "spc_corpus/output/chapter_3/corpus.openalex_pro/corpus/",
#'   n = 250,
#'   partition_col = "page",
#'   seed = 123
#' )
sample_n <- function(
  corpus,
  n,
  partition_col = "page",
  select = c("id", "doi", "citation", "title", "abstract"),
  seed = NULL,
  batch_size = 3000,
  workers = 1
) {
  stopifnot(is.character(corpus), length(corpus) == 1)
  stopifnot(is.numeric(n), length(n) == 1, n > 0)

  if (!is.null(seed)) {
    set.seed(seed)
  }

  partition_dirs <- list.dirs(
    path = corpus,
    full.names = TRUE,
    recursive = FALSE
  ) |>
    grepv(
      pattern = paste0("/", partition_col, "=*")
    )

  names(partition_dirs) <- sub(
    paste0("^", partition_col, "="),
    "",
    basename(partition_dirs)
  ) |>
    utils::URLdecode()

  if (length(partition_dirs) == 0L) {
    stop(
      "No hive partitions found at ",
      corpus,
      " named ",
      partition_col,
      "<value>"
    )
  }

  sample_partition <- function(
    part_dir
  ) {
    part_value <- sub(
      paste0("^", partition_col, "="),
      "",
      basename(part_dir)
    )

    scanner <- arrow::open_dataset(
      sources = part_dir,
      format = "parquet"
    ) |>
      dplyr::select(
        dplyr::all_of(select)
      ) |>
      arrow::Scanner$create(
        batch_size = batch_size
      )
    reader <- scanner$ToRecordBatchReader()

    reservoir <- vector("list", n)
    seen <- 0L

    repeat {
      # stream batches; never holds the full partition
      batch <- reader$read_next_batch()
      if (is.null(batch)) {
        break
      }

      df <- as.data.frame(batch)
      if (nrow(df) == 0) {
        next
      }

      for (i in seq_len(nrow(df))) {
        seen <- seen + 1L
        if (seen <= n) {
          reservoir[[seen]] <- df[i, , drop = FALSE]
        } else {
          j <- sample.int(seen, 1L)
          if (j <= n) {
            reservoir[[j]] <- df[i, , drop = FALSE]
          }
        }
      }
    }

    if (seen == 0L) {
      return(NULL)
    }

    result <- dplyr::bind_rows(
      reservoir[seq_len(min(seen, n))]
    )

    result[[partition_col]] <- part_value

    return(result)
  }

  old_plan <- future::plan()
  on.exit(future::plan(old_plan), add = TRUE)
  if (workers > 1) {
    future::plan(
      future::multisession,
      workers = workers
    )
  } else {
    future::plan(
      future::sequential
    )
  }

  samples <- future.apply::future_lapply(
    partition_dirs,
    sample_partition,
    future.seed = TRUE
  )
  samples <- Filter(Negate(is.null), samples)

  if (length(samples) == 0) {
    return(list())
  }

  # keep list elements named by partition value
  samples
}
