#---------------------------------------------------#
#----------------GET SHARED FEATURES----------------#
#---------------------------------------------------#

# This notebook gathers the shared features between the known document,
# and each of the Impostor documents for a given corpus.

library(quanteda)
library(stringr)
library(tidyr)
library(ggplot2)
library(dplyr)
library(purrr)

# ----LOCATIONS----
base_loc <- "/Volumes/BCross/datasets/author_verification/training/"
corpus <- "Wiki"

known_loc <- paste0(base_loc, corpus, "/known_raw_dfm.rds")

impostor_loc <- paste0(base_loc, corpus, "/gpt_4o_mini_full/impostor_dfm/")

# ----UTILITY FUNCTIONS----
create_temp_doc_id <- function(input_text) {
  # 1. extract what’s between [ and ] (NA if no match)
  extracted <- str_extract(input_text, "(?<=\\[)[^\\]]+(?=\\])")
  
  # 2. replace any non-word character (anything other than letter/digit/_) with "_"
  cleaned   <- str_replace_all(extracted, "[^\\w]", "_")
  
  # 3. collapse runs of 2 or more "_" to a single "_"
  cleaned   <- str_replace_all(cleaned, "_{2,}", "_")
  
  # 4. lowercase
  str_to_lower(cleaned)
}

# ----FORMAT KNOWN DFM----
known <- readRDS(known_loc)
docvars(known, "temp_doc_id") <- create_temp_doc_id(docvars(known, "doc_id"))

# ----GET PERCENT OF SHARED NON_ZERO KNOWN FEATURES----
# Pull out your doc_ids
doc_ids <- docvars(known, "temp_doc_id")

# Pre-compute each known doc’s feature set & length
known_feats <- map(doc_ids, function(id) {
  dfm_i <- dfm_subset(known, temp_doc_id == id)
  feats  <- featnames(dfm_i)[as.numeric(dfm_i) > 0]
  list(feats = feats, n = length(feats))
})
names(known_feats) <- doc_ids

# Process documents “one doc at a time”
process_doc <- function(doc) {
  # load that doc’s impostor‐DFM
  imp_path <- paste0(impostor_loc, doc, "_dfm.rds")
  imp_dfm   <- readRDS(imp_path)
  
  # give them more relavent ids
  n_imp     <- ndoc(imp_dfm)
  width     <- nchar(n_imp)
  docvars(imp_dfm, "impostor_id") <- 
    sprintf("impostor_%0*d", width, seq_len(n_imp))
  
  # subset to only the known features, then count non-zero per row
  feats    <- known_feats[[doc]]$feats
  num_feats <- length(feats)
  imp_sub  <- dfm_select(imp_dfm, pattern = feats, selection = "keep")
  shared   <- rowSums(imp_sub > 0)
  
  # Assemble into a dataframe
  tibble(
    doc_id     = doc,
    impostor_id = docvars(imp_dfm, "impostor_id"),
    known_features = num_feats,
    shared      = shared,
    pct_shared  = shared / known_feats[[doc]]$n * 100
  )
}

# Run & bind:
result <- map_dfr(doc_ids, process_doc)

result |> head()

# ----PLOT THE DENSITY OF THE SHARED FEATURE PERCENTAGES----
ggplot(result, aes(x = pct_shared)) +
  geom_density(fill = NA, size = 1) +
  geom_vline(aes(xintercept = mean(pct_shared)),
             linetype = "dashed", linewidth = 0.8) +
  labs(
    title = "Density of Percentage Shared Features",
    x     = "Percentage of Known Features Present",
    y     = "Density"
  ) +
  theme_minimal()

# ----SHOW THE OVERLAP OF FEATURES----

show_overlap <- function(doc,
                         known_dfm,
                         impostor_loc,
                         imp_to_show = NULL) {
  # 1. extract known features
  k_dfm   <- dfm_subset(known_dfm, temp_doc_id == doc)
  k_mat   <- as.numeric(k_dfm)
  k_feats <- featnames(k_dfm)[ k_mat > 0 ]
  cat("\n-- Known [", doc, "] has", length(k_feats), "non-zero features --\n")
  cat(paste0(k_feats, collapse = ", "), "\n\n")
  
  # 2. load impostor dfm + assign ids
  imp_path <- file.path(impostor_loc, paste0(doc, "_dfm.rds"))
  imp_dfm  <- readRDS(imp_path)
  n_imp    <- ndoc(imp_dfm)
  width    <- nchar(n_imp)
  imp_ids  <- sprintf("impostor_%0*d", width, seq_len(n_imp))
  docvars(imp_dfm, "impostor_id") <- imp_ids
  
  # 3. determine which rows to show
  if (!is.null(imp_to_show)) {
    if (!(imp_to_show %in% imp_ids)) {
      stop("`", imp_to_show, "` not found in this DFM (available: ",
           paste(imp_ids, collapse = ", "), ").")
    }
    row_idxs <- which(imp_ids == imp_to_show)
  } else {
    row_idxs <- seq_len(n_imp)
  }
  
  # 4. for each selected impostor, show feats & intersection
  for(i in row_idxs) {
    row_vec <- as.numeric(imp_dfm[i, ])
    i_feats <- featnames(imp_dfm)[ row_vec > 0 ]
    common  <- intersect(k_feats, i_feats)
    
    cat(">>>", imp_ids[i], "- has", length(i_feats), "features\n")
    cat("    Non-zero impostor features:\n      ",
        if(length(i_feats)) paste(i_feats, collapse = ", ") else "(none)", "\n")
    cat("    Shared with known (", length(common), "):\n      ",
        if(length(common)) paste(common, collapse = ", ") else "(none)", "\n\n")
  }
}

# Show ALL impostors for document “smith_j_1856”
# show_overlap(
#   doc           = doc_ids[1],
#   known_dfm     = known,
#   impostor_loc  = impostor_loc
# )

# Show specific impostor
# show_overlap(
#   doc           = doc_ids[1],
#   known_dfm     = known,
#   impostor_loc  = impostor_loc,
#   imp_to_show   = "impostor_304"
# )
