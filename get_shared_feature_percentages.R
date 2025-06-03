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
library(ggrepel)
library(hexbin)

# ----LOCATIONS----
base_loc <- "/Volumes/BCross/datasets/author_verification/training/"
base_loc <- "/Volumes/ExternalHDD/cloud_backup/datasets/author_verification/training/"
corpus <- "Wiki"

known_loc <- paste0(base_loc, corpus, "/known_raw_dfm.rds")

impostor_loc <- paste0(base_loc, corpus, "/gpt_4o_mini_full/impostor_dfm/")
impostor_loc <- "/Volumes/ExternalHDD/cloud_backup/datasets/author_verification/training/Wiki/gpt_4o_mini_full/impostor_dfm/fyunck_click_text_2_dfm.rds"
names(docvars(readRDS(impostor_loc)))
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

process_doc <- function(doc, impostor_loc, known_feats, id_col) {
  # load impostor DFM
  imp_path <- paste0(impostor_loc, doc, "_dfm.rds")
  imp_dfm  <- readRDS(imp_path)
  
  n_imp <- ndoc(imp_dfm)
  width <- nchar(n_imp)
  docvars(imp_dfm, "impostor_id") <- 
    sprintf("impostor_%0*d", width, seq_len(n_imp))
  
  # Get known features
  known <- known_feats[[doc]]$feats
  known_set <- as.character(known)
  
  # Subset to known features *before* binary conversion
  imp_known <- dfm_select(imp_dfm, pattern = known_set, selection = "keep")
  
  # Binary matrices
  imp_binary <- imp_dfm > 0
  imp_known_binary <- imp_known > 0
  
  # Shared = intersection
  shared <- rowSums(imp_known_binary)
  
  # Impostor's feature count
  impostor_total_feats <- rowSums(imp_binary)
  
  # Combined = union = known + impostor - shared
  combined <- length(known_set) + impostor_total_feats - shared
  
  # Output
  tibble(
    id = id_col,
    doc_id = doc,
    impostor_id = docvars(imp_dfm, "impostor_id"),
    parascore = docvars(imp_dfm, "parascore_free"),
    combined_features = combined,
    shared_features = shared,
    pct_shared = shared / combined * 100
  )
}

get_feature_similarity <- function(known_loc, impostor_loc, id_col='gpt-4o-mini'){
  
  # ----FORMAT KNOWN DFM----
  known <- readRDS(known_loc)
  docvars(known, "temp_doc_id") <- create_temp_doc_id(docvars(known, "doc_id"))
  
  # Pull out your doc_ids
  doc_ids <- docvars(known, "temp_doc_id")

  # Pre-compute each known doc’s feature set & length
  known_feats <- map(doc_ids, function(id) {
    dfm_i <- dfm_subset(known, temp_doc_id == id)
    feats  <- featnames(dfm_i)[as.numeric(dfm_i) > 0]
    list(feats = feats, n = length(feats))
  })
  names(known_feats) <- doc_ids
  
  # Run & bind:
  result <- map_dfr(doc_ids,
                    process_doc,
                    impostor_loc = impostor_loc,
                    known_feats = known_feats,
                    id_col = id_col)
  
  return(result)
}

# ----LOCATIONS----
base_loc <- "/Volumes/BCross/datasets/author_verification/training/"
base_loc <- "/Volumes/ExternalHDD/cloud_backup/datasets/author_verification/training/"

corpuses <- c('Wiki', 'Enron')

results <- data.frame()

for(corpus in corpuses){
  
  print(paste0('Processing ', corpus, ' data...'))
  result <- get_feature_similarity(
    known_loc = paste0(base_loc, corpus, "/known_raw_dfm.rds"),
    impostor_loc = paste0(base_loc, corpus, "/gpt_4o_mini_full/impostor_dfm/"),
    id_col = paste0('gpt-4o-mini-training-', tolower(corpus))
  )
  
  results <- rbind(results_v2, result)
}
results

library(ggplot2)
library(stringr)

ggplot(results_v2, aes(x = parascore, y = pct_shared, fill = id)) +
  # use a solid point with border + fill
  geom_point(shape = 21, color = "white", size = 2.5, stroke = 0.2, alpha = 0.8) +
  
  # a gentle loess curve (optional—comment out if you only want raw points)
  # geom_smooth(aes(color = id), method = "loess", se = FALSE, size = 0.8, linetype = "solid") +
  
  # nice qualitative palette
  scale_fill_brewer(palette = "Set2") +
  scale_color_brewer(palette = "Set2") +
  
  # tighten up the axes so points aren’t right on the edges
  scale_x_continuous(
    name   = "Parascore",
    expand = expansion(mult = 0.02)
  ) +
  scale_y_continuous(
    name   = "Percentage of Known Features Present",
    expand = expansion(mult = 0.02)
  ) +
  
  # title & subtitle
  labs(
    title    = "Parascore vs. Percentage of Shared Features",
    subtitle = "Each point is one document, filled by dataset",
    fill     = "Dataset",
    color    = "Dataset"
  ) +
  
  # clean, minimal theme
  theme_minimal(base_size = 14) +
  theme(
    panel.grid.minor       = element_blank(),
    panel.grid.major       = element_line(color = "grey90"),
    legend.position        = "right",
    legend.key             = element_rect(fill = "transparent"),
    legend.background      = element_rect(fill = "transparent"),
    plot.title             = element_text(face = "bold", size = 16),
    plot.subtitle          = element_text(size = 12)
  )

library(ggplot2)
library(ggExtra)

p <- ggplot(results, aes(y = parascore, x = pct_shared, color = id, fill = id)) +
  geom_point(shape = 21, alpha = 0.6, size = 1.8, stroke = 0.1) +
  scale_color_brewer(palette = "Set2") +
  scale_fill_brewer (palette = "Set2") +
  labs(
    title = str_to_title("Grouped distributions of shared feature percenatge vs parascore"),
    x = "Shared Feature Percentage",
    y = "ParaScore",
    color = "Dataset",
    fill = "Dataset"
  ) +
  theme_minimal() +
  theme(
    legend.position = "bottom"
  )

ggMarginal(
  p,
  type       = "density",
  margins    = "both",
  groupColour= TRUE,
  groupFill  = TRUE,
  size       = 5,
  trim = TRUE
)


# ----PLOT THE DENSITY OF THE SHARED FEATURE PERCENTAGES----

# 1. Compute per-id stats
ext <- results %>%
  group_by(id) %>%
  summarize(
    min_pct  = min(pct_shared),
    mean_pct = mean(pct_shared),
    max_pct  = max(pct_shared)
  ) %>%
  ungroup() %>%
  # pivot longer so we can label in a single layer
  pivot_longer(cols = c(min_pct, mean_pct, max_pct),
               names_to  = "stat",
               values_to = "pct") %>%
  mutate(
    label = case_when(
      stat == "min_pct"  ~ paste0("Min:  ", round(pct,1), "%"),
      stat == "mean_pct" ~ paste0("Mean: ", round(pct,1), "%"),
      stat == "max_pct"  ~ paste0("Max:  ", round(pct,1), "%")
    ),
    # position all labels at y = 0, then nudge up
    y = 0
  )

# 2. Plot
ggplot(results, aes(x = pct_shared, color = id, fill = id)) +
  
  # densities
  geom_density(trim = TRUE, alpha = 0.2, size = 1) +
  
  # v‐lines for each stat
  geom_vline(data = ext, 
             aes(xintercept = pct, color = id, linetype = stat),
             size = 0.8) +
  scale_linetype_manual(
    "", 
    values = c(min_pct = "dotted", mean_pct = "dashed", max_pct = "dotted")
  ) +
  
  # pop‐out labels
  geom_text_repel(
    data = ext,
    aes(x = pct, y = y, label = label, color = id),
    direction    = "y",
    nudge_y      = 0.02,
    segment.size = 0.3,
    show.legend  = FALSE
  ) +
  
  # facet per id, single column
  facet_wrap(~ id, ncol = 1) +
  
  # fix axis
  xlim(0, 100) +
  
  # brewer palette for both fill & color
  scale_color_brewer(type = "qual", palette = "Set2") +
  scale_fill_brewer(type = "qual", palette = "Set2") +
  
  # labels & legend at bottom
  labs(
    title    = str_to_title("Percentage of shared features between known and impostor documents"),
    subtitle = "100% indicates identical features but not identical documents.",
    x        = "Shared Feature Percentage",
    y        = "Density",
    color    = "Dataset",
    fill     = "Dataset"
  ) +
  
  theme_minimal(base_size = 12) +
  theme(
    panel.grid.major.x  = element_line(color = "grey90"),
    legend.position     = "none",
    strip.text          = element_text(face = "bold"),
  )

# ----PLOT THE SHARED FEATURE PERCENTAGE VS PARASCORE----

ggplot(results, aes(x = pct_shared, y = parascore)) +
  stat_binhex(bins = 50) +
  scale_fill_viridis_c(option = "C") +
  labs(
    title = "Shared Feature Percentage vs Parascore",
    subtitle = "Lighter areas indicate a higher density of points",
    y = "Parascore",
    x = "Shared Feature Percentage",
    fill = "Count"
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
