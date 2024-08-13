# --------------------------------------------------------- #
# ----------BLOGGER RANK-BASED IMPOSTORS PIPELINE---------- #
# --------------------------------------------------------- #

# -----RUN SOURCE FILE TO GET THE FUNCTIONS IN GLOBAL ENVIRONMENT----- #
source("/Users/user/Documents/GitHub/para_verifyr/rank_based_impostors_blog.R")

# -----SET LOCATIONS----- #
base <- "/Users/user/Documents/datasets/blogger/raw_error_fix/"

unknown_loc <- paste0(base, "known_final.jsonl") # Got known and unknown wrong way around compare to impostors
known_loc <- paste0(base, "unknown_final.jsonl") # Got known and unknown wrong way around compare to impostors
impostor_loc <- paste0(base, "phi_impostor_paragraphs_noqual.jsonl")
metadata_loc <- paste0(base, "metadata.jsonl")
general_impostor_loc <- paste0(base, "general_impostors_final.jsonl")
raw_filtered_for_authors <- paste0(base, "raw_filtered_authors.jsonl")


# -----READ IN THE DATA AND SAVE AS RDS FILES----- #

# The locations above are the base jsonl data locations not the final locations.
# In the code below we read in the jsonl files and convert them to document feature matrices.
# The metadata file doesn't change.
# For the other dfm's we have to specify whether it's x or y for the columns to be the same as for the metadata.
# Once the data has been converted to a dfm we save it as an RDS for faster reads.

metadata <- read_jsonl(metadata_loc)
# known <- read_jsonl(known_loc) |> sample_id_from_metadata(metadata, df_type = "x") |> convert_to_dfm()
# unknown <- read_jsonl(unknown_loc) |> sample_id_from_metadata(metadata, df_type = "y") |> convert_to_dfm()
# impostors <- read_jsonl(impostor_loc) |> sample_id_from_metadata(metadata, df_type = "x") |>
#   dplyr::rename('text' = 'rephrased') |>
#   convert_to_dfm()
# general_impostors <- read_jsonl(general_impostor_loc) |>
#   convert_to_dfm()

# This is the raw dataframe with any authors from the metadata filtered out.
raw_before_top_impostors <- read_jsonl(raw_filtered_for_authors) |>
  convert_to_dfm()
# 
# saveRDS(known, paste0(base, 'dfm/known_dfm.rds'))
# saveRDS(unknown, paste0(base, 'dfm/unknown_dfm.rds'))
# saveRDS(impostors, paste0(base, 'dfm/para_impostors_dfm.rds'))
# saveRDS(general_inpostors, paste0(base, 'dfm/general_impostors_dfm.rds'))

# -----READ IN RDS FILES----- #

# Here re-read in the RDS files, known, unknown and para_impostors are necessary for the paraphrasing
# test. Then the general_impostors is when we want to run the baseline test.

known <- readRDS(paste0(base, 'dfm/known_dfm.rds'))
unknown <- readRDS(paste0(base, 'dfm/unknown_dfm.rds'))
para_impostors <- readRDS(paste0(base, 'dfm/para_impostors_dfm.rds'))
# general_impostors <- readRDS(paste0(base, 'dfm/general_impostors_dfm.rds'))


top_impostors <- function(dfm_x, dfm_ref, num_top = 500){

  # Weight the dfm matrices
  x_weighted <- dfm_x |> quanteda::dfm_weight(scheme='prop')
  imp_weighted <- dfm_ref |> quanteda::dfm_weight(scheme='prop')

  # Get the union of the features
  feats <- union(featnames(x_weighted), featnames(imp_weighted))
  
  # Transpose the padded dfm's
  x_t <- t(quanteda:::pad_dfm(x_weighted, feats))
  imp_t <- t(quanteda:::pad_dfm(imp_weighted, feats))

  # Get the similarities of the impostors compared to x 
  test <- proxyC::simil(x_t, imp_t, margin = 2, method = "fjaccard") # Highest score most similar
  
  ranking <- rank(as.matrix(test), ties.method = "max")
  docs_to_keep <- which(ranking > nrow(dfm_ref) - num_top)
  
  # Docs selected
  imp_selected <- dfm_ref[docs_to_keep,]
  
  return(imp_selected)
}

# -----TOP IMPOSTORS PREPROCESSING BEFORE IMPOSTOR METHOD----- #

# Keep only the top n most similar impostors to the known docs then save the dfm.
# most_relevant_top <- preprocess_dfm(known, general_impostors)

# Impostors from the ENTIRE set of possibles
most_relevent_top_entire_selection <- preprocess_dfm(known, raw_before_top_impostors)
# saveRDS(most_relevant_top, paste0(base, "general_ref_dfm.rds"))
ref_impostors <- readRDS(paste0(base, "general_ref_dfm.rds"))

# -----RUN THE TEST----- #

# The code below runs the test 5 times and saves the results to 5 different locations.

for (i in 1:5) {
  # Run the function
  results_vs_known_rep <- rank_based_impostors(known, unknown, ref_impostors,
                                               num_feats = 100000,
                                               num_repetitions = 100,
                                               num_impostors = 100)
  
  # Construct the filename
  filename <- paste0(base, "results_ref_", i, ".jsonl")
  
  # Save the result
  save_jsonl(results_vs_known_rep, filename)
}
