# ---------------------------------------------------------------------------- #
# ----------BLOGGER RANK-BASED IMPOSTORS LEXICAL DIFFERENCE PIPELINE---------- #
# ---------------------------------------------------------------------------- #

# -----RUN SOURCE FILE TO GET THE FUNCTIONS IN GLOBAL ENVIRONMENT----- #
source("/Users/user/Documents/GitHub/para_verifyr/rank_based_impostors_blog.R")

# -----SET LOCATIONS----- #
base <- "/Users/user/Documents/datasets/blogger/raw_error_fix/"

unknown_loc <- paste0(base, "known_final.jsonl") # Got known and unknown wrong way around compare to impostors
known_loc <- paste0(base, "unknown_final.jsonl") # Got known and unknown wrong way around compare to impostors
metadata_loc <- paste0(base, "metadata.jsonl")

impostor_loc <- "/Users/user/Documents/datasets/blogger/gpt-4o-lex-impostors-qual.jsonl"
# -----READ IN THE DATA AND SAVE AS RDS FILES----- #

# The locations above are the base jsonl data locations not the final locations.
# In the code below we read in the jsonl files and convert them to document feature matrices.
# The metadata file doesn't change.
# For the other dfm's we have to specify whether it's x or y for the columns to be the same as for the metadata.
# Once the data has been converted to a dfm we save it as an RDS for faster reads.

metadata <- read_jsonl(metadata_loc)
known <- read_jsonl(known_loc) |> sample_id_from_metadata(metadata, df_type = "x") |> convert_to_dfm()
unknown <- read_jsonl(unknown_loc) |> sample_id_from_metadata(metadata, df_type = "y") |> convert_to_dfm()

max_sim_impostors <- read_jsonl(impostor_loc) |> sample_id_from_metadata(metadata, df_type = "x") |>
  dplyr::rename('text' = 'rephrased') |>
  dplyr::group_by(sample_id) |>
  dplyr::arrange(desc(average_score)) |>
  dplyr::slice_head(n = 500) %>%
  dplyr::ungroup()

max_sim_impostors_dfm <- convert_to_dfm(max_sim_impostors)

# -----RUN THE TEST----- #

# The code below runs the test 5 times and saves the results to 5 different locations.

docs_gpt_4o <- docvars(max_sim_impostors_dfm)['sample_id'] |> unique() |> pull() |> as.numeric()
known_filtered <- dfm_subset(known, sample_id %in% docs_gpt_4o)
unknown_filtered <- dfm_subset(unknown, sample_id %in% docs_gpt_4o)

for (i in 1:5) {
  # Run the function
  results_vs_known_rep <- rank_based_impostors(known_filtered, unknown_filtered, max_sim_impostors_dfm,
                                               num_feats = 100000,
                                               num_repetitions = 100,
                                               num_impostors = 100)
  
  # Construct the filename
  filename <- paste0(base, "/results_4o_lex_diff/results_gpt_4o_lex_diff_", i, ".jsonl")
  
  # Save the result
  save_jsonl(results_vs_known_rep, filename)
}

