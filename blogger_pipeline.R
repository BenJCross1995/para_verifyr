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
gpt_4o_loc <- "/Users/user/Documents/datasets/blogger/gpt-4o-impostors-no-qual.jsonl"
gpt_4o_qual_loc <- "/Users/user/Documents/datasets/blogger/gpt-4o-impostors-qual.jsonl"
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
# gpt_4o_impostors <- read_jsonl(gpt_4o_loc) |> sample_id_from_metadata(metadata, df_type = "x") |>
#   dplyr::rename('text' = 'rephrased') |>
#   convert_to_dfm()
gpt_4o_qual_impostors <- read_jsonl(gpt_4o_qual_loc) |> sample_id_from_metadata(metadata, df_type = "x") |>
  dplyr::rename('text' = 'rephrased') |>
  dplyr::group_by(sample_id) |>
  dplyr::arrange(desc(average_score)) |>
  dplyr::slice_head(n = 500) %>%
  dplyr::ungroup()


gpt_4o_qual_impostors |>
  ggplot(aes(x = average_score)) +
  geom_density() +
  ggtitle("Density plot of ParaScore for the Top 500 impostors for each sample")

gpt_4o_qual_impostors <- convert_to_dfm(gpt_4o_qual_impostors)

# general_impostors <- read_jsonl(general_impostor_loc) |>
#   convert_to_dfm()

# This is the raw dataframe with any authors from the metadata filtered out.
# raw_before_top_impostors <- read_jsonl(raw_filtered_for_authors) |>
#   convert_to_dfm()
# 
# saveRDS(known, paste0(base, 'dfm/known_dfm.rds'))
# saveRDS(unknown, paste0(base, 'dfm/unknown_dfm.rds'))
# saveRDS(impostors, paste0(base, 'dfm/para_impostors_dfm.rds'))
# saveRDS(general_inpostors, paste0(base, 'dfm/general_impostors_dfm.rds'))
# saveRDS(gpt_4o_impostors, paste0(base, 'dfm/para_gpt_4o_impostors_dfm.rds'))
# saveRDS(gpt_4o_qual_impostors, paste0(base, 'dfm/para_gpt_4o_qual_dfm.rds'))

# -----READ IN RDS FILES----- #

# Here re-read in the RDS files, known, unknown and para_impostors are necessary for the paraphrasing
# test. Then the general_impostors is when we want to run the baseline test.

known <- readRDS(paste0(base, 'dfm/known_dfm.rds'))
unknown <- readRDS(paste0(base, 'dfm/unknown_dfm.rds'))
# para_impostors <- readRDS(paste0(base, 'dfm/para_impostors_dfm.rds'))
# general_impostors <- readRDS(paste0(base, 'dfm/general_impostors_dfm.rds'))
# gpt_4o_imp <- readRDS(paste0(base, 'dfm/para_gpt_4o_impostors_dfm.rds'))
# gpt_4o_qual_imp <- readRDS(paste0(base, 'dfm/para_gpt_4o_qual_dfm.rds'))

# -----TOP IMPOSTORS PREPROCESSING BEFORE IMPOSTOR METHOD----- #

# Keep only the top n most similar impostors to the known docs then save the dfm.
# most_relevant_top <- preprocess_dfm(known, general_impostors)

# -----IMPOSTORS FROM ENTIRE SET OF POSSIBILITIES----- #
# max_sim_impostors <- preprocess_dfm(known, raw_before_top_impostors)
# saveRDS(max_sim_impostors, paste0(base, "dfm/max_sim_impostors.rds"))
# max_sim_impostors <- readRDS(paste0(base, "dfm/max_sim_impostors.rds"))

# saveRDS(most_relevant_top, paste0(base, "general_ref_dfm.rds"))
#ref_impostors <- readRDS(paste0(base, "general_ref_dfm.rds"))

# -----RUN THE TEST----- #

# The code below runs the test 5 times and saves the results to 5 different locations.

docs_gpt_4o <- docvars(gpt_4o_qual_impostors)['sample_id'] |> unique() |> pull() |> as.numeric()
known_filtered <- dfm_subset(known, sample_id %in% docs_gpt_4o)
unknown_filtered <- dfm_subset(unknown, sample_id %in% docs_gpt_4o)

for (i in 1:5) {
  # Run the function
  results_vs_known_rep <- rank_based_impostors(known_filtered, unknown_filtered, gpt_4o_qual_impostors,
                                               num_feats = 100000,
                                               num_repetitions = 100,
                                               num_impostors = 100)
  
  # Construct the filename
  filename <- paste0(base, "/gpt_4o_results/results_gpt_4o_qual", i, ".jsonl")
  
  # Save the result
  save_jsonl(results_vs_known_rep, filename)
}

