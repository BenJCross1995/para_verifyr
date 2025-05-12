# ----PART OF SPEECH TAGGING PIPELINE---- #

# This notebook allows us to run the impostor method on part-of-speech tagged impostors.

# ----LOAD LIBRARIES----
suppressPackageStartupMessages({
  library(idiolect)
  library(quanteda)
  library(dplyr)
})

# ----RUN SCRIPT TO INCLUDE FUNCTIONS----
source("/Users/user/Documents/GitHub/para_verifyr/rank_based_impostors_blog.R")

# ----SET LOCATIONS----
base <- "/Users/user/Documents/datasets/blogger/raw_error_fix/"

unknown_loc <- paste0(base, "known_final.jsonl") # Got known and unknown wrong way around compare to impostors
known_loc <- paste0(base, "unknown_final.jsonl") # Got known and unknown wrong way around compare to impostors
metadata_loc <- paste0(base, "metadata.jsonl")    # Contains sample information

# This is the option with any authors present in the metadata filtered out.
# No impostors have been selected yet.
raw_filtered_for_authors_loc <- paste0(base, "raw_filtered_authors.jsonl")

# ----LOAD DATA----

metadata <- read_jsonl(metadata_loc)
known <- read_jsonl(known_loc) |> sample_id_from_metadata(metadata, df_type = "x")
unknown <- read_jsonl(unknown_loc) |> sample_id_from_metadata(metadata, df_type = "y")

raw_filtered_for_authors <- read_jsonl(raw_filtered_for_authors_loc)

# ----GET MAX SIM IMPOSTORS----
# Convert known and raw to dfms
known_dfm <- convert_to_dfm(known)
raw_dfm <- convert_to_dfm(raw_filtered_for_authors)

# return the impostors
max_sim_impostors <- preprocess_dfm(known_dfm, raw_dfm)

# Filter the raw df to just select the docs included 
impostors_to_pos <- docvars(max_sim_impostors) |> as_tibble() |> pull(doc_id) |> unique()

raw_filtered_max_sim <- raw_filtered_for_authors |> filter(doc_id %in% impostors_to_pos)

# ----PART OF SPEECH TAGGING----
# Convert loaded data into corpus objects
known_corpus <- corpus(known)
unknown_corpus <- corpus(unknown)
raw_corpus <- corpus(raw_filtered_max_sim)

known_pos <- idiolect::contentmask(known_corpus)
unknown_pos <- idiolect::contentmask(unknown_corpus)
raw_pos <- idiolect::contentmask(raw_corpus)

# ----MERGE THE POS REF DOCS BACK INTO THE MAX SIM----
max_impostors_df <- docvars(max_sim_impostors) |> as_tibble() |> relocate(sample_id)
raw_pos_df <- raw_pos |> convert(to= c("data.frame", "json"), pretty=FALSE) |> select(doc_id, text)
raw_pos_df$doc_id <- as.integer(raw_pos_df$doc_id)

max_impostors_merged_df <- max_impostors_df |> left_join(raw_pos_df, by = 'doc_id')

# ----CONVERT TO DFM----
known_pos_dfm <- known_pos |> convert(to= c("data.frame", "json"), pretty=FALSE) |> convert_to_dfm()
unknown_pos_dfm <- unknown_pos |> convert(to= c("data.frame", "json"), pretty=FALSE) |> convert_to_dfm()
impostor_pos_dfm <- max_impostors_merged_df |> convert_to_dfm()

# ----RUN THE IMPOSTOR METHOD----
for (i in 1:5) {
  # Run the function
  results_vs_known_rep <- rank_based_impostors(known_pos_dfm, unknown_pos_dfm, impostor_pos_dfm,
                                               num_feats = 100000,
                                               num_repetitions = 100,
                                               num_impostors = 100)
  
  # Construct the filename
  filename <- paste0(base, "/results_pos_after_max_sim/results_pos_after_max_sim_", i, ".jsonl")
  
  # Save the result
  save_jsonl(results_vs_known_rep, filename)
}
