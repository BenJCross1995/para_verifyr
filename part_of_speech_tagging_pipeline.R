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

# ----PART OF SPEECH TAGGING----
# Convert loaded data into corpus objects
known_corpus <- corpus(known)
unknown_corpus <- corpus(unknown)
raw_corpus <- corpus(raw_filtered_for_authors)

known_pos <- idiolect::contentmask(known_corpus)
unknown_pos <- idiolect::contentmask(unknown_corpus)
raw_pos <- idiolect::contentmask(raw_corpus)

# ----CONVERT TO DFM----
known_pos_dfm <- known_pos |> convert(to= c("data.frame", "json"), pretty=FALSE) |> convert_to_dfm()
unknown_pos_dfm <- unknown_pos |> convert(to= c("data.frame", "json"), pretty=FALSE) |> convert_to_dfm()
raw_pos_dfm <- raw_pos |> convert(to= c("data.frame", "json"), pretty=FALSE) |> convert_to_dfm()

# ----GET MAX SIM IMPOSTORS----
max_sim_impostors <- preprocess_dfm(known_pos_dfm, raw_pos_dfm)

# ----RUN THE IMPOSTOR METHOD----
for (i in 1:5) {
  # Run the function
  results_vs_known_rep <- rank_based_impostors(known_pos_dfm, unknown_pos_dfm, max_sim_impostors,
                                               num_feats = 100000,
                                               num_repetitions = 100,
                                               num_impostors = 100)
  
  # Construct the filename
  filename <- paste0(base, "/gpt_4o_results/results_pos_", i, ".jsonl")
  
  # Save the result
  save_jsonl(results_vs_known_rep, filename)
}
