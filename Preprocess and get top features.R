#---------------------------------------------------#
#----------PREPROCESS AND GET TOP FEATURES----------#
#---------------------------------------------------#

# This script pulls in the documents from the four locations.
# It then preprocesses the documents, gets the top features and
# saves the dfm matrices in respective folders.
# This is done so that we can run the code in parallel on the csf.

# -----RUN SOURCE FILE TO GET THE FUNCTIONS IN GLOBAL ENVIRONMENT----- #
source("/Users/user/Documents/GitHub/para_verifyr/rank_based_impostors_v2.R")

# -----SET LOCATIONS----- #
base <- "/Users/user/Documents/datasets/blogger/raw_error_fix/"

unknown_loc <- paste0(base, "known_final.jsonl") # Got known and unknown wrong way around compare to impostors
known_loc <- paste0(base, "unknown_final.jsonl") # Got known and unknown wrong way around compare to impostors
metadata_loc <- paste0(base, "metadata.jsonl")

impostor_loc <- "/Users/user/Documents/datasets/blogger/gpt-4o-lex-impostors-qual.jsonl"

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

top_feature_list <- top_n_features_v2(known, unknown, max_sim_impostors_dfm, n_feats=100000)

write.csv(top_feature_list, paste0(base, "top_features.csv"), row.names = FALSE)

sample_list <- metadata$sample_id

for(s in sample_list){
  
  known_sample <- known |> dfm_subset(sample_id == s) |> saveRDS(file=paste0(base,"impostors_data/known/sample_", s, ".RDS"))
  unknown_sample <- unknown |> dfm_subset(sample_id == s) |> saveRDS(file=paste0(base,"impostors_data/unknown/sample_", s, ".RDS"))
  impostors_sample <- max_sim_impostors_dfm |> dfm_subset(sample_id == s) |> saveRDS(file=paste0(base,"impostors_data/impostor/sample_", s, ".RDS"))
  
  
}
