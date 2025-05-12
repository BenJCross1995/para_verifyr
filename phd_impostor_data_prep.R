# ------------------------------------------------------------------------------ #
# ----------------------ANDREA DATASET IMPOSTORS DATA PREP---------------------- #
# ------------------------------------------------------------------------------ #

# -----RUN SOURCE FILE TO GET THE FUNCTIONS IN GLOBAL ENVIRONMENT----- #
source("/Users/user/Documents/GitHub/para_verifyr/rank_based_impostors_v2.R")

create_temp_doc_id <- function(input_text) {
  # Extract everything between the brackets
  match <- regmatches(input_text, regexpr("\\[(.*?)\\]", input_text, perl = TRUE))
  
  if (length(match) > 0) {
    extracted_text <- sub("\\[|\\]", "", match)  # Remove brackets
  } else {
    extracted_text <- input_text
  }
  
  # Replace punctuation and spaces with "_", then reduce multiple underscores to one
  cleaned_text <- gsub("[^[:alnum:]]", "_", extracted_text)
  cleaned_text <- gsub("_+", "_", cleaned_text)
  
  # Remove any leading or trailing "_"
  final_text <- gsub("^_+|_+$", "", cleaned_text)
  
  return(tolower(final_text))
}

# -----SET LOCATIONS----- #
base <- "/Volumes/BCross/datasets/author_verification"
data_type <- "training"
selected_corpus <- "The Telegraph"

known_loc <- paste0(base, "/", data_type, "/", selected_corpus, "/known_raw.jsonl")
unknown_loc <- paste0(base, "/", data_type, "/", selected_corpus, "/unknown_raw.jsonl")
metadata_loc <- paste0(base, "/", data_type, "/metadata.rds")
impostor_loc <- paste0(base, "/", data_type, "/", selected_corpus, "/batch_sentence_impostors_top_500.jsonl")

feature_base_loc <- paste0(base, "/", data_type, "/",
                           selected_corpus,
                           "/batch_sentence_impostor_test/")
known_base_loc <- paste0(base, "/", data_type, "/",
                           selected_corpus,
                           "/batch_sentence_impostor_test/known_dfm/")
unknown_base_loc <- paste0(base, "/", data_type, "/",
                           selected_corpus,
                           "/batch_sentence_impostor_test/unknown_dfm/")
impostor_base_loc <- paste0(base, "/", data_type, "/",
                           selected_corpus,
                           "/batch_sentence_impostor_test/impostor_dfm/")


# -----READ DATA----- #
metadata <- readRDS(metadata_loc) |>
  filter(corpus == selected_corpus) |>
  mutate(sample_id = row_number()) |>
  relocate(sample_id)

known <- read_jsonl(known_loc)
unknown <- read_jsonl(unknown_loc)
impostors <- read_jsonl(impostor_loc)

# -----ADD SAMPLE ID----- #

# Unknown has 1 doc per author but known has 2,
# Here we find the sample id for unknown docs
unknown_with_sample_id <- metadata |>
  select(sample_id, unknown_author) |>
  rename('author' = 'unknown_author') |>
  left_join(unknown, by = 'author')

known_with_sample_id <- known |> 
  mutate('temp_doc_id' = create_temp_doc_id(doc_id),
         author_doc_num = sub(".*_", "", temp_doc_id),
         sample_id = row_number()) |>
  relocate(sample_id)

impostors_with_sample_id <- impostors %>%
  left_join(known_with_sample_id %>% select(sample_id, temp_doc_id),
            by = c('doc_id' = 'temp_doc_id')) |>
  rename('text' = 'rephrased') |>
  relocate(sample_id)

# -----CREATE DFM----- #

known_dfm <- convert_to_dfm(known_with_sample_id)
unknown_dfm <- convert_to_dfm(unknown_with_sample_id)
impostor_dfm <- convert_to_dfm(impostors_with_sample_id)

# -----GET TOP FEATURES----- #

top_feature_list <- top_n_features(known_dfm, unknown_dfm, impostor_dfm, n_feats=100000)

# -----SAVE THE PREPROCESSED DATA----- #

saveRDS(top_feature_list, file=paste0(feature_base_loc, "top_features.RDS"))
saveRDS(metadata, file=paste0(feature_base_loc, "metadata.RDS"))

sample_list <- metadata$sample_id

for(s in sample_list){
  
  known_sample <- known_dfm |> dfm_subset(sample_id == s) |> saveRDS(file=paste0(known_base_loc, "sample_", s, ".RDS"))
  unknown_sample <- unknown_dfm |> dfm_subset(sample_id == s) |> saveRDS(file=paste0(unknown_base_loc,"sample_", s, ".RDS"))
  impostors_sample <- impostor_dfm |> dfm_subset(sample_id == s) |> saveRDS(file=paste0(impostor_base_loc,"sample_", s, ".RDS"))

}