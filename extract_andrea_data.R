#--------------------------------------------------#
#----------EXTRACT FILES FROM ANDREA DATA----------#
#--------------------------------------------------#

library(jsonlite)

# ----LOAD THE FILES---- #

base_loc <- "/Users/user/Documents/datasets/author_verification/"

# corpus_type <- "training"
corpus_type <- "test"

corpus <- readRDS(paste0(base_loc, corpus_type, "/corpus_raw.rds"))
metadata <- readRDS(paste0(base_loc, corpus_type, "/metadata.rds"))

base_file_save_loc <- paste0(base_loc, corpus_type)

# ----GATHER KNOWN AND UNKNOWN---- #
known_corpus <- quanteda::corpus_subset(corpus, texttype == 'known')
unknown_corpus <- quanteda::corpus_subset(corpus, texttype == 'unknown')

saveRDS(known_corpus, paste0(base_loc, corpus_type, "/known_raw.rds"))
saveRDS(unknown_corpus, paste0(base_loc, corpus_type, "/unknown_raw.rds"))

known_df <- quanteda::convert(known_corpus, "data.frame")
unknown_df <- quanteda::convert(unknown_corpus, "data.frame")

# Function to save a data frame to a JSONL file
save_jsonl <- function(data_frame, file_path) {
  
  # Open a connection to the file for writing
  con <- file(file_path, open = "w")
  
  # Ensure the connection is closed properly
  on.exit(close(con), add = TRUE)
  
  # Iterate over each row of the data frame
  for (i in 1:nrow(data_frame)) {
    # Convert the row to a JSON string
    json_line <- jsonlite::toJSON(data_frame[i, ], auto_unbox = TRUE)
    
    # Write the JSON string to the file
    writeLines(json_line, con)
  }
}

parse_df <- function(data_frame, base_file_path, known_or_unknown='known'){
  
  # Get the unique corpus items
  unique_corpus_names <- data_frame |>
    dplyr::pull(corpus) |> 
    unique()
  
  for(corp in unique_corpus_names){
    
    dir_path <- paste0(base_file_path, "/", corp)
    
    # Check if the directory exists, and create it if it doesn't
    if (!file.exists(dir_path)) {
      dir.create(dir_path, recursive = TRUE)
      message("Directory created: ", dir_path)
    } else {
      message("Directory already exists: ", dir_path)
    }
    
    filtered_df <- data_frame |> dplyr::filter(corpus == corp)
    
    final_file_path <- paste0(dir_path, "/", known_or_unknown, "_raw.jsonl")
    
    save_jsonl(filtered_df, final_file_path)
  }
}

# ----CREATE THE KNOWN AND UNKNOWN DATA---- #

parse_df(known_df, base_file_save_loc, 'known')
parse_df(unknown_df, base_file_save_loc, 'unknown')
