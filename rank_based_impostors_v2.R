# Blogger Impostors

suppressPackageStartupMessages({
  library(jsonlite)
  library(dplyr)
  library(authorverifyr)
  library(quanteda)
  library(quanteda.textstats)
  library(foreach)
  library(parallel)
  library(doParallel)
  library(tidyr)
})

# ---- HELPER FUNCTIONS ---- #

# Function to read JSONL files and return a data frame
read_jsonl <- function(file_path) {
  
  # Read the JSONL file line by line
  lines <- readLines(file_path)
  
  # Initialize an empty list to store parsed data and track errors
  parsed_data <- list()
  problematic_lines <- list()
  
  # Parse each line as JSON with error handling
  for (i in seq_along(lines)) {
    line <- lines[i]
    tryCatch({
      parsed_data[[i]] <- fromJSON(line)
    }, error = function(e) {
      problematic_lines[[length(problematic_lines) + 1]] <- list(line_number = i, content = line, error_message = e$message)
    })
  }
  
  # Print out problematic lines
  if (length(problematic_lines) > 0) {
    message("The following lines caused errors:")
    for (issue in problematic_lines) {
      message(sprintf("Line %d: %s\nError: %s", issue$line_number, issue$content, issue$error_message))
    }
  }
  
  # Check for field inconsistencies and standardize
  combined_data <- bind_rows(lapply(parsed_data, function(x) {
    if (is.list(x$rephrased)) {
      x$rephrased <- toString(x$rephrased)  # Convert lists to strings
    }
    x
  }))
  
  # Return the combined data frame
  return(combined_data)
}

# Function to save a data frame to a JSONL file
save_jsonl <- function(data_frame, file_path) {
  
  # Open a connection to the file for writing
  con <- file(file_path, open = "w")
  
  # Iterate over each row of the data frame
  for (i in 1:nrow(data_frame)) {
    # Convert the row to a JSON string
    json_line <- toJSON(data_frame[i, ], auto_unbox = TRUE)
    
    # Write the JSON string to the file
    writeLines(json_line, con)
  }
  
  # Close the file connection
  close(con)
}

# Add the sample_id to the tables
sample_id_from_metadata <- function(df, metadata, df_type="unknown"){
  
  # TODO - add check for only unknown and known
  
  # Convert the user input to a colname
  col_name <- paste0("doc_id_", df_type)
  
  # Only keep sample_id, relevant doc_id then rename the doc_id
  # for an easier join
  metadata_options <- metadata |>
    select(sample_id, all_of(col_name))
  colnames(metadata_options) <- c('sample_id', "doc_id")
  
  # Join the dataframes and order by sample_id
  result <- df |>
    dplyr::left_join(metadata_options, by = 'doc_id') |>
    dplyr::relocate(sample_id) |>
    dplyr::arrange(sample_id)
  
  return(result)
}

# Function to convert the dataframes to corpus objects
docs_to_corpus <- function(df){
  
  # The function adds an index as a unique id is needed then coverts to 
  # a corpus object
  corpus_object <- df |> 
    dplyr::mutate(index = row_number()) |>
    dplyr::relocate(index) |>
    quanteda::corpus(text_field = 'text', docid_field = 'index')
  
  return(corpus_object) 
}

# Get the top_n features from the known and unknown dfms
top_n_features <- function(known_dfm, unknown_dfm, impostor_dfm, n_feats = 100000){
  
  # Get the feature frequencies and keep the top n
  top_feats <- sort(quanteda::featfreq(rbind(known_dfm, unknown_dfm, impostor_dfm)),
                    decreasing = TRUE) |>
    utils::head(n_feats) |>
    names()
  
  return(top_feats)
  
}

min_max_similarity <- function(row1, row2) {
  min_vals <- pmin(row1, row2)
  max_vals <- pmax(row1, row2)
  similarity <- sum(min_vals) / sum(max_vals)
  return(similarity)
}

# Convert the data to dfm
convert_to_dfm <- function(df){
  
  dfm_df <- character_n_grams(docs_to_corpus(df))
  
  return(dfm_df)
}

get_top_impostors <- function(dfm_x, dfm_ref, num_top = 500){
  
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

# Define the function
preprocess_dfm <- function(dfm_x, dfm_ref, num_top = 500) {
  
  # Initialize a list to store filtered DFMs
  filtered_dfms <- list()
  
  # Grab the sample id's from the known dfm
  sample_ids <- quanteda::docvars(dfm_x, field = 'sample_id')
  
  # Loop through each sample_id
  for (s in sample_ids) {
    
    print(paste0("Sample ID: ", s))
    # Subset DFMs for the current sample_id
    dfm_x_sample <- quanteda::dfm_subset(dfm_x, sample_id == s)
    if( 'sample_id' %in% quanteda::docvars(dfm_ref)){
      dfm_ref_sample <- quanteda::dfm_subset(dfm_ref, sample_id == s)
      
    } else {
      dfm_ref_sample <- dfm_ref
      docvars(dfm_ref_sample)['sample_id'] <- s
    }
    
    top_dfm_ref_sample <- get_top_impostors(dfm_x_sample, dfm_ref_sample)
    
    # Store the filtered dfm
    filtered_dfms[[s]] <- top_dfm_ref_sample
  }
  
  # Combine all filtered DFMs
  filtered_dfm_ref <- do.call(rbind, filtered_dfms)
  
  return(filtered_dfm_ref)
}

preprocess_dfm_parallel <- function(dfm_x, dfm_ref, num_top = 500) {
  
  # Initialize a list to store filtered DFMs
  filtered_dfms <- list()
  
  # Grab the sample id's from the known dfm
  sample_ids <- docvars(dfm_x, field = 'sample_id')
  
  # Set up parallel backend
  num_cores <- detectCores() - 2  # Use one less than the number of available cores
  cl <- makeCluster(num_cores)
  registerDoParallel(cl)
  
  # Use foreach to loop through each sample_id in parallel
  filtered_dfms <- foreach(s = sample_ids, .packages = 'quanteda', .combine = rbind,
                           .export = c('get_top_impostors', 'min_max_similarity')) %dopar% {
                             print(paste0("Sample ID: ", s))
                             # Subset DFMs for the current sample_id
                             dfm_x_sample <- dfm_subset(dfm_x, sample_id == s)
                             dfm_ref_sample <- dfm_subset(dfm_ref, sample_id == s)
                             
                             top_dfm_ref_sample <- get_top_impostors(dfm_x_sample, dfm_ref_sample)
                             
                             # Return the filtered dfm
                             return(top_dfm_ref_sample)
                           }
  
  # Stop the parallel backend
  stopCluster(cl)
  
  return(filtered_dfms)
}

# The version comparing impostors to the known docs
rank_based_impostors_v2 <- function(known_dfm, unknown_dfm, impostor_dfm, features = NULL,
                                    num_impostors = 100, num_repetitions = 100){
  
  print("Beginning Rank-Based Impostor Method")
  
  # Print out num features if less than user specified
  print(paste0("Feature Universe Size: ", length(features)))
  
  # Convert to relative weightings
  known_weighted <- quanteda::dfm_weight(known_dfm, scheme = "prop")
  unknown_weighted <- quanteda::dfm_weight(unknown_dfm, scheme = "prop")
  impostor_weighted <- quanteda::dfm_weight(impostor_dfm, scheme = "prop")
  
  result_df <- data.frame()
  
  score_d_known <- 0
  
  for(i in 1:num_repetitions){
    
    print(paste0("Repetition: ", i, " Out of ", num_repetitions))
    
    # Select impostors and 50% of features
    selected_impostors <- quanteda::dfm_sample(impostor_weighted, size = num_impostors)
    selected_feats <- sample(features, size = length(features) / 2)
      
    # Match the three dfm matrices by the selected feats vector
    known_matched <- quanteda::dfm_match(known_weighted, selected_feats)
    unknown_matched <- quanteda::dfm_match(unknown_weighted, selected_feats)
    impostor_matched <- quanteda::dfm_match(selected_impostors, selected_feats)
      
    # Get the score of the known doc vs the unknown doc
    score_known <- min_max_similarity(as.numeric(known_matched),
                                      as.numeric(unknown_matched))
      
    # Get the score for the unknown vs the impostors
    score_ref <- pbapply::pbapply(impostor_matched, 1,
                       function(row) min_max_similarity(unknown_matched[1, ], row))
      
    # Combine reference score with unknown scores and rank them. Using ties.method = 'min'
    # carries out skip ranking
    all_scores <- c(score_known, score_ref)
    ranking <- rank(-all_scores, ties.method = "min")
      
    # Get the rank of the unknown doc
    pos <- ranking[1]
      
    # Increment the score with each repetition
    score_d_known <- score_d_known + 1 / (num_repetitions * pos)
      
  }
    
  # Save necessary details
  sample_results <- cbind('x_id' = docvars(known_dfm)$doc_id,
                          'y_id' = docvars(unknown_dfm)$doc_id,
                          'score' = score_d_known)
    
  result_df <- rbind(result_df, sample_results)
  
  return(result_df)
}

