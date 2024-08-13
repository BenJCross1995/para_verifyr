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
top_n_features <- function(known_dfm, unknown_dfm, n_feats = 100000){
  
  # Get the feature frequencies and keep the top n
  top_feats <- sort(quanteda::featfreq(rbind(known_dfm, unknown_dfm)), decreasing = TRUE) |>
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


# Get the top n similar docs
get_top_impostors <- function(dfm_x, dfm_ref, num_top = 500) {
  
  # Combining the results which merges features
  combined <- rbind(dfm_x, dfm_ref)
  
  # Get the full tf-idf matrix
  combined_tfidf <- quanteda::dfm_tfidf(combined)
  
  # Get back to originals
  known <- combined_tfidf[1,]
  imps <- combined_tfidf[-1,]
  
  # Calculate similarity scores
  score_ref <- apply(imps, 1, function(row) min_max_similarity(known, row))
  
  # Get the indices of the top similar documents
  top_indices <- order(score_ref, decreasing = TRUE)[1:num_top]
  top_indices <- order(top_indices)

  # Filter the dfm_ref to keep only the top scoring rows
  top_dfm_ref <- dfm_ref[top_indices, ]
  
  return(top_dfm_ref)
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
rank_based_impostors <- function(dfm_x, dfm_y, dfm_ref, num_impostors = 100,
                                 num_feats = 100000, num_repetitions = 100){
  
  # Get the top features from the known and unknown docs, we will use 50% of these
  top_features <- top_n_features(dfm_x, dfm_y, n_feats = num_feats)

  # Print out num features if less than user specified
  if(length(top_features) < num_feats){
    print(paste0("Feature Universe Size: ", length(top_features)))
  }
  
  # Grab the sample id's from the known dfm
  sample_ids <- quanteda::docvars(dfm_x, field = 'sample_id')
  
  result_df <- data.frame()
  
  print("Beginning Rank-Based Impostor Method")
  for(s in sample_ids){
    # Filter the full dfms to only include the correct sample
    x_sample <- quanteda::dfm_subset(dfm_x, sample_id == s)
    y_sample <- quanteda::dfm_subset(dfm_y, sample_id == s)
    ref_sample <- quanteda::dfm_subset(dfm_ref, sample_id == s)
    
    # To be used in results dataframe
    x_id <- quanteda::docvars(x_sample, field = 'doc_id')
    y_id <- quanteda::docvars(y_sample, field = 'doc_id')
    
    score_d_known <- 0
    
    for(i in 1:num_repetitions){
      
      print(paste0("Sample ID: ", s, " - Repetition: ", i, " Out of ", num_repetitions))
      # Select 50% of features
      selected_refs <- quanteda::dfm_sample(ref_sample, size = num_impostors)
      selected_feats <- sample(top_features, size = length(top_features) / 2)

      # Match the three dfm matrices by the selected feats vector
      x_matched <- quanteda::dfm_match(x_sample, selected_feats)
      y_matched <- quanteda::dfm_match(y_sample, selected_feats)
      ref_matched <- quanteda::dfm_match(selected_refs, selected_feats)
      
      # Get the score of the known doc vs the unknown doc
      # TODO - Deal with this: <sparse>[ <logic> ]: .M.sub.i.logical() maybe inefficient
      # Maybe quanteda::dfm_trim(rbind(x, y), min_termfreq = 1, termfreq_type = 'count')
      score_known <- min_max_similarity(x_matched[1, ], y_matched[1, ])
      
      # Get the score for the unknown vs the impostors
      score_ref <- apply(ref_matched, 1, function(row) min_max_similarity(y_matched[1, ], row))
      
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
    sample_results <- cbind('sample_id' = s,
                            'x_id' = x_id,
                            'y_id' = y_id,
                            'score' = score_d_known)
    
    result_df <- rbind(result_df, sample_results)
  }
  
  return(result_df)
}
