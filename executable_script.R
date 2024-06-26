#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(jsonlite)
  library(dplyr)
  library(authorverifyr)
  library(quanteda)
  library(foreach)
  library(doParallel)
  library(tidyr)
})

# Get command-line arguments
args <- commandArgs(trailingOnly = TRUE)

#----ASSIGN PASSED ARGUMENTS TO VARIABLES----#

known_loc <- args[1]
unknown_loc <- args[2]
save_loc <- args[3]
num_rep <- as.integer(args[4])
sample_size <- as.integer(args[5])

#----CREATE FUNCTIONS----#
setup_parallel_backend <- function() {
  numCores <- detectCores()
  cl <- makeCluster(numCores)
  registerDoParallel(cl)
  return(cl)
}

stop_parallel_backend <- function(cl) {
  stopCluster(cl)
}

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

get_unknown_info <- function(base_df, rephrased_df){
  
  require(dplyr)
  
  set.seed(42)
  
  base_data <- base_df |>
    distinct(id, author)
  
  rephrased_docs <- rephrased_df |>
    select(doc_id) |>
    unique() |>
    left_join(base_data, by = c('doc_id' = 'id'))
  
  # Create a vector with roughly equal TRUE and FALSE values
  num_rephrased <- nrow(rephrased_docs)
  same_author <- sample(c(rep(TRUE, num_rephrased %/% 2), rep(FALSE, num_rephrased - (num_rephrased %/% 2))))
  
  # Add the same_author to the rephrased docs dataframe
  result <- cbind('sample_id' = seq(1, num_rephrased),
                  rephrased_docs,
                  same_author)
  colnames(result) <- c("sample_id", "doc_id", "author_id", "same_author")
  return(result)
  
}

get_x_y_info <- function(base_df, rephrased_df){
  
  require(dplyr)
  
  unknown_info <- get_unknown_info(base_df, rephrased_df)
  set.seed(420)
  
  base_data <- base_df |>
    distinct(id, author)
  
  sample_size <- nrow(unknown_info)
  
  result_df <- data.frame()
  
  for(i in 1:sample_size){
    sample_id <- i
    doc_id <- unknown_info[i, 'doc_id']
    author_id <- unknown_info[i, 'author_id']
    same_author <- unknown_info[i, 'same_author']
    
    if(same_author == TRUE){
      sample_row <- base_data |>
        filter('id' != doc_id) |>
        filter(author == author_id) |>
        sample_n(1) |>
        mutate(same_author = same_author)
    } else {
      sample_row <- base_data |>
        filter('id' != doc_id) |>
        filter(author != author_id) |>
        sample_n(1) |>
        mutate(same_author = same_author)
    }
    
    result_df <- rbind(result_df, sample_row)
    
  }
  
  result_df <- cbind('sample_id' = seq(1, sample_size),
                     result_df)
  
  colnames(result_df) <- c("sample_id", "doc_id", "author_id", "same_author")
  result <- list('x' = result_df,
                 'y' = unknown_info)
  
  return(result)
}

add_text_to_metadata <- function(base_data, metadata){
  
  # Get the number of samples from the metadata
  num_samples = max(metadata$sample_id)
  result <- data.frame()
  
  # We want to filter the metadata for each sample_id
  for(i in 1:num_samples){
    filtered_metadata <- metadata |>
      filter(sample_id == i)
    
    # Then get the doc_id for this sample
    doc_id <- filtered_metadata |>
      select(doc_id) |>
      pull() |>
      as.numeric()
    
    # Get the base data for the specific doc_id
    filtered_base_data <- base_data |>
      filter(id == doc_id)
    
    # Add the sample_id 
    sample_id = rep(i, nrow(filtered_base_data))
    
    result_df <- cbind(sample_id, filtered_base_data)
    
    result <- rbind(result, result_df)
    
  }
  
  # Keep the desired columns and rename them
  result <- result |>
    select(sample_id, id, chunk_id, subchunk_id, author, topic, text)
  
  colnames(result) <- c("sample_id", "doc_id", "chunk_id", "subchunk_id",
                        "author_id", "topic_id", "text")
  
  return(result)
}

filter_zero_paraphrases <- function(unknown, ref){
  
  ref_doc_count <- ref |>
    group_by(sample_id, doc_id, chunk_id, subchunk_id, author_id, topic_id) |>
    summarise(num_paraphrases = n())
  
  unknown_updated <- unknown |>
    inner_join(ref_doc_count, by = c('sample_id', 'doc_id', 'chunk_id', 'subchunk_id',
                                     'author_id', 'topic_id')) |>
    select(-num_paraphrases)
  
  return(unknown_updated)
}

sample_doc_to_corpus <- function(df, samp){
  corpus_object <- df |> 
    filter(sample_id == samp) |>
    mutate(index = row_number()) |>
    relocate(index) |>
    corpus(text_field = 'text', docid_field = 'index')
  
  return(corpus_object)
}

docs_to_corpus <- function(df){
  corpus_object <- df |> 
    mutate(index = row_number()) |>
    relocate(index) |>
    corpus(text_field = 'text', docid_field = 'index')
  
  return(corpus_object) 
}

top_n_features <- function(known_dfm, unknown_dfm, n_feats = 10000){
  
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

#-----RUN THE PREPROCESS FUNCTIONS-----#

preprocess_docs <- function(known_loc, unknown_loc){
  known <- read_jsonl(known_loc)
  unknown <- read_jsonl(unknown_loc)
  
  doc_info <- known |>
    group_by(id) |>
    summarise(author_id = min(author),
              topic_id = min(topic)) |>
    rename('doc_id' = 'id')

  x_y_info <- get_x_y_info(known, unknown)

  known_docs_metadata <- x_y_info$x
  unknown_docs_metadata <- x_y_info$y

  known_docs <- add_text_to_metadata(known, known_docs_metadata)
  unknown_docs <- add_text_to_metadata(known, unknown_docs_metadata)

  ref_docs <- unknown |>
    left_join(unknown_docs_metadata, by = 'doc_id') |>
    rename('text' = 'rephrased') |>
    left_join(doc_info, by = 'doc_id') |>
    select(sample_id, doc_id, chunk_id, subchunk_id,
           author_id.x, topic_id, text) |>
    rename('author_id' = 'author_id.x')

  unknown_updated <- filter_zero_paraphrases(unknown_docs, ref_docs)
  
  docs <- list(known_docs, unknown_docs, ref_docs)
}

impostor_algorithm_inner_parallel <- function(known, unknown, ref, n_rep, save_loc=NULL, sample_size=0){
  
  # Initialise parallel processing
  cl <- setup_parallel_backend()
  
  if(sample_size > 0){
    samples <- unknown |> 
      group_by(sample_id) |> 
      summarise(num_unknown_chunks = n()) |>
      left_join((known |> group_by(sample_id) |> summarise(num_known_chunks = n())), by = 'sample_id') |>
      mutate(total_combinations = num_unknown_chunks * 2 * num_unknown_chunks) |>
      arrange(total_combinations) |>
      head(sample_size) |>
      pull(sample_id)
    
  } else {
    # Get a list of the sample id's
    samples <- known |>
      pull(sample_id) |>
      unique()
  }
  
  # Filter the docs for the correct sample_id and convert to a corpus
  # Then convert to a dfm
  dfm_known <- character_n_grams(docs_to_corpus(known))
  dfm_unknown <- character_n_grams(docs_to_corpus(unknown))
  dfm_ref <- character_n_grams(docs_to_corpus(ref))
  
  # Get the top features from the known and unknown docs, we will use 50% of these
  top_features <- top_n_features(dfm_known, dfm_unknown, n_feats = 10000)
  
  result_df <- data.frame()
  
  for(samp in samples){
    
    print(paste0("Document ", which(samples == samp), " Out Of ", length(samples)))
    
    dfm_known_sample <- dfm_subset(dfm_known, sample_id == samp)
    dfm_unknown_sample <- dfm_subset(dfm_unknown, sample_id == samp)
    dfm_ref_sample <- dfm_subset(dfm_ref, sample_id == samp)
    
    # Get the number of sentences in the known and unknown corpus
    num_known_sentence <- ndoc(dfm_known_sample)
    num_unknown_sentence <- ndoc(dfm_unknown_sample)
    
    sample_result_df <- foreach(i = 1:num_unknown_sentence, .combine = rbind, .packages = c('foreach', 'quanteda', 'dplyr', 'authorverifyr'), .export = c("min_max_similarity", "dfm_subset", "dfm_match", "ndoc")) %dopar% {
      
      dfm_unknown_subset <- dfm_unknown_sample[i,]
      
      # Return the docvars which will be used to filter the reference data
      unknown_docvars <- docvars(dfm_unknown_subset[1,])
      unknown_chunk_id <- unknown_docvars$chunk_id
      unknown_subchunk_id <- unknown_docvars$subchunk_id
      
      # Get the reference data from the docvars of the unknown data
      dfm_ref_subset <- dfm_subset(dfm_ref_sample,
                                   chunk_id == unknown_chunk_id &
                                     subchunk_id == unknown_subchunk_id)
      
      num_paraphrases <- nrow(dfm_ref_subset)
      
      score_vec <- numeric(0)
      
      score_vec <- foreach(j = 1:num_known_sentence, .combine = "c", .packages = c('foreach', 'quanteda'), .export = c("min_max_similarity")) %dopar% {
        
        dfm_known_subset <- dfm_known_sample[j,]
        
        # Now we grab the chunk and subchunk from the docvars of the subset
        known_docvars <- docvars(dfm_known_subset[1,])
        
        score_d_known <- 0
        
        for(k in 1:n_rep){
          
          # Select 50% of features
          selected_feats <- sample(top_features, size = length(top_features) / 2)
          
          # Match the dfms with these features
          known_matched <- dfm_match(dfm_known_subset, selected_feats)
          unknown_matched <- dfm_match(dfm_unknown_subset, selected_feats)
          ref_matched <- dfm_match(dfm_ref_subset, selected_feats)
          
          # Remove any columns with just zeros, score would be zero anyway
          trimmed_k_vs_u <- dfm_trim(rbind(known_matched, unknown_matched),
                                     min_termfreq = 1,
                                     termfreq_type = 'count')
          
          # Remove any columns with just zeros, score would be zero anyway
          trimmed_k_vs_ref <- dfm_trim(rbind(known_matched, ref_matched),
                                       min_termfreq = 1,
                                       termfreq_type = 'count')
          
          # Calculate the unknown score and create a vector of reference scores
          score_unknown <- min_max_similarity(trimmed_k_vs_u[1, ],
                                              trimmed_k_vs_u[2, ])
          
          score_ref <- apply(trimmed_k_vs_ref[-1,], 1,
                             function(row) min_max_similarity(trimmed_k_vs_ref[1, ], row))
          
          # Combine reference score with unknown scores and rank them. Using ties.method = 'min'
          # carries out skip ranking
          all_scores <- c(score_unknown, score_ref)
          ranking <- rank(-all_scores, ties.method = "min")
          
          # Get the rank of the unknown doc
          pos <- ranking[1]
          
          # Increment the score with each repetition
          score_d_known <- score_d_known + 1 / (n_rep * pos)
          
        }
        # This is similar to return(final_score)
        score_d_known
      }
      
      # Now we get the score for sentence i vs the entire unknown document
      # I am summing instead of averaging as i'm essentially doing a weighted average by
      # multiplying by the score_multiplier
      sentence_score <- mean(score_vec)
      
      # Compute same_author value based on the sentence score
      if (sentence_score < 0.5) {
        same_author <- 0
      } else if (sentence_score == 0.5) {
        same_author <- 0.5
      } else {
        same_author <- 1
      }
      
      sentence_info <- cbind(unknown_docvars, num_paraphrases, sentence_score, same_author)
      
      sentence_info
    }
    # Return the sentence information as rbind
    result_df <- rbind(result_df, sample_result_df)
    
    # Save after each document
    if (!is.null(save_loc)) {
      write.csv(result_df, file = save_loc, row.names = FALSE)
    }
    
  }
  stop_parallel_backend(cl)
  return(result_df)
}

preprocessed_data <- preprocess_docs(known_loc, unknown_loc)

known_docs <- preprocessed_data[[1]]
unknown_docs <- preprocessed_data[[2]]
ref_docs <- preprocessed_data[[3]]

impostor_algorithm_inner_parallel(known_docs,
                                  unknown_docs,
                                  ref_docs,
                                  n_rep = num_rep,
                                  save_loc = save_loc,
                                  sample_size = sample_size)