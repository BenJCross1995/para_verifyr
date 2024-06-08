suppressPackageStartupMessages({
  library(jsonlite)
  library(dplyr)
  library(authorverifyr)
  library(quanteda)
})

read_jsonl <- function(file_path) {
  
  require(jsonlite)
  
  # Step 2: Read the JSONL file line by line
  lines <- readLines(file_path)
  print(lines[25465])
  # Step 3: Parse each line as JSON
  parsed_data <- lapply(lines, fromJSON)
  
  # Step 4: Combine into a data frame using bind_rows to handle differing columns
  data_frame <- bind_rows(parsed_data)
  
  # Return the data frame
  return(data_frame)
}

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

google_drive_loc <- "/Users/user/Library/CloudStorage/GoogleDrive-benjcross1995@gmail.com/My Drive/"
guardian_base_file_loc <- paste0(google_drive_loc, "datasets/guardian/guardian_preprocessed.jsonl")
guardian_rephrased_phi_loc <- paste0(google_drive_loc, "datasets/guardian/guardian_phi/processed/rephrased.jsonl")

guardian_base <- read_jsonl(guardian_base_file_loc)
guardian_phi <- read_jsonl(guardian_rephrased_phi_loc)
doc_info <- guardian_base |>
  group_by(id) |>
  summarise(author_id = min(author),
            topic_id = min(topic)) |>
  rename('doc_id' = 'id')

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

x_y_info <- get_x_y_info(guardian_base, guardian_phi)

min_max_similarity <- function(row1, row2) {
  min_vals <- pmin(row1, row2)
  max_vals <- pmax(row1, row2)
  similarity <- sum(min_vals) / sum(max_vals)
  return(similarity)
}

known_docs_metadata <- x_y_info$x
unknown_docs_metadata <- x_y_info$y

known_docs <- add_text_to_metadata(guardian_base, known_docs_metadata)
unknown_docs <- add_text_to_metadata(guardian_base, unknown_docs_metadata)

ref_docs <- guardian_phi |>
  left_join(unknown_docs_metadata, by = 'doc_id') |>
  rename('text' = 'rephrased') |>
  left_join(doc_info, by = 'doc_id') |>
  select(sample_id, doc_id, chunk_id, subchunk_id,
         author_id.x, topic_id, text) |>
  rename('author_id' = 'author_id.x')

unknown_updated <- filter_zero_paraphrases(unknown_docs, ref_docs)

impostor_algorithm <- function(known, unknown, ref, n_rep, save_loc = NULL){
  
  # Get a list of the sample id's
  samples <- known |>
    pull(sample_id) |>
    unique()
  
  result_df <- data.frame()
  for(sample_id in samples){
    
    # Filter the docs for the correct sample_id and convert to a corpus
    known_corp <- sample_doc_to_corpus(known, sample_id)
    unknown_corp <- sample_doc_to_corpus(unknown, sample_id)
    ref_corp <- sample_doc_to_corpus(ref, sample_id)
    
    # Get the dfm matrix for each corpus
    dfm_known <- character_n_grams(known_corp)
    dfm_unknown <- character_n_grams(unknown_corp)
    dfm_ref <- character_n_grams(ref_corp)
    
    # Get all of the known doc features, we will use 50% of these in the
    # result
    known_feats <- colnames(dfm_known)
  
    # Get the number of sentences in the known and unknown corpus
    num_known_sentence <- ndoc(known_corp)
    num_unknown_sentence <- ndoc(unknown_corp)
    num_total_ref_sentences <- ndoc(ref_corp)
    
    # Create vectors of FALSE, we will replace each element with TRUE in a 
    # loop to subset the dfms
    base_known_false <- rep(FALSE, num_known_sentence)
    base_unknown_false <- rep(FALSE, num_unknown_sentence)
    
    for(i in 1:num_known_sentence){
      
      # subset the known dfm
      updated_known_false <- replace(base_known_false, i, TRUE)
      dfm_known_subset <- dfm_subset(dfm_known, updated_known_false)
      known_docvars <- docvars(dfm_known_subset[1,])
      
      # Initialise a vector to store all scores for the known sentence i
      score_vec <- c()
      
      for(j in 1:num_unknown_sentence){
        
        # Subset the unknown dfm as we will be looping through this
        updated_unknown_false <- replace(base_unknown_false, j, TRUE)
        dfm_unknown_subset <- dfm_subset(dfm_unknown, updated_unknown_false)
        
        # Now we grab the chunk and subchunk from the docvars of the subset
        docvar_selection <- docvars(dfm_unknown_subset[1,])
        unknown_chunk_id <- docvar_selection$chunk_id
        unknown_subchunk_id <- docvar_selection$subchunk_id
        
        # Use this info to subset the reference dfm
        dfm_ref_subset <- dfm_subset(dfm_ref, chunk_id == unknown_chunk_id & subchunk_id == unknown_subchunk_id)
        
        # Get the number of paraphrases in order to set a score multiplier
        num_subset_ref_sentences <- ndoc(dfm_ref_subset)
        score_multiplier <- num_subset_ref_sentences / num_total_ref_sentences

        score_d_known <- 0
        
        # Repeat a number of times set by the user
        for(k in 1:n_rep){
          
          # Select 50% of features
          selected_feats <- sample(known_feats, size = length(known_feats) / 2)
          
          # Match the dfms with these features
          known_matched <- dfm_match(dfm_known_subset, selected_feats)
          unknown_matched <- dfm_match(dfm_unknown_subset, selected_feats)
          ref_matched <- dfm_match(dfm_ref_subset, selected_feats)
          
          # Calculate the unknown score and create a vector of reference scores
          score_unknown <- min_max_similarity(as.numeric(known_matched[1,]),
                                            as.numeric(unknown_matched[1, ]))
          
          score_ref <- apply(ref_matched, 1, function(row) min_max_similarity(as.numeric(known_matched[1,]),
                                                                              as.numeric(row)))
          
          # Combine reference score with unknown scores and rank them. Using ties.method = 'min'
          # carries out skip ranking
          all_scores <- c(score_unknown, score_ref)
          ranking <- rank(-all_scores, ties.method = "min")
          
          # Get the rank of the unknown doc
          pos <- ranking[1]
          
          # Increment the score with each repetition
          score_d_known <- score_d_known + 1 / (n_rep * pos)
          
        }
        
        # Multiply the score by the score_multiplier and add it to the vectore with scores for all sentences
        final_score <- score_d_known * score_multiplier
        print(paste0("Sample: ", sample_id, " - Sentence: ", i, " - Unknown Sentence: ", j, " - Score Before Multiplier: ", score_d_known, " - Final Score: ", final_score))
        score_vec <- c(score_vec, final_score)
      }
      
      
      # Now we get the score for sentence i vs the entire unknown document
      # I am summing instead of averaging as i'm essentially doing a weighted average by
      # multiplying by the score_multiplier
      sentence_score <- sum(score_vec)
      print(paste0("Sample: ", sample_id, " - Sentence: ", i, " - Sentence Score: ", sentence_score))
      
      # Compute same_author value based on the sentence score
      if (sentence_score < 0.5) {
        same_author <- 0
      } else if (sentence_score == 0.5) {
        same_author <- 0.5
      } else {
        same_author <- 1
      }
      
      sentence_info <- cbind(known_docvars, sentence_score, same_author)
      result_df <- rbind(result_df, sentence_info)
    }
    
    # Save after each document
    if (!is.null(save_loc)) {
      write.csv(result_df, file = save_loc, row.names = FALSE)
    }
  }
  return(result_df)
}


result <- impostor_algorithm(known_docs, unknown_updated, ref_docs, n_rep = 10, save_loc = "./guardian_phi_results_10_reps.csv")
result

