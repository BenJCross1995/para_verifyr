# Load necessary libraries
library(tidyverse)
library(jsonlite)
library(ggplot2)

# Set the folder path where your .jsonl files are stored
folder_path <- "/Users/user/Documents/datasets/blogger/raw_error_fix/results"
base <- "/Users/user/Documents/datasets/blogger/raw_error_fix/"
metadata_loc <- paste0(base, "metadata.jsonl")

# ----------HELPER FUNCTIONS---------- #

# Function to read jsonl files
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
      json_content <- fromJSON(line)
      if (is.list(json_content) && length(json_content) == 1) {
        parsed_data[[i]] <- json_content[[1]]
      } else {
        parsed_data[[i]] <- json_content
      }
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
  
  # Combine parsed data into a single data frame
  combined_data <- bind_rows(parsed_data)
  
  # Return the combined data frame
  return(combined_data)
}

# Function to read each file and add the required columns
read_and_augment <- function(file) {
  # Read the .jsonl file
  data <- read_jsonl(file)
  
  data$sample_id = as.numeric(data$sample_id)
  data$x_id = as.numeric(data$x_id)
  data$y_id = as.numeric(data$y_id)
  data$score = as.double(data$score)
  
  # Extract the file name without the path
  file_name <- basename(file)
  
  # Remove the .jsonl extension
  base_name <- tools::file_path_sans_ext(file_name)
  
  # Extract the numeric suffix at the end (for the repetition column)
  repetition_value <- as.integer(str_extract(base_name, "\\d+$"))
  
  # Remove the numeric suffix at the end to get the prefix
  prefix_name <- sub("_\\d+$", "", base_name)
  
  # Add the new columns to the data
  data <- data %>%
    mutate(file_name = base_name,
           prefix_name = prefix_name,
           repetition = repetition_value)
  
  rownames(data) <- NULL  # Reset the row names
  
  return(data)
}

# Function to filter the data for only complete 
filter_complete_sample_ids <- function(df) {
  # Step 1: Count the number of unique prefix_names per sample_id
  sample_prefix_count <- df %>%
    group_by(sample_id) %>%
    summarise(unique_prefix_count = n_distinct(prefix_name)) %>%
    ungroup()
  
  # Step 2: Find the total number of different prefix_names
  total_prefix_names <- df %>%
    summarise(total_prefix_count = n_distinct(prefix_name)) %>%
    pull(total_prefix_count)
  
  # Step 3: Filter to keep only the sample_ids that have a value for every prefix_name
  valid_sample_ids <- sample_prefix_count %>%
    filter(unique_prefix_count == total_prefix_names) %>%
    pull(sample_id)
  
  # Step 4: Subset the original data to keep only rows with the valid sample_ids
  filtered_data <- df %>%
    filter(sample_id %in% valid_sample_ids)
  
  return(filtered_data)
}

# Function to add thresholds to the data
compare_thresholds <- function(df, thresholds) {
  # Initialize an empty dataframe to store the results
  final_df <- data.frame()
  
  # Loop over each threshold value
  for (threshold in thresholds) {
    
    # Create a copy of the dataframe to avoid modifying the original
    df_copy <- df
    
    # Create a new column for the current threshold
    df_copy$threshold <- threshold
    
    # Compare final_score with threshold and create pred_same_author column
    df_copy$same_author <- ifelse(df_copy$same_author == TRUE, 1, 0)
    
    # Compare final_score with threshold and create pred_same_author column
    df_copy$pred_same_author <- ifelse(df_copy$score > threshold, 1,
                                       ifelse(df_copy$score == threshold, 0.5, 0))
    
    df_copy$result = ifelse(df_copy$same_author == df_copy$pred_same_author, 1, 0)
    
    # Append the dataframe to the final_df
    final_df <- rbind(final_df, df_copy)
  }
  
  return(final_df)
}

calculate_metrics <- function(df) {
  df %>%
    group_by(prefix_name, repetition, threshold) %>%
    summarise(
      total_rows = dplyr::n(),
      FP = sum(same_author == FALSE & pred_same_author == TRUE),
      FN = sum(same_author == TRUE & pred_same_author == FALSE),
      TP = sum(same_author == TRUE & pred_same_author == TRUE),
      TN = sum(same_author == FALSE & pred_same_author == FALSE),
      U = sum(pred_same_author == "Undecided"),
      auc = Metrics::auc(same_author, pred_same_author)
    ) %>%
    mutate(
      accuracy = (TP + TN) / total_rows,
      precision = TP / (TP + FP),
      recall = TP / (TP + FN),
      FPR = FP / (FP + TN),
      F1 = 2 * (precision * recall) / (precision + recall)
    )
}

grouped_avg_metrics <- function(df) {
  df |> 
    group_by(prefix_name, threshold) |>  # Group by prefix_name and threshold
    summarise(across(auc:F1, \(x) mean(x, na.rm = TRUE)))  # Calculate the mean of all other columns
}

# ----------

metadata <- read_jsonl(metadata_loc)

# List all .jsonl files in the folder
files <- list.files(path = folder_path, pattern = "\\.jsonl$", full.names = TRUE)
files
# Apply the function to all files and combine them into a single data frame
all_data <- map_dfr(files, read_and_augment)

# Filter the data 
filtered_data <- filter_complete_sample_ids(all_data)

combined_results <- filtered_data |>
  dplyr::left_join(metadata, by = 'sample_id') |>
  dplyr::select(sample_id, doc_id_x, doc_id_y,
                author_id_x, author_id_y, topic_x,
                topic_y, same_author, same_topic,
                file_name, prefix_name, repetition,
                score)

combined_results |> head()

colnames(combined_results)
combined_results |>
  group_by(prefix_name, sample_id, same_author) |>
  summarise('avg_score' = mean(score), .groups="drop")


added_thresholds <- compare_thresholds(combined_results, seq(0, 1, 0.01))

added_thresholds |>
  group_by(prefix_name, sample_id, repetition, threshold) |>
  summarise(accuracy = Metrics::accuracy(same_author, pred_same_author),
            precision = Metrics::precision(same_author, pred_same_author),
            recall = Metrics::recall(same_author, pred_same_author),
            f1 = Metrics::f1(same_author, pred_same_author),
            auc = Metrics::auc(same_author, pred_same_author)) 

result_metrics <- calculate_metrics(added_thresholds)

result_metrics |>
  ggplot(aes(x = FPR, y = recall, fill = as.character(repetition))) +  # Use the corrected column names
  geom_line() +  # Add lines to connect the points for each ROC curve
  geom_point() +  # Optionally add points to see the individual thresholds
  facet_grid(prefix_name ~ .) +  # Facet by 'prefix_name' over rows instead of columns
  labs(x = "False Positive Rate (FPR)", y = "True Positive Rate (Recall)", fill = "Repetition") +  # Label the axes and legend
  theme_minimal()  # Apply a minimal theme for better readability
  

grouped_metrics <- grouped_avg_metrics(result_metrics)
grouped_metrics |>
  group_by(prefix_name) |>
  summarise(avg_auc = mean(auc))
grouped_metrics |>
  ggplot(aes(x = FPR, y = recall, color = prefix_name)) +
  geom_line() +  # Plot the ROC curves
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "black") +  # Add the dashed x = y line
  labs(x = "False Positive Rate (FPR)", y = "True Positive Rate (Recall)", color = "Impostor Type") +
  theme_minimal() +
  ggtitle("Average TPR vs FPR for 5 iterations of Rank-Based Impostors")

grouped_metrics |>
  ggplot(aes(x = threshold, y = accuracy, color = prefix_name)) +
  geom_line() +
  ggtitle("Average Accuracy score for 5 iterations of Rank-Based Impostors")

grouped_metrics |>
  ggplot(aes(x = threshold, y = auc, color = prefix_name)) +
  geom_line() +
  ggtitle("Average AUC score for 5 iterations of Rank-Based Impostors")


# ---- IDIOLECT RESULTS ---- #

library(idiolect)

looped_performance_results <- function(df){
  
  
  # Get the averaged results across runs and 
  logistic_df <- df |>
    group_by(prefix_name, sample_id, same_author) |>
    summarise('score' = mean(score), .groups="drop") |>
    rename('target' = 'same_author')
  
  models <- logistic_df |> pull(prefix_name) |> unique()

  results <- data.frame()
  for(model in models){
    
    performance <- logistic_df |>
      filter(prefix_name == model) |>
      performance()
    
    
    result_df <- performance$evaluation
    result_df <- cbind(model, result_df)
    
    results <- rbind(results, result_df)
  }
  return(results)
}

idiolect_results <- looped_performance_results(combined_results)

idiolect_results

# write_csv(idiolect_results, "/Users/user/Documents/datasets/blogger/results/idiolect_results.csv")

library(grid)

table <- tableGrob(idiolect_results)

grid.draw(table)

# names(docvars(unknown))[names(docvars(unknown)) == "author_id"] <- "author"
# names(docvars(known))[names(docvars(known)) == "author_id"] <- "author"
# names(docvars(max_sim_impostors))[names(docvars(max_sim_impostors)) == "author_id"] <- "author"
# 
# 
# impostors(unknown, known, max_sim_impostors)
