#--------------------------------------------------#
#------------------GATHER RESULTS------------------#
#--------------------------------------------------#

library(idiolect)
library(dplyr)
library(Metrics)

base_loc <- "/Volumes/BCross/datasets/author_verification"

read_results_from_folder <- function(base_loc, data_type, corpus, results_type){
  
  folder_path <- paste0(base_loc, "/", data_type, "/", corpus, "/gpt_4o_mini_full/", results_type)
  
  # -----LIST RESULTS----- #
  rds_files <- list.files(path = folder_path, pattern = "\\.rds$", full.names = TRUE)
  length(rds_files)
  
  # -----LOAD RESULTS----- #
  full_results_table <- data.frame()
  
  for(file in rds_files){
    temp_df <- readRDS(file)
    
    full_results_table <- rbind(full_results_table, temp_df)
    
  }
  
  full_results_table <- full_results_table |>
    dplyr::mutate(data_type = data_type,
                  corpus = corpus,
                  result_type = results_type,
                  same_author = ifelse(known_author == unknown_author, TRUE, FALSE),
                  score = as.numeric(score))
  
  return(full_results_table)
}

# Enron
enron_aggregated <- read_results_from_folder(base_loc, "training", 'Enron', "aggregated_results")
enron_profile <- read_results_from_folder(base_loc, "training", 'Enron', "profile_results")
enron_profile_andrea_im <- read_results_from_folder(base_loc, "training", "Enron", "profile_results_andrea_im")
## LambdaG
enron_aggregated_lambda_g <- read_results_from_folder(base_loc, 'training', 'Enron', 'aggregated_results_andrea_lambdag')
enron_profile_lambda_g <- read_results_from_folder(base_loc, 'training', 'Enron', 'profile_results_andrea_lambdag')

# Wiki
wiki_aggregated <- read_results_from_folder(base_loc, "training", 'Wiki', "aggregated_results")
wiki_profile <- read_results_from_folder(base_loc, "training", 'Wiki', "profile_results")
wiki_profile_andrea_im <- read_results_from_folder(base_loc, "training", "Wiki", "profile_results_andrea_im")
## LambdaG
wiki_aggregated_lambda_g <- read_results_from_folder(base_loc, 'training', 'Wiki', 'aggregated_results_andrea_lambdag')
wiki_profile_lambda_g <- read_results_from_folder(base_loc, 'training', 'Wiki', 'profile_results_andrea_lambdag')

aggregated_results <- function(df){
  
  data_type <- df |> pull(data_type) |> unique()
  corpus <- df |> pull(corpus) |> unique()
  result_type <- df |> pull(result_type) |> unique()
    
  # Get the averaged results across runs and 
  aggregated_df <- df |>
    group_by(corpus, data_type, result_type, problem, same_author) |>
    summarise('score' = mean(score, na.rm = TRUE), .groups="drop")
  
  return(aggregated_df)
}

looped_performance_results <- function(df){
  
  data_type <- df |> pull(data_type) |> unique()
  corpus <- df |> pull(corpus) |> unique()
  result_type <- df |> pull(result_type) |> unique()
  
  # Get the averaged results across runs and 
  logistic_df <- aggregated_results(df) |>
    rename('target' = 'same_author')
  
  performance <- logistic_df |>
    performance()
  
  result_df <- performance$evaluation
  
  result_df <- cbind(corpus, data_type, result_type, result_df)
  return(result_df)
}

wiki_profile_results <- looped_performance_results(wiki_profile)
wiki_aggregated_results <- looped_performance_results(wiki_aggregated)
wiki_profile_andrea_im_results <- looped_performance_results(wiki_profile_andrea_im)
## LambdaG
wiki_aggregated_lambda_g_results <- looped_performance_results(wiki_aggregated_lambda_g)
wiki_profile_lambda_g_results <- looped_performance_results(wiki_profile_lambda_g)

enron_profile_results <- looped_performance_results(enron_profile)
enron_aggregated_results <- looped_performance_results(enron_aggregated)
enron_profile_andrea_im_results <- looped_performance_results(enron_profile_andrea_im)
## LambdaG
enron_aggregated_lambda_g_results <- looped_performance_results(enron_aggregated_lambda_g)
enron_profile_lambda_g_results <- looped_performance_results(enron_profile_lambda_g)

results_df <- rbind(wiki_profile_results, wiki_profile_andrea_im_results, wiki_profile_lambda_g_results, wiki_aggregated_results, wiki_aggregated_lambda_g_results,
                    enron_profile_results, enron_profile_andrea_im_results, enron_profile_lambda_g_results, enron_aggregated_results, enron_aggregated_lambda_g_results)
results_df

# aggregated_results(wiki_profile)
# # -----ADD THRESHOLDS----- #
# compare_thresholds <- function(df, thresholds) {
#   # Initialize an empty dataframe to store the results
#   final_df <- data.frame()
#   
#   # Loop over each threshold value
#   for (threshold in thresholds) {
#     
#     # Create a copy of the dataframe to avoid modifying the original
#     df_copy <- df
#     
#     # Create a new column for the current threshold
#     df_copy$threshold <- threshold
#     
#     # Compare final_score with threshold and create pred_same_author column
#     df_copy$same_author <- ifelse(df_copy$same_author == TRUE, 1, 0)
#     
#     # Compare final_score with threshold and create pred_same_author column
#     df_copy$pred_same_author <- ifelse(df_copy$score > threshold, 1,
#                                        ifelse(df_copy$score == threshold, 0.5, 0))
#     
#     df_copy$result = ifelse(df_copy$same_author == df_copy$pred_same_author, 1, 0)
#     
#     # Append the dataframe to the final_df
#     final_df <- rbind(final_df, df_copy)
#   }
#   
#   return(final_df)
# }
# 
# test_thresholds <- compare_thresholds(aggregated_results(enron_profile), seq(0, 1, 0.01))
# 
# results_metrics <- test_thresholds |> 
#   group_by(threshold) |>
#   summarise(accuracy = Metrics::accuracy(same_author, pred_same_author),
#             precision = Metrics::precision(same_author, pred_same_author),
#             recall = Metrics::recall(same_author, pred_same_author),
#             f1 = Metrics::f1(same_author, pred_same_author),
#             auc = Metrics::auc(same_author, pred_same_author)) 
# 
# library(ggplot2)
# 
# ggplot(results_metrics, aes(x = threshold, y = accuracy)) +
#   geom_line() +
#   geom_hline(yintercept = 0.9) +
#   geom_hline(yintercept = 0.8) +
#   xlim(0, 1) +
#   ylim(0, 1)
# 
# ggplot(results_metrics, aes(x = recall, y = precision)) +
#   geom_line() +
#   geom_hline(yintercept = 0.9) +
#   geom_hline(yintercept = 0.8) +
#   xlim(0, 1) +
#   ylim(0, 1)

summary(enron_aggregated$score)
summary(enron_profile$score)
summary(wiki_aggregated$score)
summary(wiki_profile$score)
