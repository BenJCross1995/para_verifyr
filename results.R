library(dplyr)

results <- read.csv("./test/impostor_method_combined.csv")
results

weighted_results <- results |>
  mutate(sample_paraphrases = sum(num_paraphrases), .by="sample_id", .after="num_paraphrases") |>
  mutate(paraphrase_prop = num_paraphrases / sample_paraphrases, .after="sample_paraphrases") |>
  mutate(weighted_score = paraphrase_prop * sentence_score, .after="sentence_score") |>
  group_by(sample_id) |>
  summarise(sample_score = sum(weighted_score)) |>
  mutate(same_author = ifelse(sample_score > 0.5, 1, ifelse(sample_score == 0.5, 0.5, 0)))

same_author_sum_results <- results |>
  group_by(sample_id) |>
  summarise(total_chunks = n(),
            same_author_sum = sum(same_author)) |>
  mutate(sample_score = same_author_sum / total_chunks) |>
  mutate(same_author = ifelse(sample_score > 0.5, 1, ifelse(sample_score == 0.5, 0.5, 0))) |>
  select(-total_chunks, -same_author_sum)
