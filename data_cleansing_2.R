install.packages("sparklyr")
knitr::opts_chunk$set(echo = TRUE)
library(tidyverse)

Sys.getenv("SPARK_HOME")

# install the SparkR package
#devtools::install_github('apache/spark', ref='master', subdir='R/pkg')

# load the SparkR package
library('sparklyr')
sc <- spark_connect(master = "local")

# Read parequet table to data.frame
read_parquet_table <- function(tablename) {
  fname <- list.files(pattern = paste0(tablename, ".parquet$"), recursive = TRUE, path = '~/')
  p_tbl <- spark_read_parquet(sc, tablename, paste0('~/', fname))
  df <- collect(p_tbl)
}

concept_tbl <- data.table::fread("../data/OMOP-Concepts-5bc1e3a3/CONCEPT/CONCEPT.csv")
saveRDS(concept_tbl, file = "concept_df.RDS")

#create person table
person_tbl <- read_parquet_table("person")
saveRDS(person_tbl, file = "person_df.RDS")

#create visit table
visit_tbl <- read_parquet_table("visit_occurrence")
saveRDS(visit_tbl, file = "visit_df.RDS")

#create condition occourence table
condition_occurrence_tbl <- read_parquet_table("condition_occurrence")
saveRDS(condition_occurrence_tbl, file = "condition_occurrence_df.RDS")

#create death table
death_tbl <- read_parquet_table("death")
saveRDS(death_tbl, file = "death_df.RDS")

#create observation table
observation_tbl <- read_parquet_table("observation")
saveRDS(observation_tbl, file = "observation_df.RDS")

# read the full tables
person_df <- readRDS("person_df.RDS")
visit_df <- readRDS("visit_df.RDS")
condition_occurrence_df <- readRDS("condition_occurrence_df.RDS")
death_df <- readRDS("death_df.RDS")
observation_df <- readRDS("observation_df.RDS")
concept_df <- readRDS("concept_df.RDS")

person <- readRDS("person.RDS")
visit <- readRDS("visit.RDS")
condition_occurrence <- readRDS("condition_occurrence.RDS")
death <- readRDS("death.RDS")
observation_df <- readRDS("observation_df.RDS")

# select relevant attributes in condition occurrence table and drop duplicates
condition_occurrence <- condition_occurrence_df  %>%
  select(person_id, condition_name, status)

# select relavent attributes in death table
death <-death_df  %>%
  select(person_id, death_date)

#load data-frames by RDS file
person_df <- readRDS("person_df.RDS")
visit_df <- readRDS("visit_df.RDS")
observation_df <- readRDS("observation_df.RDS")
condition_occurrence_df <- readRDS("condition_occurrence_df.RDS")
concept_df <- readRDS("concept_df.RDS")

# Assign the concept id and concept class of Clinical Finding
concept_clinical <- concept_df  %>%
  select(concept_id, concept_class_id) %>%
  filter(concept_class_id == 'Clinical Finding')

#Assign person_id, concept_id, background_diseases from observation_df (rename the columns names)
observation_1 <- observation_df  %>%
  select(person_id, observation_concept_id, observation) 
colnames(observation_1)[2] <- "concept_id"
colnames(observation_1)[3] <- "background_diseases"

# create new dataset that include person_id and background_diseases (by inner concept_clinial and observation_1)
observation_concept <- merge.data.frame(concept_clinical,observation_1, "concept_id")
background_by_personID <- observation_concept %>% 
  select(person_id, background_diseases) %>%
  distinct

# Assign the background diseases in Clinical Finding category
clinical_background <- background_by_personID %>% group_by(background_diseases) %>% summarize(counted=n())
clinical_background <- clinical_background[order(-clinical_background$counted),]

person_visit <- merge(person,visit, by="person_id")
person_visit_condition <- merge(person_visit,condition_occurrence,by="visit_occurrence_id",all.x=TRUE)
person_visit <- person_visit %>% mutate(age_during_hospitalization = as.integer(substr(person_visit$visit_start_date, 0, 4))- year_of_birth)

#choose specific backcground diseases 
background_by_personID <- background_by_personID %>% filter(background_diseases == 'Allergy to penicillin' |
                                                              background_diseases == 'Diabetes mellitus' |
                                                              background_diseases == 'Hyperlipidemia' |
                                                              background_diseases == 'Heart disease' |
                                                              background_diseases == 'Chronic ischemic heart disease')

# group by the background diseases
background_by_personID <- aggregate(.~ person_id, background_by_personID, list) 

# save the final table as RDS files
obesity_df <- readRDS("obesity.RDS")
obesity_df %>% mutate(age_during_hospitalization = as.integer(substr(obesity_df$visit_start_date, 0, 4))- year_of_birth,
                      age_after_1_years_hospitalization = as.integer(substr(obesity_df$visit_start_date, 0, 4))- year_of_birth + 1,
                      age_after_5_years_hospitalization = as.integer(substr(obesity_df$visit_start_date, 0, 4))- year_of_birth + 5)

obesity_df <- readRDS("obesity.RDS")

# select relevant attributes in condition occurrence table
condition_occurrence <- condition_occurrence_df  %>%
  select(visit_occurrence_id, condition_name, status)

id_group_death <- merge(x = cohort_person_id, y = death, by = "person_id", all = TRUE)
id_group_death <- id_group_death %>%
  mutate(cohort_definition_id = replace(cohort_definition_id, cohort_definition_id == 213,"15-18.5"),
         cohort_definition_id = replace(cohort_definition_id, cohort_definition_id == 214,"18.5-25"),
         cohort_definition_id = replace(cohort_definition_id, cohort_definition_id == 215,"25-30"))

death <- death %>% 
  select(person_id, death_date)
person_visit_condition_death <- merge(person_visit_condition,death,by="person_id",all.x=TRUE)
person_visit_condition_death %>% 
  filter(((as.Date(death_date) >= as.Date(visit_end_date)) & ( as.Date(visit_end_date) + 180) >as.Date(death_date))|
           (as.Date(death_date) + 180 >= as.Date(visit_end_date) & as.Date(visit_end_date) + 540 > as.Date(death_date))|
           (as.Date(death_date) + 1800 >= as.Date(visit_end_date) & as.Date(visit_end_date) + 2160 > as.Date(death_date))) %>%
  distinct(person_id,.keep_all = TRUE)














