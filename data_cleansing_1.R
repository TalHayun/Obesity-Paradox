install.packages("sparklyr")
knitr::opts_chunk$set(echo = TRUE)

install.packages("ggplot2")
install.packages("viridis")
install.packages("ggridges")
library(tidyverse)
library(ggplot2)
library(viridis)
library(ggridges)
library(tidymodels)

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

#create data-frame tables
person_df <- read_parquet_table("person")
measurement <- read_parquet_table("measurement_specific")
visit_df <- read_parquet_table("visit_occurrence")
condition_occurrence_df <- read_parquet_table("condition_occurrence")
death_df <- read_parquet_table("death")
observation_df <- read_parquet_table("observation")
measurement_bmi_df <- read_parquet_table("measurement_BMI")
concept_df <- data.table::fread("../data/OMOP-Concepts-5bc1e3a3/CONCEPT/CONCEPT.csv")

# save the full tables as RDS files
saveRDS(person_df, file = "person_df.RDS")
saveRDS(visit_df, file = "visit_df.RDS")
saveRDS(condition_occurrence_df, file = "condition_occurrence_df.RDS")
saveRDS(death_df, file = "death_df.RDS")
saveRDS(observation_df, file = "observation_df.RDS")
saveRDS(concept_df, file = "concept_df.RDS")
saveRDS(measurement_bmi_df, file = "measurement_bmi_df.RDS")
measurement_specific_df <- readRDS("measurement_specific_df.RDS")

visit_df <- readRDS("visit_df.RDS")
concept_df <- readRDS("concept_df.RDS")

# select relavent attributes in person table
person <- person_df %>%
  select(person_id, year_of_birth, gender)

# filter visit table between 2000 to 2015 and select relavent attributes
visit <- visit_df  %>%
  select(person_id, visit_occurrence_id, visit_start_date, visit_end_date, discharge_to) 

# select relevant attributes in condition occurrence table
condition_occurrence <- condition_occurrence_df  %>%
  select(visit_occurrence_id, condition_name)

# select relavent attributes in death table
death <-death_df  %>%
  select(person_id, death_date)

# select the concept id and concept class of Clinical Finding
concept_clinical <- concept_df  %>%
  select(concept_id, concept_class_id) %>%
  filter(concept_class_id == 'Clinical Finding')

# select person_id, concept_id, background_diseases from observation_df (rename the columns names)
observation <- observation_df  %>%
  select(person_id, observation_concept_id, observation) 
  colnames(observation)[2] <- "concept_id"
  colnames(observation)[3] <- "background_diseases"

# join concept and observation tables
observation_concept <- merge.data.frame(concept_clinical, observation, "concept_id")

# select the clinical background diseases
observation <- observation_concept %>% 
  select(person_id, background_diseases) %>%
  distinct 

# select the following background diseases: Allergy to penicillin, Diabetes mellitus, Hyperlipidemia,
# Heart disease and Chronic ischemic heart disease
observation <- observation %>% filter(background_diseases == 'Allergy to penicillin' |
                                        background_diseases == 'Diabetes mellitus' |
                                        background_diseases == 'Hyperlipidemia' |
                                        background_diseases == 'Heart disease' |
                                        background_diseases == 'Chronic ischemic heart disease')

measurement_albumin <- measurement_specific_df %>%
  filter(measurement_concept_id == 3028286) 
measurement_albumin <- measurement_albumin[with(measurement_albumin, order(person_id, as.Date(measurement_date, "%d-%m-%Y"))),]
measurement_albumin <- measurement_albumin %>% distinct(person_id,.keep_all = TRUE)


measurement <- measurement_bmi_df  %>%
  select(person_id, cohort_definition_id, measurement_date, value_as_number) %>%
  filter(value_as_number >= 15,
         value_as_number <= 45) 

# change cohort definition id to the relevant bmi group
measurement <- measurement %>%
  mutate(cohort_definition_id = replace(cohort_definition_id, cohort_definition_id == 213,"15-18.5"),
         cohort_definition_id = replace(cohort_definition_id, cohort_definition_id == 214,"18.5-25"),
         cohort_definition_id = replace(cohort_definition_id, cohort_definition_id == 215,"25-30"),
         cohort_definition_id = replace(cohort_definition_id, cohort_definition_id == 216,"30-35"),
         cohort_definition_id = replace(cohort_definition_id, cohort_definition_id == 217,"35-40"),
         cohort_definition_id = replace(cohort_definition_id, cohort_definition_id == 235,"40-45"))

# change cohort_definition_id name to bmi_group
colnames(measurement)[2] <- "bmi_group"


# sort by person_id and then by measurement_date
measurement <- measurement[with(measurement, order(person_id, as.Date(measurement_date, "%d-%m-%Y"))),]

# drop duplications
measurement <- measurement %>%
  distinct(person_id,.keep_all = TRUE)
# delete measurement date
measurement <- measurement[-c(3)]

# save the edited tables as RDS files
saveRDS(person, file = "person.RDS")
saveRDS(visit, file = "visit.RDS")
saveRDS(condition_occurrence, file = "condition_occurrence.RDS")
saveRDS(death, file = "death.RDS")
saveRDS(observation, file = "observation.RDS")
saveRDS(measurement, file = "measurement.RDS")

# read edited tables
person <- readRDS("person.RDS")
visit <- readRDS("visit.RDS")
condition_occurrence <- readRDS("condition_occurrence.RDS")
death <- readRDS("death.RDS")
observation_df <- readRDS("observation_df.RDS")
measurement <-  readRDS("measurement.RDS")

# join measurement and visit tables by person id
measurement_visit <- merge(measurement,visit, by="person_id")

# # sort by person_id and then by measurement_date
measurement_visit <- measurement_visit[with(measurement_visit, order(person_id, as.Date(visit_start_date, "%d-%m-%Y"))),]

# # drop duplications
measurement_visit <- measurement_visit %>%
  distinct(person_id,.keep_all = TRUE)

# left join person, visit and condition occurrence tables by visit occurrence id
measurement_visit_condition <- merge(measurement_visit,condition_occurrence,by="visit_occurrence_id",all.x=TRUE)

# left join person, visit, condition occurrence and death tables by visit occurrence id
death <- death %>% 
  select(person_id, death_date)
measurement_visit_condition_death <- merge(measurement_visit_condition,death,by="person_id",all.x=TRUE)

# sort by person_id and then by visit_start_date
measurement_visit_condition_death <- measurement_visit_condition_death[with(measurement_visit_condition_death, order(person_id, as.Date(visit_start_date, "%d-%m-%Y"))),]

# drop duplications
measurement_visit_condition_death <- measurement_visit_condition_death %>%
  distinct(person_id,.keep_all = TRUE)

person <- person %>% 
  select(person_id, year_of_birth, gender)

# lower case of gender
person <- person %>%
  mutate(gender = replace(gender, gender == "MALE","Male"),
         gender = replace(gender, gender == "FEMALE","Female"))

measurement_visit_condition_death_person <- merge(measurement_visit_condition_death, person,by="person_id",all.x=TRUE)

# calculate ages at hospitalization, 1 year and 5 years after hospitalization
measurement_visit_condition_death_person <- measurement_visit_condition_death_person %>% mutate(age_during_hospitalization = as.integer(substr(measurement_visit_condition_death_person$visit_start_date, 0, 4))- year_of_birth)

# sort by person_id and then by measurement_date
measurement_visit_condition_death_person <- measurement_visit_condition_death_person[with(measurement_visit_condition_death_person, order(person_id, as.Date(visit_start_date, "%d-%m-%Y"))),]

# drop duplications
measurement_visit_condition_death_person <- measurement_visit_condition_death_person %>%
  distinct(person_id,.keep_all = TRUE)

measurement_albumin_cut <- measurement_albumin[c(5,7)] %>% rename(albumin = value_as_number)
obesity_albumin <- merge(measurement_visit_condition_death_person, measurement_albumin_cut, by="person_id",all.x=TRUE)

# save the obesity_albumin table as RDS files
saveRDS(obesity_albumin, file = "obesity_albumin.RDS")

# save the final table as RDS files
saveRDS(measurement_visit_condition_death_person, file = "obesity_measurement.RDS")

# read the final tables
obesity_measurement <- readRDS("obesity_measurement.RDS")
observation <- readRDS("observation.RDS")

# function that calculates the time from hospitalization to death, only for 0,1,5 years after hospitalization
time_from_hospitalization_to_death <- function(tablename) {
  tablename <- tablename %>% 
    mutate(years_from_hospitalization_to_death = ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date)) &  as.Date(tablename$visit_end_date) + 180 > as.Date(tablename$death_date), 0,
                                                        ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date)  + 180 & as.Date(tablename$visit_end_date) + 540 > as.Date(tablename$death_date)), 1,
                                                               ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date) + 1800  & as.Date(tablename$visit_end_date) + 2160 > as.Date(tablename$death_date)), 5, NA))))
}

# function that calculates the time from hospitalization to death, only for 0,1,5 years after hospitalization (as string)
time_string_from_hospitalization_to_death <- function(tablename) {
  tablename <- tablename %>% 
    mutate(years_from_hospitalization_to_death = ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date)) &  as.Date(tablename$visit_end_date) + 180 > as.Date(tablename$death_date), "0",
                                                        ifelse((as.Date(tablename$death_date)>= as.Date(tablename$visit_end_date)  + 180 & as.Date(tablename$visit_end_date) + 540 > as.Date(tablename$death_date)), "1",
                                                               ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date) + 1800  & as.Date(tablename$visit_end_date) + 2160 > as.Date(tablename$death_date)), "5", NA))))
}

# Apply the time_from_hospitalization_to_death fucntion on the obesity data frame
obesity_time_untill_death <- time_from_hospitalization_to_death(measurement_visit_condition_death_person)
 
# select only the impatients who died after 0,1,5 years, group by bmi_group and years_from_hospitalization_to_death and count them
plot1 <- time_string_from_hospitalization_to_death(measurement_visit_condition_death_person) %>%
  filter(years_from_hospitalization_to_death >=0) %>%
  group_by(bmi_group,years_from_hospitalization_to_death) %>%
  summarise(n = n())

ggplot(plot1, aes(x = bmi_group, y=n,
                  fill = years_from_hospitalization_to_death)) +
  geom_bar(position="dodge", stat="identity",width = 0.75)+
  scale_fill_viridis(discrete = T) +
  theme_bw() +
  labs(title = "Mortality of hospitalized patients", 
       x = "BMI Group",
       y = 'Mortality',
       subtitle = "Divided by BMI groups and the time elapsed from hospitalization",
       fill = 'Years after hospitalization',
       caption = "Interpretation: Due to small amount of information about the 15-18.5 BMI group,\nwe will examine only the two other groups") +
  theme(plot.title = element_text(face="bold"), legend.position="bottom", legend.direction="horizontal", plot.caption = element_text(size = 11, hjust = 0)) 

plot2 <- obesity_time_untill_death %>%
  count(bmi_group, years_from_hospitalization_to_death) %>%
  group_by(bmi_group)  %>%
  mutate(death_prop = n / sum(n))

plot2 <- plot2 %>%
  filter(years_from_hospitalization_to_death == 0|
           years_from_hospitalization_to_death == 1|
           years_from_hospitalization_to_death == 5,
         bmi_group != "15-18.5") %>%
  arrange(years_from_hospitalization_to_death) 

ggplot(plot2, aes(fill=bmi_group, x=death_prop, y=bmi_group)) + 
  geom_bar(position="dodge", stat="identity")+
  scale_fill_discrete(limits = c("25-30", "18.5-25"))+
  facet_grid(years_from_hospitalization_to_death~.,labeller = as_labeller(c(`0` = "short-\nterm", `1` = "1 year",`5` = "5 years" ))) +
  theme_bw() +
  labs(title = "Mortality proportion", 
       x = "Mortality proportion(%)",
       y = 'BMI Group',
       subtitle = "Divided by BMI group and the time elapsed from hospitalization",
       fill = 'BMI Group',
       caption = "Interpretation: When we examine the mortality proportion from the BMI group,\nthere is still no noticeable difference") +
  theme(plot.title = element_text(face="bold"), panel.spacing = unit(1.5, "lines"),strip.text.y = element_text(angle = 0), plot.caption = element_text(size = 11, hjust = 0)) 

plot3 <- obesity_time_untill_death %>%
  count(bmi_group, gender, years_from_hospitalization_to_death) %>%
  group_by(bmi_group, gender)  %>%
  mutate(prop_death = n / sum(n))
plot3 <- plot3 %>%
  filter(years_from_hospitalization_to_death == 0|
           years_from_hospitalization_to_death == 1|
           years_from_hospitalization_to_death == 5,
         bmi_group != "15-18.5") %>% 
  group_by(gender, years_from_hospitalization_to_death) %>%
  mutate(ratio_of_death_prop = n / sum(n)) %>%
  arrange(years_from_hospitalization_to_death,gender) 

ggplot(plot3, aes(fill=bmi_group, x=ratio_of_death_prop, y=bmi_group)) + 
  geom_bar(position="dodge", stat="identity")+
  scale_fill_discrete(limits = c("25-30", "18.5-25"))+
  facet_grid(years_from_hospitalization_to_death~factor(gender,levels=c('Male','Female')),
             labeller = labeller(years_from_hospitalization_to_death = as_labeller(c(`0` = "Short-\nterm", `1` = "1 year",`5` = "5 years" )))) +
  theme_bw() + 
  labs(title = "Mortality proportion ratio", 
       x =  "Mortality proportion ratio",
       y = 'BMI Group',
       subtitle = "Divided by BMI group, time elapsed from hospitalization and gender",
       fill = 'BMI Group',
       caption = "Interpretation: There is a difference in the mortality ratio between\nthe males BMI groups.\nThe mortality rate of the 18.5-25 BMI group is bigger than the 25-30 BMI group\nand it is manifested in all three time constants.")  +
  theme(plot.title = element_text(face="bold"), strip.text.y = element_text(angle = 0),panel.spacing = unit(1.5, "lines"),  plot.caption = element_text(size = 12, hjust = 0)) 

plot4 <- obesity_time_untill_death %>% 
  mutate(age_group = ifelse(0 <= age_during_hospitalization & age_during_hospitalization <=60 , "0-60",
                            ifelse(61 <= age_during_hospitalization , "61 <", NA)))
plot4 <- plot4 %>%
  count(bmi_group, gender, age_group, years_from_hospitalization_to_death) %>%
  group_by(bmi_group, gender, age_group)  %>%
  mutate(prop_death = n / sum(n))
plot4 <- plot4 %>%
  filter(years_from_hospitalization_to_death >= 0,
         bmi_group != "15-18.5",
         gender == "Male") %>% 
  arrange(years_from_hospitalization_to_death) 
plot4 <- plot4 %>%
  group_by(age_group,years_from_hospitalization_to_death) %>%
  mutate(prop_death2 = n / sum(n))%>%
  arrange(years_from_hospitalization_to_death,age_group) 

ggplot(plot4, aes(fill=bmi_group, x=prop_death2, y=bmi_group)) + 
  geom_bar(position="dodge", stat="identity")+
  scale_fill_discrete(limits = c("25-30", "18.5-25"))+
  facet_grid(years_from_hospitalization_to_death~age_group, ,
             labeller = labeller(years_from_hospitalization_to_death = as_labeller(c(`0` = "Short-\nterm", `1` = "1 year",`5` = "5 years" )),age_group = as_labeller(c(`0-60` = "Ages\n0-60",`61 <` = "Ages\n60+")))) +
  theme_bw() +
  labs(title = "Males mortality proportion ratio", 
       x = "Mortality proportion ratio",
       y = 'BMI Group',
       subtitle = "Divided by BMI group, time elapsed from hospitalization and age group",
       fill = 'BMI Group',
       caption = "Interpretation: For males under the age of 60, in the BMI group of 18.5-25,\nthe mortality rate is 3-6 times higher than for the BMI group of 25-30,\nin the immediate time frame and after one year.\nIn addition, no clear difference can be seen after 5 years,\nsimilar to the research hypothesis.") +
  theme(plot.title = element_text(face="bold"), panel.spacing = unit(1.5, "lines"), strip.text.y = element_text(angle = 0), plot.caption = element_text(size = 12, hjust = 0))

# function that calculates time (in days) from of hospitalization
Duration_of_hospitalization <-function(tablename) {
  tablename <- tablename %>% 
    mutate(dur_of_hospitalization = as.numeric(difftime(as.Date(tablename$visit_end_date), as.Date(tablename$visit_start_date), units = "days")))
}

plot6 <- Duration_of_hospitalization(obesity)
plot6 <- plot6 %>% 
  filter(dur_of_hospitalization > 1,
         dur_of_hospitalization < 15)
ggplot(plot6, aes(x = dur_of_hospitalization, y = bmi_group, fill = bmi_group, color = bmi_group)) +
  geom_density_ridges(alpha = 0.4) +
  scale_fill_viridis(discrete=TRUE, limits = c("25-30", "18.5-25", "15-18.5"))+
  scale_color_viridis(discrete=TRUE, limits = c("25-30", "18.5-25", "15-18.5")) +
  theme_ridges() +
  labs(title = "Hospitalization duration distribution", 
       x = "Duration of hospitalization (days)",
       y = "BMI group",
       subtitle = "Divided by BMI group",
       fill = 'BMI group',
       color = 'BMI group',
       caption = "Interpretation: The BMI group of 15-18.5 has a high rate\nof inpatients for 6-7 and 12-14 days, compared to the larger BMI groups\nin which there was a significant decrease in the rate of inpatients.")   +
  theme(plot.title = element_text(face="bold"), axis.title.x = element_text(hjust=0.5),axis.title.y = element_text(hjust=0.5), plot.caption = element_text(hjust=0))

obesity_measurement <- readRDS("obesity_measurement.RDS")

obesity_measurement <- obesity_measurement %>% mutate_at(vars(binary_death), factor)

# function that calculates the time from hospitalization to death, only for 0,1,5 years after hospitalization
time_from_hospitalization_to_death <- function(tablename) {
  tablename <- tablename %>% 
    mutate(years_from_hospitalization_to_death = ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date)) &  as.Date(tablename$visit_end_date) + 180 > as.Date(tablename$death_date), 0,
                                                        ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date)  + 180 & as.Date(tablename$visit_end_date) + 540 > as.Date(tablename$death_date)), 1,
                                                               ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date) + 1800  & as.Date(tablename$visit_end_date) + 2160 > as.Date(tablename$death_date)), 5, NA))))
}

# function that calculates the time from hospitalization to death, only for 0,1,5 years after hospitalization (as string)
time_string_from_hospitalization_to_death <- function(tablename) {
  tablename <- tablename %>% 
    mutate(years_from_hospitalization_to_death = ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date)) &  as.Date(tablename$visit_end_date) + 180 > as.Date(tablename$death_date), "0",
                                                        ifelse((as.Date(tablename$death_date)>= as.Date(tablename$visit_end_date)  + 180 & as.Date(tablename$visit_end_date) + 540 > as.Date(tablename$death_date)), "1",
                                                               ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date) + 1800  & as.Date(tablename$visit_end_date) + 2160 > as.Date(tablename$death_date)), "5", NA))))
}

# function that returns the logistic regression model for a table

get_logistic_bmi_fit <- function(tablename, col){
  tablename <- tablename %>% mutate_at(vars(tablename$col), factor)
  
  death_mod <- logistic_reg() %>% 
    set_engine("glm") 
  death_fit  <- death_mod %>%
    fit(col ~ value_as_number, data = tablename, family = "binomial")
  tidy(death_fit)
}

# find who died in an immediate period

obesity_measurement_imm <- obesity_measurement %>%
  mutate(binary_death_imm = ifelse((as.Date(death_date) >= as.Date(visit_end_date)) &  (as.Date(visit_end_date) + 180 > as.Date(death_date)), 1,0))
obesity_measurement_imm <- obesity_measurement_imm %>% 
  mutate(binary_death_imm = ifelse(is.na(death_date), 0, binary_death_imm))


# find who survived the immediate period
obesity_measurement_1_year <- obesity_measurement_imm %>%
  filter(binary_death_imm == 0)


# find who died after 1 year
obesity_measurement_1_year_bin <- obesity_measurement_1_year %>%
  mutate(binary_death_one_year = ifelse((as.Date(death_date)>= as.Date(visit_end_date)  + 180 & as.Date(visit_end_date) + 540 > as.Date(death_date)), 1,0))
obesity_measurement_1_year_bin <- obesity_measurement_1_year_bin %>% 
  mutate(binary_death_one_year = ifelse(is.na(binary_death_one_year), 0, binary_death_one_year))


# find who survived after 1 year
obesity_measurement_5_year <- obesity_measurement_1_year_bin %>%
  filter(binary_death_one_year == 0)


# find who died after 5 years
obesity_measurement_5_year_bin <- obesity_measurement_5_year %>%
  mutate(binary_death_five_year = ifelse((as.Date(death_date) >= as.Date(visit_end_date) + 1800  & as.Date(visit_end_date) + 2160 > as.Date(death_date)), 1,0))
obesity_measurement_5_year_bin <- obesity_measurement_5_year_bin %>% 
  mutate(binary_death_five_year = ifelse(is.na(binary_death_five_year), 0, binary_death_five_year))

# logistic regression model for immediate period
obesity_measurement_imm <- obesity_measurement_imm %>% mutate_at(vars(binary_death_imm), factor)
death_mod <- logistic_reg() %>% 
  set_engine("glm") 
death_fit  <- death_mod %>%
  fit(binary_death_imm ~ value_as_number, data = obesity_measurement_imm, family = "binomial")
tidy( death_fit)

obesity_measurement_imm <- obesity_measurement_imm %>% mutate_at(vars(binary_death_imm), factor)
get_logistic_bmi_fit(obesity_measurement_imm, binary_death_imm)




























