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

install.packages("gapminder")
library(gapminder)

Sys.getenv("SPARK_HOME")

# Read parequet table to data.frame
read_parquet_table <- function(tablename) {
fname <- list.files(pattern = paste0(tablename, ".parquet$"), recursive = TRUE, path = '~/')
p_tbl <- spark_read_parquet(sc, tablename, paste0('~/', fname))
df <- collect(p_tbl)
}

#create person table
person_df <- read_parquet_table("person")

#create visit table
visit_df <- read_parquet_table("visit_occurrence")

#create condition occourence table
condition_occurrence_df <- read_parquet_table("condition_occurrence")

#create death table
death_df <- read_parquet_table("death")

#create observation table
observation_df <- read_parquet_table("observation")

#create concept_df table
concept_df <- data.table::fread("../data/OMOP-Concepts-5bc1e3a3/CONCEPT/CONCEPT.csv")

# save the full tables as RDS files
saveRDS(person_df, file = "person_df.RDS")
saveRDS(visit_df, file = "visit_df.RDS")
saveRDS(condition_occurrence_df, file = "condition_occurrence_df.RDS")
saveRDS(death_df, file = "death_df.RDS")
saveRDS(observation_df, file = "observation_df.RDS")
saveRDS(concept_df, file = "concept_df.RDS")

# read the full tables
person_df <- readRDS("person_df.RDS")
visit_df <- readRDS("visit_df.RDS")
condition_occurrence_df <- readRDS("condition_occurrence_df.RDS")
death_df <- readRDS("death_df.RDS")
observation_df <- readRDS("observation_df.RDS")
concept_df <- readRDS("concept_df.RDS")

# select relavent attributes in person table
person <- person_df %>%
select(person_id,cohort_definition_id, year_of_birth, gender)

# select relavent attributes in person table
visit <- visit_df %>%
select(person_id, visit_occurrence_id, visit_start_date, visit_end_date, discharge_to) 

# select relevant attributes in condition occurrence table
condition_occurrence <- condition_occurrence_df %>%
select(visit_occurrence_id, condition_name)

# select relavent attributes in death table
death <-death_df %>%
select(person_id, death_date)

# select the concept id and concept class of Clinical Finding
concept_clinical <- concept_df %>%
select(concept_id, concept_class_id) %>%
filter(concept_class_id == 'Clinical Finding')

# select person_id, concept_id, background_diseases from observation_df (rename the columns names)
observation <- observation_df %>%
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

# save the edited tables as RDS files
saveRDS(person, file = "person.RDS")
saveRDS(visit, file = "visit.RDS")
saveRDS(condition_occurrence, file = "condition_occurrence.RDS")
saveRDS(death, file = "death.RDS")
saveRDS(observation, file = "observation.RDS")

# read edited tables
person <- readRDS("person.RDS")
visit <- readRDS("visit.RDS")
condition_occurrence <- readRDS("condition_occurrence.RDS")
death <- readRDS("death.RDS")
observation_df <- readRDS("observation_df.RDS")

# Create the full table

# join person and visit tables by person id
person_visit <- merge(person,visit, by="person_id")

# calculate ages at hospitalization, 1 year and 5 years after hospitalization
person_visit <- person_visit %>% mutate(age_during_hospitalization = as.integer(substr(person_visit$visit_start_d
ate, 0, 4))- year_of_birth)

# left join person, visit and condition occurrence tables by visit occurrence id
person_visit_condition <- merge(person_visit,condition_occurrence,by="visit_occurrence_id",all.x=TRUE)

# left join person, visit, condition occurrence and death tables by visit occurrence id
death <- death %>%
select(person_id, death_date)
person_visit_condition_death <- merge(person_visit_condition,death,by="person_id",all.x=TRUE)

# sort by person_id and then by visit_start_date
person_visit_condition_death <- person_visit_condition_death[with(person_visit_condition_death, order(person_id,
as.Date(visit_start_date, "%d-%m-%Y"))),]

# drop duplications
person_visit_condition_death <- person_visit_condition_death %>%
distinct(person_id,.keep_all = TRUE)
person_visit_condition_death

# change cohort definition id to the relevant bmi group
person_visit_condition_death <- person_visit_condition_death %>%
mutate(cohort_definition_id = replace(cohort_definition_id, cohort_definition_id == 213,"15-18.5"),
cohort_definition_id = replace(cohort_definition_id, cohort_definition_id == 214,"18.5-25"),
cohort_definition_id = replace(cohort_definition_id, cohort_definition_id == 215,"25-30"))

# change cohort_definition_id name to bmi_group
colnames(person_visit_condition_death)[3] <- "bmi_group"

# drop visit_occurrence_id column
person_visit_condition_death <- person_visit_condition_death[-c(2)]

# lower case of gender
person_visit_condition_death <- person_visit_condition_death %>%
mutate(gender = replace(gender, gender == "MALE","Male"),
gender = replace(gender, gender == "FEMALE","Female"))
person_visit_condition_death

# save the final table as RDS files
saveRDS(person_visit_condition_death, file = "obesity.RDS")

# read the final tables
obesity <- readRDS("obesity.RDS")
observation <- readRDS("observation.RDS")

#create measurement table
measurement <- read_parquet_table("measurement_specific")

#create measurement_bmi table
measurement_bmi_df <- read_parquet_table("measurement_BMI")

# save the measurement_specific table as RDS files
saveRDS(measurement, file = "measurement_specific_df.RDS")

# save themeasurement_bmi_df table as RDS files
saveRDS(measurement_bmi_df, file = "measurement_bmi_df.RDS")

# get the rows with albumin values
measurement_albumin <- measurement_specific_df %>%
filter(measurement_concept_id == 3028286) 
measurement_albumin <- measurement_albumin[with(measurement_albumin, order(person_id, as.Date(measurement_date, "%d-%m-%Y"))),]
measurement_albumin <- measurement_albumin %>% distinct(person_id,.keep_all = TRUE)

# get the relavent bmi values
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

# lower case of gender
measurement <- measurement %>%
      mutate(gender = replace(gender, gender == "MALE","Male"),
            gender = replace(gender, gender == "FEMALE","Female"))

# sort by person_id and then by measurement_date
measurement <- measurement[with(measurement, order(person_id, as.Date(measurement_date, "%d-%m-%Y"))),]

# create binary_death column
measurement <- measurement %>%
    mutate(binary_death = ifelse(death_date != is.null(death_date), 1,0))
measurement <- measurement %>% 
    mutate(binary_death = ifelse(is.na(binary_death), 0, binary_death))

# drop duplications
measurement <- measurement %>%
       distinct(person_id,.keep_all = TRUE)

# delete measurement date
measurement <- measurement[-c(3)]

saveRDS(measurement, file = "measurement.RDS")

# get the full table with bmi values

# join measurement and visit tables by person id
measurement_visit <- merge(measurement,visit, by="person_id")
 
# sort by person_id and then by measurement_date
measurement_visit <- measurement_visit[with(measurement_visit, order(person_id, as.Date(visit_start_date, "%d-%m-%Y"))),]

# drop duplications
measurement_visit <- measurement_visit %>%
       distinct(person_id,.keep_all = TRUE)


# left join measurement, visit and condition occurrence tables by visit occurrence id
measurement_visit_condition <- merge(measurement_visit,condition_occurrence,by="visit_occurrence_id",all.x=TRUE)


# left join measurement, visit, condition occurrence and death tables by visit occurrence id
death <- death %>% 
    select(person_id, death_date)
measurement_visit_condition_death <- merge(measurement_visit_condition,death,by="person_id",all.x=TRUE)

# sort by person_id and then by visit_start_date
measurement_visit_condition_death <- measurement_visit_condition_death[with(measurement_visit_condition_death, order(person_id, as.Date(visit_start_date, "%d-%m-%Y"))),]


person <- person %>% 
    select(person_id, year_of_birth, gender)

# lower case of gender
person <- person %>%
      mutate(gender = replace(gender, gender == "MALE","Male"),
            gender = replace(gender, gender == "FEMALE","Female"))

# left join measurement, visit, condition occurrence, death and person tables by person_id
measurement_visit_condition_death_person <- merge(measurement_visit_condition_death, person,by="person_id",all.x=TRUE)

# calculate ages at hospitalization, 1 year and 5 years after hospitalization
measurement_visit_condition_death_person <- measurement_visit_condition_death_person %>% mutate(age_during_hospitalization = as.integer(substr(measurement_visit_condition_death_person$visit_start_date, 0, 4))- year_of_birth)

# sort by person_id and then by measurement_date
measurement_visit_condition_death_person <- measurement_visit_condition_death_person[with(measurement_visit_condition_death_person, order(person_id, as.Date(visit_start_date, "%d-%m-%Y"))),]

# drop duplications
measurement_visit_condition_death_person <- measurement_visit_condition_death_person %>%
       distinct(person_id,.keep_all = TRUE)
       
# get relavent attributes from albumin table
measurement_albumin_cut <- measurement_albumin[c(5,7)] %>% rename(albumin = value_as_number)
obesity_albumin <- merge(measurement_visit_condition_death_person, measurement_albumin_cut, by="person_id",all.x=TRUE)

# save the final table as RDS files
saveRDS(measurement_visit_condition_death_person, file = "obesity_measurement.RDS")

# save the obesity_albumin table as RDS files
saveRDS(obesity_albumin, file = "obesity_albumin.RDS")

obesity_measurement <- readRDS("obesity_measurement.RDS")

# function that calculates the time from hospitalization to death, only for 0,1,5 years after hospitalization
time_from_hospitalization_to_death <- function(tablename) {
tablename <- tablename %>%
    mutate(years_from_hospitalization_to_death = ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date)) & as.Date(tablename$visit_end_date) + 180 > as.Date(tablename$death_date), 0,
    ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date) + 180 & as.Date(tablename$visit_end_date) + 540 > as.Date(tablename$death_date)), 1,
    ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date) + 1800 & as.Date(tablename$visit_end_date) + 2160 > as.Date(tablename$death_date)), 5, NA))))
}

# function that calculates the time from hospitalization to death, only for 0,1,5 years after hospitalization (as string)
time_string_from_hospitalization_to_death <- function(tablename) {
    tablename <- tablename %>% 
        mutate(years_from_hospitalization_to_death = ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date)) &  as.Date(tablename$visit_end_date) + 180 > as.Date(tablename$death_date), "0",
                    ifelse((as.Date(tablename$death_date)>= as.Date(tablename$visit_end_date)  + 180 & as.Date(tablename$visit_end_date) + 540 > as.Date(tablename$death_date)), "1",
                    ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date) + 1800  & as.Date(tablename$visit_end_date) + 2160 > as.Date(tablename$death_date)), "5", NA))))
}

# change the binary_death to factor (for logistic model)
obesity_measurement <- obesity_measurement %>% mutate_at(vars(binary_death), factor)

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


# find who died after 5 years
obesity_measurement_5_year_bin <- time_from_hospitalization_to_death(obesity_measurement)
obesity_measurement_5_year_bin <- obesity_measurement_5_year_bin %>% filter((as.Date(death_date) >= as.Date(visit_end_date)  + 4*365) | binary_death ==  0)
obesity_measurement_5_year_bin <- obesity_measurement_5_year_bin %>% mutate(death_after_x_years = ifelse((as.Date(death_date) <= as.Date(visit_end_date)  + 5*365), 1, 0))
obesity_measurement_5_year_bin <- obesity_measurement_5_year_bin %>% 
    mutate(death_after_x_years = ifelse(is.na(death_date), 0, death_after_x_years))
    
# find independent variables for immediate period
imm_rec <- obesity_measurement_imm %>% mutate_at(vars(binary_death_imm, gender), factor)
imm_fit2 <- logistic_reg() %>%
 set_engine("glm") %>%
 fit(binary_death_imm ~ value_as_number + age_during_hospitalization + gender, data = imm_rec, family = "binomial")
tidy(imm_fit2)

# diffrence between genders and age (0-60, and 60+)

# logistic regression model for males, 0-60 in an immediate period
obesity_measurement_imm <- obesity_measurement_imm %>% mutate_at(vars(binary_death_imm), factor)
obesity_measurement_imm_male_under60 <- obesity_measurement_imm %>% filter(gender == "Male", age_during_hospitalization < 60)
death_mod <- logistic_reg() %>% 
    set_engine("glm") 
death_fit1  <- death_mod %>%
    fit(binary_death_imm ~ value_as_number, data = obesity_measurement_imm_male_under60, family = "binomial")
tidy(death_fit1)

# logistic regression model for males, 60+ in an immediate period
obesity_measurement_imm <- obesity_measurement_imm %>% mutate_at(vars(binary_death_imm), factor)
obesity_measurement_imm_male_above60 <- obesity_measurement_imm %>% filter(gender == "Male", age_during_hospitalization >= 60)
death_mod <- logistic_reg() %>% 
    set_engine("glm") 
death_fit2  <- death_mod %>%
    fit(binary_death_imm ~ value_as_number, data = obesity_measurement_imm_male_above60, family = "binomial")
tidy(death_fit2)

# logistic regression model for females, 0-60 in an immediate period
obesity_measurement_imm <- obesity_measurement_imm %>% mutate_at(vars(binary_death_imm), factor)
obesity_measurement_imm_female_under60 <- obesity_measurement_imm %>% filter(gender == "Female", age_during_hospitalization < 60)
death_mod <- logistic_reg() %>% 
    set_engine("glm") 
death_fit3  <- death_mod %>%
    fit(binary_death_imm ~ value_as_number, data = obesity_measurement_imm_female_under60, family = "binomial")
tidy(death_fit3)

# logistic regression model for females, 60+ in an immediate period
obesity_measurement_imm <- obesity_measurement_imm %>% mutate_at(vars(binary_death_imm), factor)
obesity_measurement_imm_female_above60 <- obesity_measurement_imm %>% filter(gender == "Female", age_during_hospitalization >= 60)
death_mod <- logistic_reg() %>% 
    set_engine("glm") 
death_fit4  <- death_mod %>%
    fit(binary_death_imm ~ value_as_number, data = obesity_measurement_imm_female_above60, family = "binomial")
tidy(death_fit4)

# create a graph for immediate period

new_bmis1 <- seq(15,45)

# males under 60 model coefs
a1 = tidy(death_fit1)$estimate[1]
b1 = tidy(death_fit1)$estimate[2]

# males above 60 model coefs
a2 = tidy(death_fit2)$estimate[1]
b2 = tidy(death_fit2)$estimate[2]

# # females under 60 model coefs
# a3 = tidy(death_fit3)$estimate[1]
# b3 = tidy(death_fit3)$estimate[2]

# females above 60 model coefs
a4 = tidy(death_fit4)$estimate[1]
b4 = tidy(death_fit4)$estimate[2]

males_under_60 <- a1 + 
  b1*new_bmis1 

males_above_60 <- a2 + 
  b2*new_bmis1 

# females_under_60 <- a3 + 
#   b3*new_bmis1 

females_above_60 <- a4 + 
  b4*new_bmis1 

a_probs <- exp(males_under_60)/(1 + exp(males_under_60))
b_probs <- exp(males_above_60)/(1 + exp(males_above_60))
# c_probs <- exp(females_under_60)/(1 + exp(females_under_60))
d_probs <- exp(females_above_60)/(1 + exp(females_above_60))

# mortality probability by gender and age
plot.data <- data.frame("Males, 0-60"=a_probs, "Males, 60+"=b_probs,"Females, 60+" =d_probs, X1=new_bmis1)
plot.data <- rename(plot.data, "Males, 0-60" = Males..0.60,  "Males, 60+" = Males..60.,"Females, 60+" = Females..60. )
plot.data <- gather(plot.data, key=Group, value= prob, "Males, 0-60", "Males, 60+", "Females, 60+")

cols <- c("Males, 60+" = "indianred1",  "Males, 0-60" = "green3", "Females, 60+" = "deepskyblue2")

ggplot(plot.data, aes(x=X1, y=prob, color=Group)) + # asking it to set the color by the variable "group" is what makes it draw three different lines
  geom_line(lwd=2) +  
  scale_color_manual(values = cols, breaks = c( "Males, 60+", "Females, 60+", "Males, 0-60"),) +
      labs(x = expression("BMI (kg/m"^"2"*")"), y="Mortality rate (%)", title="How BMI affects hospitalized mortality rate in the immediate period",
       subtitle = "Divided by gender and age groups") +
        theme(plot.title = element_text(face="bold",size = 14), axis.title.x = element_text(hjust=0.5,size = 14),axis.title.y = element_text(hjust=0.5,size = 14),
              plot.subtitle = element_text(, size = 13), legend.title = element_text(face="bold", size=13), legend.text = element_text(size=13))
              
# find another independent variables for 1 year
one_year_rec <- obesity_measurement_1_year_bin %>% mutate_at(vars(binary_death_one_year, gender), factor)
one_year_rec_fit <- logistic_reg() %>%
 set_engine("glm") %>%
 fit(binary_death_one_year ~ value_as_number + age_during_hospitalization + gender, data = one_year_rec, family = "binomial")
tidy(one_year_rec_fit)

# diffrence between genders and age (0-60, and 60+) after one year

# logistic regression model for males, 0-60 after one year
obesity_measurement_1_year_bin <- obesity_measurement_1_year_bin %>% mutate_at(vars(binary_death_one_year), factor)
obesity_measurement_year_male_under60 <- obesity_measurement_1_year_bin %>% filter(gender == "Male", age_during_hospitalization < 60)
death_mod <- logistic_reg() %>% 
    set_engine("glm") 
death_fit5  <- death_mod %>%
    fit(binary_death_one_year ~ value_as_number, data = obesity_measurement_year_male_under60, family = "binomial")
tidy(death_fit5)

# diffrence between genders and age (0-60, and 60+) after one year

# logistic regression model for males, 60+ after one year
obesity_measurement_1_year_bin <- obesity_measurement_1_year_bin %>% mutate_at(vars(binary_death_one_year), factor)
obesity_measurement_year_male_above60 <- obesity_measurement_1_year_bin %>% filter(gender == "Male", age_during_hospitalization >= 60)
death_mod <- logistic_reg() %>% 
    set_engine("glm") 
death_fit6  <- death_mod %>%
    fit(binary_death_one_year ~ value_as_number, data = obesity_measurement_year_male_above60, family = "binomial")
tidy(death_fit6)

# diffrence between genders and age (0-60, and 60+) after one year

# logistic regression model for females, 0-60 after one year
obesity_measurement_1_year_bin <- obesity_measurement_1_year_bin %>% mutate_at(vars(binary_death_one_year), factor)
obesity_measurement_year_female_under60 <- obesity_measurement_1_year_bin %>% filter(gender == "Female", age_during_hospitalization < 60)
death_mod <- logistic_reg() %>% 
    set_engine("glm") 
death_fit7  <- death_mod %>%
    fit(binary_death_one_year ~ value_as_number, data = obesity_measurement_year_female_under60, family = "binomial")
tidy(death_fit7)

# diffrence between genders and age (0-60, and 60+) after one year

# logistic regression model for females, 60+ after one year
obesity_measurement_1_year_bin <- obesity_measurement_1_year_bin %>% mutate_at(vars(binary_death_one_year), factor)
obesity_measurement_year_female_above60 <- obesity_measurement_1_year_bin %>% filter(gender == "Female", age_during_hospitalization >= 60)
death_mod <- logistic_reg() %>% 
    set_engine("glm") 
death_fit8  <- death_mod %>%
    fit(binary_death_one_year ~ value_as_number, data = obesity_measurement_year_female_above60, family = "binomial")
tidy(death_fit8)

new_bmis1 <- seq(15,45)

# males above 60 model coefs
a1 = tidy(death_fit6)$estimate[1]
b1 = tidy(death_fit6)$estimate[2]

# females above 60 model coefs
a2 = tidy(death_fit8)$estimate[1]
b2 = tidy(death_fit8)$estimate[2]

males_above_60 <- a1 + 
  b1*new_bmis1 

females_above_60 <- a2 + 
  b2*new_bmis1 

a_probs <- exp(males_above_60)/(1 + exp(males_above_60))
b_probs <- exp(females_above_60)/(1 + exp(females_above_60))

# mortality probability by gender and age after one year
plot.data <- data.frame("Males, 60+"=a_probs,"Females, 60+" =b_probs, X1=new_bmis1)
plot.data <- rename(plot.data,"Males, 60+" = Males..60.,"Females, 60+" = Females..60. )
plot.data <- gather(plot.data, key=Group, value= prob,"Males, 60+", "Females, 60+")

cols <- c("Males, 60+" = "indianred1", "Females, 60+" = "deepskyblue2")

ggplot(plot.data, aes(x=X1, y=prob, color=Group)) + # asking it to set the color by the variable "group" is what makes it draw three different lines
  geom_line(lwd=2) + 
    scale_color_manual(values = cols, breaks = c( "Males, 60+", "Females, 60+")) +
  labs(x=expression("BMI (kg/m"^"2"*")"), y="Mortality rate (%)", title="How BMI affects hospitalized mortality rate after 1 year",
       subtitle = "Divided by gender and age groups") +
    theme(plot.title = element_text(face="bold",size = 14), axis.title.x = element_text(hjust=0.5,size = 14),axis.title.y = element_text(hjust=0.5,size = 14),
              plot.subtitle = element_text(, size = 13), legend.title = element_text(face="bold",size=13), legend.text = element_text(size=13)) 

# find another independent variables for 5 year
obesity_measurement_5_year_bin <- obesity_measurement_5_year_bin %>% mutate_at(vars(death_after_x_years), factor)
five_year_rec_fit <- logistic_reg() %>%
 set_engine("glm") %>%
 fit(death_after_x_years ~ value_as_number + age_during_hospitalization + gender, data = obesity_measurement_5_year_bin, family = "binomial")
tidy(five_year_rec_fit)

# how bmi size affects the probability to be discharged to home visit

# males under 60 in an immediate time
obesity_measurement_dicharge <- obesity_measurement %>%
    mutate(discharge_bin = ifelse(discharge_to == "Home Visit", 1,0))
x <- obesity_measurement_dicharge %>% mutate_at(vars(discharge_bin, gender), factor)

dicharge_mod <- logistic_reg() %>% 
    set_engine("glm") 
dicharge_fit12  <- dicharge_mod %>%
    fit(discharge_bin ~ value_as_number + gender + age_during_hospitalization, data = x, family = "binomial")
tidy(dicharge_fit12)

# how bmi size affects the probability to be discharged to home visit

# males under 60 in an immediate time
obesity_measurement_dicharge <- obesity_measurement %>%
    mutate(discharge_bin = ifelse(discharge_to == "Home Visit", 1,0))
obesity_measurement_dicharge <- obesity_measurement_dicharge %>% mutate_at(vars(discharge_bin, gender), factor)
obesity_measurement_dicharge_male_under60 <- obesity_measurement_dicharge %>% filter(gender == "Male", age_during_hospitalization < 60)
dicharge_mod <- logistic_reg() %>% 
    set_engine("glm") 
dicharge_fit  <- dicharge_mod %>%
    fit(discharge_bin ~ value_as_number, data = obesity_measurement_dicharge_male_under60, family = "binomial")
tidy(dicharge_fit)

# males above 60 in an immediate time
obesity_measurement_dicharge <- obesity_measurement %>%
    mutate(discharge_bin = ifelse(discharge_to == "Home Visit", 1,0))
obesity_measurement_dicharge <- obesity_measurement_dicharge %>% mutate_at(vars(discharge_bin, gender), factor)
obesity_measurement_dicharge_male_above60 <- obesity_measurement_dicharge %>% filter(gender == "Male", age_during_hospitalization >= 60)
dicharge_mod <- logistic_reg() %>% 
    set_engine("glm") 
dicharge_fit  <- dicharge_mod %>%
    fit(discharge_bin ~ value_as_number, data = obesity_measurement_dicharge_male_above60, family = "binomial")
tidy(dicharge_fit)

# females under 60 in an immediate time
obesity_measurement_dicharge <- obesity_measurement %>%
    mutate(discharge_bin = ifelse(discharge_to == "Home Visit", 1,0))
obesity_measurement_dicharge <- obesity_measurement_dicharge %>% mutate_at(vars(discharge_bin, gender), factor)
obesity_measurement_dicharge_female_under60 <- obesity_measurement_dicharge %>% filter(gender == "Female", age_during_hospitalization < 60)
dicharge_mod <- logistic_reg() %>% 
    set_engine("glm") 
dicharge_fit  <- dicharge_mod %>%
    fit(discharge_bin ~ value_as_number, data = obesity_measurement_dicharge_female_under60, family = "binomial")
tidy(dicharge_fit)

# females above 60 in an immediate time
obesity_measurement_dicharge <- obesity_measurement %>%
    mutate(discharge_bin = ifelse(discharge_to == "Home Visit", 1,0))
obesity_measurement_dicharge <- obesity_measurement_dicharge %>% mutate_at(vars(discharge_bin, gender), factor)
obesity_measurement_dicharge_female_above60 <- obesity_measurement_dicharge %>% filter(gender == "Female", age_during_hospitalization >= 60)
dicharge_mod <- logistic_reg() %>% 
    set_engine("glm") 
dicharge_fit  <- dicharge_mod %>%
    fit(discharge_bin ~ value_as_number, data = obesity_measurement_dicharge_female_above60, family = "binomial")
tidy(dicharge_fit)
# p.value too high

# find who died in an immediate period 
obesity_albumin_1 <- obesity_albumin %>%
    mutate(binary_death_imm = ifelse((as.Date(death_date) >= as.Date(visit_end_date)) &  (as.Date(visit_end_date) + 180 > as.Date(death_date)), 1,0))
obesity_albumin_1 <- obesity_albumin_1 %>% 
    mutate(binary_death_imm = ifelse(is.na(death_date), 0, binary_death_imm))
obesity_albumin_1 <- obesity_albumin_1 %>% mutate_at(vars(binary_death_imm, gender), factor)

# find another independent variables for immediate period (including albumin)
fit_albumin1 <- logistic_reg() %>%
 set_engine("glm") %>%
 fit(binary_death_imm ~ value_as_number + age_during_hospitalization + gender + albumin, data = obesity_albumin_1, family = "binomial")
tidy(fit_albumin1)
