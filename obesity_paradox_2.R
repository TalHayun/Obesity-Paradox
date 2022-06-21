install.packages("sparklyr")
install.packages("dplyr") 
install.packages("dunn.test")
install.packages("psych")
knitr::opts_chunk$set(echo = TRUE)
library(tidyverse)
library(psych)
library(dunn.test)
library(ggplot2)
library(ggridges
        
Sys.getenv("SPARK_HOME")

# install the SparkR package
#devtools::install_github('apache/spark', ref='master', subdir='R/pkg')

# load the SparkR package
library('sparklyr')
sc <- spark_connect(master = "local")

person <- readRDS("person.RDS")
visit <- readRDS("visit.RDS")
condition_occurrence <- readRDS("condition_occurrence.RDS")
death <- readRDS("death.RDS")
observation_df <- readRDS("observation_df.RDS")
measurement_specific <- readRDS("measurement_specific_df.RDS")
obesity_measurement <- readRDS("obesity_measurement.RDS")

# mutate a binary death column (0 - live, 1 - died), 25077 records
obesity_measurement1 <-obesity_measurement %>% mutate_at(vars(binary_death), factor) 

# function that calculates the time from hospitalization to death, only for 0,1,5 years after hospitalization
time_from_hospitalization_to_death <- function(tablename) {
  tablename <- tablename %>% 
    mutate(years_from_hospitalization_to_death = ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date)) &  as.Date(tablename$visit_end_date) + 180 > as.Date(tablename$death_date), 0,
                                                        ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date)  + 180 & as.Date(tablename$visit_end_date) + 540 > as.Date(tablename$death_date)), 1,
                                                               ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date) + 1800  & as.Date(tablename$visit_end_date) + 2160 > as.Date(tablename$death_date)), 5, NA))))
}

# selected the relevant column for clean table Kruskal-Waills test 
obesity_time_untill_death <- time_from_hospitalization_to_death(obesity_measurement1) %>% select(bmi_group, gender, age_during_hospitalization, binary_death, years_from_hospitalization_to_death)

# mutate death_immidiate binary column (0 - Null or above 0 years in hospitalizon, 1 - immidate death), repalcing the null values to 'Null'
obesity_time_untill_death$years_from_hospitalization_to_death[is.na(obesity_time_untill_death$years_from_hospitalization_to_death)] <- 'Null'
obesity_time_untill_death_1 <- obesity_time_untill_death %>%
  mutate(death_immidiate = ifelse(years_from_hospitalization_to_death == 'Null' | years_from_hospitalization_to_death > 0, 0, 1))


# Use Kruskal Wallis test
# We can see the the p-calue is small (3.121e-05) therefore at least one group is differnce from the others
result_1 = kruskal.test(death_immidiate ~ bmi_group,
                        data = obesity_time_untill_death_1)


# define 6 different death_immidaite values by bmi_group (6 groups)
imm_result_15_18.5_bmi_1 <- obesity_time_untill_death_1 %>% 
  filter(bmi_group == '15-18.5') %>% 
  select(death_immidiate)

imm_result_18.5_25_bmi_1 <- obesity_time_untill_death_1 %>% 
  filter(bmi_group == '18.5-25') %>% 
  select(death_immidiate)

imm_result_25_30_bmi_1 <- obesity_time_untill_death_1 %>% 
  filter(bmi_group == '25-30') %>% 
  select(death_immidiate)

imm_result_30_35_bmi_1 <- obesity_time_untill_death_1 %>% 
  filter(bmi_group == '30-35') %>% 
  select(death_immidiate)

imm_result_35_40_bmi_1 <- obesity_time_untill_death_1 %>% 
  filter(bmi_group == '35-40') %>% 
  select(death_immidiate)

imm_result_40_45_bmi_1 <- obesity_time_untill_death_1 %>% 
  filter(bmi_group == '40-45') %>% 
  select(death_immidiate)

#each group valus by bmi_group
a_1 <- imm_result_15_18.5_bmi_1$death_immidiate #916
b_1 <- imm_result_18.5_25_bmi_1$death_immidiate #8798
c_1 <- imm_result_25_30_bmi_1$death_immidiate #9359
d_1 <- imm_result_30_35_bmi_1$death_immidiate #3751
e_1 <- imm_result_35_40_bmi_1$death_immidiate #1586
f_1 <- imm_result_40_45_bmi_1$death_immidiate #667

#perform Dunn's Test with Benjamini-Hochberg correction for p-values - immediate
dunn.test(x=list(a_1, b_1, c_1, d_1, e_1, f_1), method="bh")

# immidiate death prop by bmi_group 
obesity_time_untill_death_1 %>% 
  count(bmi_group, death_immidiate)%>%
  group_by(bmi_group)%>%
  mutate(death_prop = n / sum(n))%>%
  filter(death_immidiate == 1) %>%
  select(bmi_group,death_prop) 

# Drop immidiate died patients  
obesity_time_untill_death_2 <- obesity_time_untill_death %>%
  filter(years_from_hospitalization_to_death >=1 | years_from_hospitalization_to_death == 'Null')

# mutate death_1_years (0 - Null or above one years in hospitalizon, 1 - immidate death)
obesity_time_untill_death_2 <- obesity_time_untill_death_2 %>%
  
  
  mutate(death_after_one_year = ifelse(years_from_hospitalization_to_death == 'Null' | years_from_hospitalization_to_death > 1, 0, 1))


# define 6 different death_after_one_year values by bmi_group (6 groups)
imm_result_15_18.5_bmi_2 <- obesity_time_untill_death_2 %>% 
  filter(bmi_group == '15-18.5') %>% 
  select(death_after_one_year)

imm_result_18.5_25_bmi_2 <- obesity_time_untill_death_2 %>% 
  filter(bmi_group == '18.5-25') %>% 
  select(death_after_one_year)

imm_result_25_30_bmi_2 <- obesity_time_untill_death_2 %>% 
  filter(bmi_group == '25-30') %>% 
  select(death_after_one_year)

imm_result_30_35_bmi_2 <- obesity_time_untill_death_2 %>% 
  filter(bmi_group == '30-35') %>% 
  select(death_after_one_year)

imm_result_35_40_bmi_2 <- obesity_time_untill_death_2 %>% 
  filter(bmi_group == '35-40') %>% 
  select(death_after_one_year)

imm_result_40_45_bmi_2 <- obesity_time_untill_death_2 %>% 
  filter(bmi_group == '40-45') %>% 
  select(death_after_one_year)

a_2 <- imm_result_15_18.5_bmi_2$death_after_one_year
b_2 <- imm_result_18.5_25_bmi_2$death_after_one_year
c_2 <- imm_result_25_30_bmi_2$death_after_one_year
d_2 <- imm_result_30_35_bmi_2$death_after_one_year
e_2 <- imm_result_35_40_bmi_2$death_after_one_year 
f_2 <- imm_result_40_45_bmi_2$death_after_one_year 

# use Kruskal Wallis test - we can see the the p-calue is small (2.759e-05) therefore a differnce between the groups
result_2 = kruskal.test(death_after_one_year ~ bmi_group,
                        data = obesity_time_untill_death_2)
result_2

#perform Dunn's Test with Benjamini-Hochberg correction for p-values - 1 year after hospitalization
dunn.test(x=list(a, b, c, d, e, f), method="bh")

# one year after hospitalization - death prop by bmi_group 
obesity_time_untill_death_2 %>% 
  count(bmi_group, death_after_one_year)%>%
  group_by(bmi_group)%>%
  mutate(death_prop = n / sum(n))%>%
  filter(death_after_one_year == 1) %>% 
  select(bmi_group,death_prop) 

# Drop immidiate or after one hospitalization died patients 
obesity_time_untill_death_3 <- obesity_time_untill_death %>%
  filter(years_from_hospitalization_to_death >=5 | years_from_hospitalization_to_death == 'Null')
# mutate death_5_years (0 - Null or above one years in hospitalizon, 1 - immidate death)
obesity_time_untill_death_3 <- obesity_time_untill_death_3 %>%
  mutate(death_after_five_year = ifelse(years_from_hospitalization_to_death == 'Null' | years_from_hospitalization_to_death > 5, 0, 1))


# define 6 different death_after_five_year values by bmi_group (6 groups)
imm_result_15_18.5_bmi_3 <- obesity_time_untill_death_3 %>% 
  filter(bmi_group == '15-18.5') %>% 
  select(death_after_five_year)

imm_result_18.5_25_bmi_3 <- obesity_time_untill_death_3 %>% 
  filter(bmi_group == '18.5-25') %>% 
  select(death_after_five_year)

imm_result_25_30_bmi_3 <- obesity_time_untill_death_3 %>% 
  filter(bmi_group == '25-30') %>% 
  select(death_after_five_year)

imm_result_30_35_bmi_3 <- obesity_time_untill_death_3 %>% 
  filter(bmi_group == '30-35') %>% 
  select(death_after_five_year)

imm_result_35_40_bmi_3 <- obesity_time_untill_death_3 %>% 
  filter(bmi_group == '35-40') %>% 
  select(death_after_five_year)

imm_result_40_45_bmi_3 <- obesity_time_untill_death_3 %>% 
  filter(bmi_group == '40-45') %>% 
  select(death_after_five_year)

a_3 <- imm_result_15_18.5_bmi_3$death_after_five_year
b_3 <- imm_result_18.5_25_bmi_3$death_after_five_year
c_3 <- imm_result_25_30_bmi_3$death_after_five_year
d_3 <- imm_result_30_35_bmi_3$death_after_five_year
e_3 <- imm_result_35_40_bmi_3$death_after_five_year 
f_3 <- imm_result_40_45_bmi_3$death_after_five_year 

# use Kruskal Wallis test - we can see the the p-calue is big 0.517 therefore a not differnce between the groups
result_3 = kruskal.test(death_after_five_year ~ bmi_group,
                        data = obesity_time_untill_death_3)
result_3

#perform Dunn's Test with Benjamini-Hochberg correction for p-values - 5 year after hospitalization
dunn.test(x=list(a_3, b_3, c_3, d_3, e_3, f_3), method="bh")

# five year after hospitalization - death prop by bmi_group 
obesity_time_untill_death_3 %>% 
  count(bmi_group, death_after_five_year)%>%
  group_by(bmi_group)%>%
  mutate(death_prop = n / sum(n))%>%
  filter(death_after_five_year == 1) %>%
  select(bmi_group,death_prop) 

# function that calculates time (in days) from of hospitalization
Duration_of_hospitalization <-function(tablename) {
  tablename <- tablename %>% 
    mutate(days_of_hospitalization = as.numeric(difftime(as.Date(tablename$visit_end_date), as.Date(tablename$visit_start_date), units = "days")))
}
# Select BMI_GROUP and mutate each record days duration in hospitalization - 25077 records
groups_days_hos<- Duration_of_hospitalization(obesity_measurement %>% select(bmi_group, visit_start_date, visit_end_date))
# statistic information on days_of_hospitalization
groups_days_hos <- groups_days_hos %>% filter (days_of_hospitalization >= 0)
groups_days_hos %>% filter(2 < days_of_hospitalization, days_of_hospitalization < 20) %>% count() # 6146 our of 25077

#Display hospitalization duration distribution that divided by BMI group between 1-4 days (include)
patients_one_until_twetnty_days <- groups_days_hos %>% 
  filter(days_of_hospitalization > 0,
         days_of_hospitalization < 5)

ggplot(patients_one_until_twetnty_days, aes(x = days_of_hospitalization, y = bmi_group, fill = bmi_group, color = bmi_group)) +
  geom_density_ridges(alpha = 0.4) +
  theme_ridges() +
  labs(title = "Hospitalization duration distribution", 
       x = "Duration of hospitalization (days)",
       y = "BMI group",
       subtitle = "Divided by BMI group",
       fill = 'BMI group',
       color = 'BMI group') +
  theme(plot.title = element_text(face="bold"), axis.title.x = element_text(hjust=0.5),axis.title.y = element_text(hjust=0.5), plot.caption = element_text(hjust=0))

# Divide to 2 BMI_groups: 15- 25, 25-45 
two_groups <- groups_days_hos %>% mutate(group = ifelse(bmi_group < 25, '0', '1')) %>% select(bmi_group, days_of_hospitalization, group)
#Display hospitalization duration distribution that divided by BMI group between 2-7 days (include)
plot_2 <- two_groups %>% 
  filter(days_of_hospitalization > 1,
         days_of_hospitalization < 8)

ggplot(plot_2, aes(x = days_of_hospitalization, y = group,  fill = group, color = group)) +
  geom_density_ridges(alpha = 0.4) + 
  theme_ridges() +
  labs(title = "Hospitalization duration distribution", 
       x = "Duration of hospitalization (days)",
       y = "group",
       subtitle = "Divided by BMI group",
       fill = 'group',
       color = 'group') +
  theme(plot.title = element_text(face="bold"), axis.title.x = element_text(hjust=0.5),axis.title.y = element_text(hjust=0.5), plot.caption = element_text(hjust=0))

# comparing 2 bmi groups between 1-8 days in hospitalization (zoom in last plot)
plot_3 <- two_groups %>% 
  filter(days_of_hospitalization >= 1,
         days_of_hospitalization < 8)

ggplot(plot_3, aes(x = days_of_hospitalization, 
                   fill = group)) +
  geom_density(alpha = 0.5) +
  theme_bw() +
  labs(title = "Hospitalization duration distribution", 
       y = "Density",
       subtitle = "Divided by 2 BMI group",
       fill = "Group")+
  scale_fill_manual("BMI Group", labels = c("15-25","25-45"), 
                    values=c("pink1","skyblue1"))+
  theme(plot.title = element_text(face="bold"), axis.title.x = element_text(hjust=0.5),axis.title.y = element_text(hjust=0.5), plot.caption = element_text(hjust=0))

# filter days of hospitalization between 1 to 7 (9828 out of 10943)
plot_4 <- groups_days_hos %>% 
  filter(days_of_hospitalization >= 1, 
         days_of_hospitalization <= 15)

# Comparing 6 bmi_groups (separately)
ggplot(plot_4, aes(x = days_of_hospitalization, y = bmi_group,  fill = bmi_group, color = bmi_group))+ 
  geom_density_ridges(alpha = 0.4, adjust = 2) +
  labs(title = "Hospitalization duration distribution", 
       x = "Duration of hospitalization (days)",
       y = "bmi_group",
       fill = 'BMI group',
       color = 'BMI group',
       subtitle = "Divided by BMI group")+
  theme(plot.title = element_text(face="bold"), axis.title.x = element_text(hjust=0.5),axis.title.y = element_text(hjust=0.5), plot.caption = element_text(hjust=0))

# filter days of hospitalization between 7 to 22  (include) - 9828 out of 10943
plot_4.5<- groups_days_hos %>% 
  filter(days_of_hospitalization >=7, 
         days_of_hospitalization <= 22)

plot_3 <- ggplot(plot_4.5, aes(x = days_of_hospitalization, y = bmi_group, fill = bmi_group))+ 
  geom_density_ridges(alpha = 0.4) +
  theme_ridges() +
  xlim(c(7,21))+
  scale_x_continuous(breaks = seq(7, 22, by = 3))+
  labs(title = "Hospitalization duration distribution", 
       x = "Duration of hospitalization (days)",
       y = " BMI Group",
       subtitle = "Divided by BMI group") +
  scale_fill_manual(breaks = c("40-45", "35-40", "30-35","25-30","18.5-25","15-18.5"), values=c("pink2","skyblue2","green3", "orange2", "yellow2","purple2")) +
  theme(plot.title = element_text(face="bold"), axis.title.x = element_text(hjust=0.5),axis.title.y = element_text(hjust=0.5), plot.caption = element_text(hjust=0)) 
plot_3

# comparing 2 bmi groups between 7-22 days in hospitalization (zoom in last plot)
plot_5 <- groups_days_hos %>% 
  filter(days_of_hospitalization >=7, 
         days_of_hospitalization <= 22)
# plot_5 <- plot_5 %>% mutate(new_groups =  ifelse(bmi_group == "15-18.5", "15-18.5",
#                                        ifelse(bmi_group == "18.5-25", "18.5-25", "25-45"))) 
ggplot(plot_5, aes(x = days_of_hospitalization, 
                   fill = bmi_group)) +
  scale_x_continuous(breaks = seq(7, 22, by = 3))+
  geom_density(alpha = 0.4,adjust = 1) +
  theme_bw() +
  labs(title = "Hospitalization duration distribution", 
       y = "Density",
       subtitle = "Divided by 6 BMI group",
       fill = "Group")+
  theme(plot.title = element_text(face="bold"), axis.title.x = element_text(hjust=0.5),axis.title.y = element_text(hjust=0.5), plot.caption = element_text(hjust=0))

