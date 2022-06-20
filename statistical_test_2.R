install.packages("sparklyr")
knitr::opts_chunk$set(echo = TRUE)
install.packages("ggplot2")
install.packages("viridis")
install.packages("ggridges")
install.packages("dplyr") 
library(ggplot2)
library(viridis)
library(ggridges)
library(tidyverse)
library('sparklyr')
remove.packages("rlang")

Sys.getenv("SPARK_HOME")

# install the SparkR package
#devtools::install_github('apache/spark', ref='master', subdir='R/pkg')

# load the SparkR package
sc <- spark_connect(master = "local")

# Read parequet table to data.frame
read_parquet_table <- function(tablename) {
  fname <- list.files(pattern = paste0(tablename, ".parquet$"), recursive = TRUE, path = '~/')
  p_tbl <- spark_read_parquet(sc, tablename, paste0('~/', fname))
  df <- collect(p_tbl)
}

# read the measurement_specific table
measurement <- read_parquet_table("measurement_specific")
saveRDS(measurement, file = "measurement_specific_df.RDS")

# Read obesity measurment RDS table
obesity_measurement <- readRDS("obesity_measurement.RDS")
obesity_measurement

# function that calculates the time from hospitalization to death, only for 0,1,5 years after hospitalization
time_from_hospitalization_to_death <- function(tablename) {
  tablename <- tablename %>% 
    mutate(years_from_hospitalization_to_death = ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date)) &  as.Date(tablename$visit_end_date) + 180 > as.Date(tablename$death_date), 0,
                                                        ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date)  + 180 & as.Date(tablename$visit_end_date) + 540 > as.Date(tablename$death_date)), 1,
                                                               ifelse((as.Date(tablename$death_date) >= as.Date(tablename$visit_end_date) + 1800  & as.Date(tablename$visit_end_date) + 2160 > as.Date(tablename$death_date)), 5, NA))))
}

# selected the relevant column for Kruskal-Waills test 
obesity_time_untill_death <- time_from_hospitalization_to_death(obesity_measurement1) %>% select(bmi_group, gender, age_during_hospitalization, binary_death, years_from_hospitalization_to_death)

# mutate death_immidiate binary column (0 - Null or above 0 years in hospitalizon, 1 - immidate death)
# ( repalcing the null values to 'Null' and )
obesity_time_untill_death$years_from_hospitalization_to_death[is.na(obesity_time_untill_death$years_from_hospitalization_to_death)] <- 'Null'
obesity_time_untill_death_1 <- obesity_time_untill_death %>%
  mutate(death_immidiate = ifelse(years_from_hospitalization_to_death == 'Null' | years_from_hospitalization_to_death > 0, 0, 1))

# Install and import package for dunn-tust
install.packages("dunn.test")
library(dunn.test)

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
a <- imm_result_15_18.5_bmi_1$death_immidiate #916
b <- imm_result_18.5_25_bmi_1$death_immidiate #8798
c <- imm_result_25_30_bmi_1$death_immidiate #9359
d <- imm_result_30_35_bmi_1$death_immidiate #3751
e <- imm_result_35_40_bmi_1$death_immidiate #1586
f <- imm_result_40_45_bmi_1$death_immidiate #667

# perform Dunn's Test with Bonferroni correction for p-values - immediate
dunn.test(x=list(a, b, c, d, e, f), method="bonferroni")

#perform Dunn's Test with Benjamini-Hochberg correction for p-values -  immediate
dunn.test(x=list(a, b, c, d, e, f), method="bh")

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
obesity_time_untill_death_2 


# define 6 different death_immidaite values by bmi_group (6 groups)
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

a <- imm_result_15_18.5_bmi_2$death_after_one_year
b <- imm_result_18.5_25_bmi_2$death_after_one_year
c <- imm_result_25_30_bmi_2$death_after_one_year
d <- imm_result_30_35_bmi_2$death_after_one_year
e <- imm_result_35_40_bmi_2$death_after_one_year 
f <- imm_result_40_45_bmi_2$death_after_one_year 

# use Kruskal Wallis test - we can see the the p-calue is small (2.759e-05) therefore a differnce between the groups
result_2 = kruskal.test(death_after_one_year ~ bmi_group,
                        data = obesity_time_untill_death_2)

# perform Dunn's Test with Bonferroni correction for p-values -  1 year after hospitalization
dunn.test(x=list(a, b, c, d, e, f), method="bonferroni")

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

# define 6 different death_immidaite values by bmi_group (6 groups)
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

a <- imm_result_15_18.5_bmi_3$death_after_five_year
b <- imm_result_18.5_25_bmi_3$death_after_five_year
c <- imm_result_25_30_bmi_3$death_after_five_year
d <- imm_result_30_35_bmi_3$death_after_five_year
e <- imm_result_35_40_bmi_3$death_after_five_year 
f <- imm_result_40_45_bmi_3$death_after_five_year 

# use Kruskal Wallis test - we can see the the p-calue is small (2.759e-05) therefore a differnce between the groups
result_3 = kruskal.test(death_after_five_year ~ bmi_group,
                        data = obesity_time_untill_death_3)

# perform Dunn's Test with Bonferroni correction for p-values - 5 year after hospitalization
dunn.test(x=list(a, b, c, d, e, f), method="bonferroni")

# perform Dunn's Test with Benjamini-Hochberg correction for p-values - 5 year after hospitalization
dunn.test(x=list(a, b, c, d, e, f), method="bh")

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

# Select BMI_GROUP and mutate each record days duration in hospitalization 
groups_days_hos<- Duration_of_hospitalization(obesity_measurement %>% select(bmi_group, visit_start_date, visit_end_date))

install.packages("psych")
library(psych)

# statistic information on days_of_hospitalization
groups_days_hos <- groups_days_hos %>% filter (days_of_hospitalization >= 0)

patients_one_until_twetnty_days <- groups_days_hos %>% 
  filter(days_of_hospitalization > 0,
         days_of_hospitalization < 5)
patients_one_until_twetnty_days
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

# Divide to 2 BMI_groups: 15- 25, 25-45 (10943 records)
two_groups <- groups_days_hos %>% mutate(group = ifelse(bmi_group < 25, '0', '1')) %>% select(bmi_group, days_of_hospitalization, group)

# plot that display the hospitalization duration distribution (divided by 2 bmi groups)
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

# comparing 2 bmi groups between 1-8 days in hospitalization (zoom in plot_2)
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

# filter days of hospitalization between 1 to 7 (9828 out of 10943)
plot_4.5<- groups_days_hos %>% 
  filter(days_of_hospitalization >=7, 
         days_of_hospitalization <= 22)

# plot_4.5 <- plot_4.5 %>% mutate(new_groups =  ifelse(bmi_group == "15-18.5", "15-18.5",
#                                        ifelse(bmi_group == "18.5-25", "18.5-25", "25-45"))) 
# Comparing 6 bmi_groups (separately)
ggplot(plot_4.5, aes(x = days_of_hospitalization, y = bmi_group,  fill = bmi_group, color = bmi_group))+ 
  geom_density_ridges(alpha = 0.4) +
  xlim(c(7,21))+
  scale_x_continuous(breaks = seq(7, 22, by = 3))+
  labs(title = "Hospitalization duration distribution", 
       x = "Duration of hospitalization (days)",
       y = "bmi_group",
       fill = 'BMI group',
       color = 'BMI group',
       subtitle = "Divided by BMI group")+
  theme(plot.title = element_text(face="bold"), axis.title.x = element_text(hjust=0.5),axis.title.y = element_text(hjust=0.5), plot.caption = element_text(hjust=0)) +
  theme_ridges()

# comparing 2 bmi groups between 1-7 days in hospitalization (zoom in plot_4)
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

# use Kruskal Wallis test - we can see the the p-calue is small (2.759e-05) therefore a differnce between the groups
result_31 = kruskal.test(days_of_hospitalization ~ bmi_group,
                         data = plot_5)

pairwise.wilcox.test(plot_5$days_of_hospitalization, plot_5$new_groups,
                     p.adjust.method = "BH")

# define 6 different death_immidaite values by bmi_group (6 groups)
a <- plot_5 %>% 
  filter(bmi_group == '15-18.5') %>% 
  select(days_of_hospitalization)

b <- plot_5 %>% 
  filter(bmi_group == '18.5-25') %>% 
  select(days_of_hospitalization)

c <- plot_5 %>% 
  filter(bmi_group == '25-30') %>% 
  select(days_of_hospitalization)

d <- plot_5 %>% 
  filter(bmi_group == '30-35') %>% 
  select(days_of_hospitalization)

e <- plot_5 %>% 
  filter(bmi_group == '35-40') %>% 
  select(days_of_hospitalization)

f <- plot_5 %>% 
  filter(bmi_group == '40-45') %>% 
  select(days_of_hospitalization)
#each group valus by bmi_group
a1 <- a$days_of_hospitalization
b1 <- b$days_of_hospitalization
c1 <- c$days_of_hospitalization 
d1 <- d$days_of_hospitalization
e1 <- e$days_of_hospitalization
f1 <- f$days_of_hospitalization 

#perform Dunn's Test with Benjamini-Hochberg correction for p-values -  immediate
dunn.test(x=list(a1, b1, c1,d1,e1,f1), method="bh")






