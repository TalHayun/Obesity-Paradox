# SISE2601 Project data description - Obesity Paradox

## Introduction
Obesity is one of the great epidemics of the 21st century. It is considered a risk factor for many diseases and can cause a decrease in function, quality of life and an increase in mortality. However, in many cases of acute stress, one encounters an opposite and surprising phenomenon called the "obesity paradox", in which a high BMI (Body mass index) is a protective factor against mortality. This phenomenon exists in inpatients in different wards of hospitals and is particularly prevalent among patients with acute infectious disease. However, there exists a small number of studies that show reversed relationship, in which obesity is indeed a risk factor for mortality after hospitalization. For example, one study1 has found that in examining the effect of obesity after one year from hospitalization, the paradox disappeared.

## Research Questions
In this research, we would like to examine how the BMI rate affects mortality
in an immediate period (0-6 months), after one year and five years from the moment of hospitalization, among patients with infectious disease who were hospitalized in the various wards.
Secondly, we would like to check the effects of BMI on hospitalization outcomes, such as: duration of hospitalization and weather the patient discharged home at the end of hospitalization.
In addition, we will examine which confounding variables may affect the obesity paradox, for example: age, gender and amount of amount of albumin in the blood (a common protein).

## Useful
Understanding the association of obesity on survival after acute hospitalization is important to guiding weight control recommendations.


# Appendix 
### obesity_measurement [25077 X 13]

1. person_id: A unique identifier for each person (Primary key).

2. bmi_group: A range that refers to six different bmi groups.
   - groups values: 15-18.5, 18.5-25, 25-30, 30-35, 35-40, 40-45.

3. bmi_value: A floating value that refers the patient's bmi.
   - minimum: 15.04.
   - mean: 27.08.
   - maximum: 45.
   - standard deviation: 5.403.

4. visit_start_date: The start date of the visit.
    - earliest visit: 1984-02-01.
    - latest visit: 2020-12-28.
  
5.  visit_end_date: The end date of the visit. 
  - earliest visit: 1984-02-17.
  - latest visit: 2021-04-23.
  
  6. discharge_to: The place where the patient was released after hospitalization. Divided to 5 places.
    - count of home_visit: 17787.
    - count of observation room: 905.
    - count of expired: 113.
    - count of Alternate care site (ACS): 439.
    - count of No matching concept: 5833.

7. death_date: The date the person was deceased.

8. year_of_birth: year_of_birth: The year of birth of the person.
    - oldest patient yearh of birth : 1907.
    - youngest patient yearh of birth : 2001.
    - standard deviation: 21.395.

9. gender: The gender of the person (Male or Female)
    - gender frequency: male - 50.3%, femele - 49.7%.

10. age_during_hospitalization: The number of years that the patient have been in hospitalization.

11. binary_death: Binary value that refers if the patient death or not (0-live, 1- died).
    - death frequency: live - 67.74%, died - 32.26%.
  
12. dur_of_hospitalizaition: Numberical value of inpatient's days in hospitalization.

13. albumin: A floating value that refers the patient's albumin.
  
