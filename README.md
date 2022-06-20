# SISE2601 Project data description
================
Team members : Tal Hayun, Oded Regev

This Markdown file describes the data folder structure and organization ...

## obesity_measurement [25077 X 11]

- person_id: A unique identifier for each person (Primary key).

- bmi_group: A range that refers to six different bmi groups. groups values: 15-18.5, 18.5-25, 25-30, 30-35, 35-40, 40-45.

- bmi_value: A floating value that refers the patient's bmi.
  minimum: 15.04.
  mean: 27.08.
  maximum: 45.
  standard deviation: 5.403.

- visit_start_date: The start date of the visit.
  earliest visit: 1984-02-01.
  latest visit: 2020-12-28.
  
- visit_end_date: The end date of the visit. 
  earliest visit: 1984-02-17.
  latest visit: 2021-04-23.
  
- discharge_to: The place where the patient was released after hospitalization. Divided to 5 places.
  count of home_visit: 17787.
  count of observation room: 905.
  count of expired: 113.
  count of Alternate care site (ACS): 439.
  count of No matching concept: 5833.

- death_date: The date the person was deceased.

- year_of_birth: year_of_birth: The year of birth of the person.
  oldest patient yearh of birth : 1907.
  youngest patient yearh of birth : 2001.
  standard deviation: 21.395.

- gender: The gender of the person (Male or Female)
  gender frequency: male - 50.3%, femele - 49.7%.

- age_during_hospitalization: The number of years that the patient have been in hospitalization.

- binary_death: Binary value that refers if the patient death or not (0-live, 1- died).
  death frequency: live - 67.74%, died - 32.26%.
  
