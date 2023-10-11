# Yelp_data Runbook
 To view data_clean_pipeline.py in a more readable way, please download: https://drive.google.com/file/d/1x1ZPbYz9rdSAjVY73tCEuGa4I-fplUDZ/view?usp=sharing

# 
#### 1. Project Goal:
- The goal of this project is to ETL data using Databricks and achieve the “delta lake process” within SCD type II policies to track data changes. (Raw data -> brown -> sliver.)

#### 2. Environment Configuration:
- Ensure that the Databricks environment is configured and connected to the Azure cloud.
- Ensure that all required libraries and dependencies are installed in Databricks.

#### 3. Data Loading:
- Load 5 raw json file from the specified source into the Databricks environment.
- **yelp_academic_dataset_business.json**:
"root":{14 items
"business_id":string"Pns2l4eNsfO8kk83dixA6A"
"name":string"Abby Rappoport, LAC, CMQ"
"address":string"1616 Chapala St, Ste 2"
"city":string"Santa Barbara"
"state":string"CA"
"postal_code":string"93101"
"latitude":float34.4266787
"longitude":float-119.7111968
"Stars":int 5
"Review_count":int 7
"is_open":int0
"attributes":{1 item
"ByAppointmentOnly":string"True"}
"categories":string"Doctors, Traditional Chinese Medicine, Naturopathic/Holistic, Acupuncture,
Health & Medical, Nutritionists"
"hours":NULL}
- **yelp_academic_dataset_checkin.json**:
"root":{2 items
"business_id":string"---kPU91CF4Lq2-WlRu9Lw"
"date":string"2020-03-13 21:10:56, 2020-06-02 22:18:06, 2020-07-24 22:42:27, 2020-10-24
21:36:13, 2020-12-09 21:23:33, 2021-01-20 17:34:57, 2021-04-30 21:02:03, 2021-05-25 21:16:54,
2021-08-06 21:08:08, 2021-10-02 15:15:42, 2021-11-11 16:23:50"}
- **yelp_academic_dataset_review.json**:
“root":{9 items
"review_id":string"KU_O5udG6zpxOg-VcAEodg"
"user_id":string"mh_-eMZ6K5RLWhZyISBhwA"
"business_id":string"XQfwVwDr-v0ZS3_CbbE5Xw"
"stars":int3
"useful":int0
"funny":int0
"cool":int0
"text":string"If you decide to eat here, just be aware it is going to take about 2 hours from beginning
to end. We have tried it multiple times, because I want to like it! I have been to it's other locations in
NJ and never had a bad experience. The food is good, but it takes a very long time to come out.
The waitstaff is very young, but usually pleasant. We have just had too many experiences where
we spent way too long waiting. We usually opt for another diner or restaurant on the weekends, in
order to be done quicker."
"date":string"2018-07-07 22:09:11"}
- **yelp_academic_dataset_tip.json**:
"root":{5 items
"user_id":string"AGNUgVwnZUey3gcPCJ76iw"
"business_id":string"3uLgwr0qeCNMjKenHJwPGQ"
"text":string"Avengers time with the ladies."
"date":string"2012-05-18 02:17:21"
"compliment_count":int0}
- **yelp_academic_dataset_user.json**:
"root":{9 items
"user_id":string"qVc8ODYU5SZjKXVBgXdI7w"
"name":string"Walker"
"review_count":int585
"yelping_since":string"2007-01-25 16:47:26"
"useful":int7217
"funny":int1259
"cool":int5994
"elite":string"2007"

#### 4. Data Preprocessing:
- Clean up the raw data and remove or fill in any missing or outlier values.
- Convert data types or formats and generate new columns as needed.
- The cleaned dataset should contain the following fields:
**final_df**
[user_id:string,
business_id:string
tip_count:integer
checkin_count:integer
num_of_user_who_reviewed:integer
business_name:string
business_name:integer
business_name:city:string
business_name:city:string
business_name:string
review_count:integer
review_count:integer
avg_stars_single_user:double
latest_review_date:timestamp
review_stars_single_user:double latest_review_date:timestamp
latest_review_date:timestamp user_name:string]


#### 5. SCD Type II Processing:
1. **Initialization Phase**:
   - Add `start_date`, `end_date` and `flag` fields to the raw data.
   - Save the processed data in Delta format.
   
2. **Incremental loading**:
   - Apply SCD Type II processing to new data changes and inserts.
   - Flag any changed records.
   - Use the `merge` operation to apply the changes to the Delta table.

#### 6. Data persistence:
- Periodically save the Delta table to external storage or database to ensure data persistence.

#### 7. Problem Troubleshooting:
- If you encounter an error, refer to the error log for the relevant error message and code.
- Take appropriate troubleshooting steps according to the type of error.

#### 8. Frequently Asked Questions and Solutions:
- **`IllegalArgumentException`**: Make sure that all operations are aligned to the correct fields and data structures.
- **`ProtocolChangedException`**: This error may be caused by concurrent writes. Please make sure that there are no other concurrent operations while writing data.

#### 9. End of project:
- Ensure that all data has been saved.
- Close any unused resources such as SparkSession or Databricks clusters.

#### 10. Contact Information:
- In an emergency, contact [Team Lead email: chenyuejin2023@gmail.com].
 
