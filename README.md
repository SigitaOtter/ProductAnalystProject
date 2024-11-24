# Product Analysis - Project


## Task

Identify how much time it takes for a user to make a purchase on website.
Show the users' duration from first arriving on the website on any given day until their first purchase on that same day. 
Show the duration dynamic daily.


## Dataset

Dataset is made of fake raw events data. <br>
Dataset includes data about 4396 purchases between 2020-11-01 and 2021-01-30.
Purchases with _null_ for number of items in purchase are omitted as incorrect entries. 
If same buyer makes 2 purchases per day, only first one is concidered.
Returning customer is a customer who bought on least 2 days in the analysis period.
Repeat purchase is a purchase made not on the first day that same buyer has bought something.


## Main Insights

It takes ~ 18 minutes to make a purchase. 
The quickest purchases happened as quick as 35 seconds, while the longest took over 23 hours.

Factors that influence the duration of purchase:
- number of items being bought - buying 1 item takes ~12 minutes vs ~31 minutes for buying 7 items
- first or repeat purchase - first purchase takes 19-20 minutes while only ~12 minutes on return to buy more


## Visualization

![image](https://github.com/user-attachments/assets/a5566f4c-6c0e-4c14-9bd7-e465e98b1aff)


## Link to Visualization

https://public.tableau.com/app/profile/sigita.vismine/viz/ProductAnalystProject_17313579939520/Dashboard1?publish=yes


## Areas for Improvement of Analysis

- Further investigation on why UK customers seem to be buying faster
- A look at the sales funnel: which of the steps in the sales funnel is taking up the most time (e.g. repeat purchases might appear to be happening faster just because customer already has presaved login or payment or shipment details)
- Search for non linear relation between item price or total purchase amount and purchase duration


## List of Attachments

1. SQL query:
- the first part of the query is for exploration and validation
- the second part of the query is the final dataset for further analysis in visualisations

2. Final dataset in csv format
