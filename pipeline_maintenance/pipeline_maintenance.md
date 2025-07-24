## Pipeline Maintenance
### Pipelines:
* Profit
* Growth
* Engagement
* Aggregate data for investors
* Aggregate data for experiments team
---
### Runbooks:
* Pipeline: **Profit**
* Description: Revenue datasets from accounts  
* Owner:
    - Primary: Danny
    - Secondary: Jane  

* Schedule: Daily @ 2:00 AM UTC  
* Upstream owner: Account team 
* Downstream owner: Data analyst team
* Common issues:
    - Numbers don't align with numbers on accounts - these numbers need to be verified by an accountant.
* SLAs: 
    - The data should be updated by 3:00 AM UTC.
* On-call schedule:
    - If pipeline fails, it will be debugged by team during work hours.
---
<br>

* Pipeline: **Growth**
* Description: Users that subscribe the license  
* Owner:
    - Primary: Laura
    - Secondary: Danny  

* Schedule: Daily @ 1:00 AM UTC  
* Upstream owner: Customercare team 
* Downstream owner: Data science team
* Common issues:
    - Spark task fails with NullPointerException - to check upstream whether any missing required column
* SLAs: 
    - The data should be updated by 4:00 AM UTC.
    - The data will contain latest account statuses.
    - If pipeline fails, it will be fixed within 24 hour from midnight UTC.
* On-call schedule:
    - One person from DE team will be standby each week including weekends. Regular schedule:
        * week 1: Danny
        * week 2: Anna
        * week 3: Laura
        * week 4: Jane
    - Need to have 30 mins meeting for on-call transition.
    - Need to check with team members for substitution on on-call week.
---
<br>

* Pipeline: **Engagement**
* Description: User clicks on the platform
* Owner:
    - Primary: Anna
    - Secondary: Laura  

* Schedule: Daily @ 1:00 AM UTC  
* Upstream owner: Software frontend team 
* Downstream owner: Customercare team, Data science team
* Common issues:
    - Zero rows - if there is no Kafka input, need to check the Kafka topics.
    - OOM issue - Check the partitions if there is data skew
* SLAs: 
    - The data should be updated by 5:00 AM UTC.
    - If pipeline fails, it will be fixed within 24 hour from midnight UTC.
* On-call schedule:
    - One person from DE team will be standby each week including weekends and holidays. Regular schedule:
        * week 1: Danny
        * week 2: Anna
        * week 3: Laura
        * week 4: Jane
    - Need to have 30 mins meeting for on-call transition.
    - Need to check with team members for substitution on on-call week.
---
<br>

* Pipeline: **Aggregate data for investors**
* Description: Data that is required to check how company is doing
* Owner:
    - Primary: Jane
    - Secondary: Anna  

* Schedule: Weekly @ 1:00 AM UTC  
* Upstream owner: Software frontend team 
* Downstream owner: Dashboard, Data science team
* Common issues:
    - Missing data causes by issues with NA or divide by 0 error
* SLAs: 
    - Issues will be fixed by end of month before generating report for management.
* On-call schedule:
    - No on-call since it only requires for investors. DE need to check the pipeline every week.