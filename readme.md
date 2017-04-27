# RM-Tools-Unlinked-Reconcile
<br/>

### Before running this tool:
Make sure you have completed the following:
1. Added config.properties the base location within this project (this contains relevant sftp information, private file paths/names etc).
2. Have downloaded copies of *linked_caserefs_master.csv* and *caserefs_to_check.csv* to */tmp/CaseRefs/* (Mac or Linux).
3. Have uploaded one of each of the following to */CTP/RM_Prod_Files/UnlinkedCase_Receipt/* in MoveIt:
 * HH Questionaires Issued In Field [DATE].csv
 * Unlinkedcasereceipt_yyyymmddhhmm.csv
 * VisitJobs_yyyymmddhhmm.csv

You can now run the application.
<br/><br/>

### Expected Output:

* *Receipts_Generated_[Datestamp].csv* (new) containing the caseref and dateOfVisit from each questionnaire that has been matched with the Unlinkedcasereceipt Extract.
* *caserefs_to_check.csv* (updated) containing any new caserefs to check from the daily VisitJobs extract.
* *linked_caserefs_master.csv* (updated) containing the questionnaireId(unlinked caseref), caseref and dateOfVisit for all Questionnaires that have been recevied via DRS but not been matched with the Unlinkedcasereceipt Extract.
* *scanned_not_matched.csv* (updated) containing caserefs of any files that have been scanned but not matched to DRS report.
<br/><br/>

### Generating Extracts from Production Database:

Create a folder in MoveIt for the days extracts (if it does not already exist) */CTP/RM_Prod_Files/UnlinkedCase_Receipt/yyyymmdd*
    
Copy to csv all actions with an actiontypeid of Visit 
>COPY (SELECT a.actionid, c.caseref FROM action.action a, casesvc.case c WHERE a.caseid = c.caseid AND a.actiontypeid IN(14,52)) TO 'VisitJobs_yyyymmddhhmm.csv' DELIMITER ',' csv HEADER;

Copy to csv all records from unlinkedcasereceipt table 
>COPY (SELECT * from casesvc.unlinkedcasereceipt) TO 'Unlinkedcasereceipt_yyyymmddhhmm.csv' DELIMITER ',' csv HEADER;

Copy these two files to the directory on MoveIt.
