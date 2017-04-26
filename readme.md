# RM-Tools-Unlinked-Reconcile

Stand-alone Java Application to reconcile Unlinked Case References from DRS/Job Manager with those in the Case Service UnlinkedCaseReferences table and submit as a print receipt file for the SDX Gateway.

**Create a folder in MoveIt for the days extracts -> /CTP/RM_Prod_Files/UnlinkedCase_Receipt/yyyymmdd**
    
-- Copy to csv all actions with an actiontypeid of Visit 
    
    COPY (SELECT a.actionid, c.caseref FROM action.action a, casesvc.case c WHERE a.caseid = c.caseid AND a.actiontypeid IN(14,52)) TO 'VisitJobs_yyyymmddhhmm.csv' DELIMITER ',' csv HEADER;

-- Copy to csv all records from unlinkedcasereceipt table 
    
    COPY (SELECT * from casesvc.unlinkedcasereceipt) TO 'Unlinkedcasereceipt_yyyymmddhhmm.csv' DELIMITER ',' csv HEADER;

Copy these two files to the directory on MoveIt.
