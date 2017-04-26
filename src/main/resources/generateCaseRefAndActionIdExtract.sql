SELECT a.actionid, c.caseref
FROM action.action a
  , casesvc.case c
WHERE a.caseid = c.caseid
AND a.actiontypeid IN(14,52);