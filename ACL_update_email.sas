/*******************************************************************************************
Program:  ACL_update_email.sas
Author:   Robins Wu
Creation  Date: June 22, 2016
Purpose:  Send email to SAS Admin team stating that ACL permissions for SPDS library
          &SYSPARM have been updated.

Modification History

Date            Prgrmr                  Change
******************************************************************************************/

%macro send;
        filename outbox email
             FROM="&EMAIL_FROM"
             to=(&EMAIL_TO)
             cc=("&EMAIL_CC")
              subject="ACL access update for &SYSPARM";


        data _null_;
           file outbox;
           put 'Hi ALL,';
           put " ";
           put "ACL access for &SYSPARM. has been updated by the scheduled process.";
           put " ";
           put "Log file: &ACL_CODEDIR/Logs/Apply_ACLs_User_Libs_&SYSPARM._&Date..log";
           put " ";
           put "Listing file: &ACL_CODEDIR/Listing/Apply_ACLs_User_Libs_&SYSPARM._&Date..lst"; 
           put " ";
           put 'Thanks';
           put 'CAE SAS Admin Team ';
        run;
%mend send;

%send;
