/************************************************************************************************************
Program: load_ACL.sas
Author: Robins Wu
Creation Date: July 7, 2016
Purpose: Create shell script ACL_Update.ksh that will update ACL permissions for SPDS libraries associated 
         with changed SAS metadata groups.

Modification Bistory

Date         Programmer     Modification
12/27/2016   Robins Wu      Path Changed
************************************************************************************************************/

filename AUTODIR '/sanpfs-sasinstall/sas/install/9.4/sashome/grid_binaries/SASFoundation/9.4/sasautos';
options sasautos=autodir /* source2  */ ;

%include "/export/home/sasadmp/mydata/ADM_PRG/AD_Dir_Sync/active-directory/load_from_active_directory_parameter.sas";

/*  Get system Date from parameter file for use in poroc printto */

proc printto log="&CODEDIR/load_ACL_&Date..log"
             print="&CODEDIR/load_ACL_&Date..lst";
run;


libname adir "&DATADIR./active-directory/sas_metadata_sync/ADIRextract";
libname metaE "&DATADIR./active-directory/sas_metadata_sync/METAextract";
libname updates "&DATADIR./active-directory/sas_metadata_sync/METAupdates";





/*  Get system date time  */

data _null_;
dt = datetime();
format dt datetime21.;
call symputx('system_datetime',dt);
run;

/*******************************************************
Start ACL Update Process
*******************************************************/

%macro rungrid_file;
/*  Get number of records in data set updates.grpmems_delete and updates.grpmems_add  */

        data updates.grpmems_delete;
        set updates.grpmems_delete;
        if index (grpkeyid,'_Read') > 0 or index (grpkeyid,'_RW') > 0 or index (grpkeyid,'_Deny') > 0;
        run;

        data updates.grpmems_add;
        set updates.grpmems_add;
        if index (grpkeyid,'_Read') > 0 or index (grpkeyid,'_RW') > 0 or index (grpkeyid,'_Deny') > 0;
        run;


        %obsnvars (dsn=updates.grpmems_delete);
        %obsnvars (dsn=updates.grpmems_add);

        %if &grpmems_delete_nobs > 0 or &grpmems_add_nobs > 0 %then
        %do;
                data updated_md_groups;
                set updates.grpmems_delete
                    updates.grpmems_add;
                run;


                /*  Ger the libnames from the changed metadata group names  */

                data updated_md_groups;
                set updated_md_groups;

                equals_pos = index(grpkeyid,'=');
                comma_pos = index(grpkeyid,',');

                group = substr(grpkeyid,equals_pos+1,(comma_pos - equals_pos) - 1);

                last_underscore = FIND(group,'_',-length(trim(left(GROUP))) + 1);
                nbr_underscrores = count(group,'_');

 /*               length libname $8;  */
                length libname $11;

                if nbr_underscrores = 1 and substr(group,last_underscore+1,1) not in ('R','D') then
                    libname = group;
                else
                    libname = substr(group,1,last_underscore-1);
                run;


                /*  Get distinct libnames  */

                proc sort data=updated_md_groups nodupkey;
                by libname;
                run;

                data _null_;
                set updated_md_groups end=last;

                call symputx(cats('changed_libname',_n_), libname);

                if last then
                  call symputx('nbr_changed_libnames',_n_);
                run;

%put changed_libname1 = &changed_libname1;
%put changed_libname2 = &changed_libname2;
%put changed_libname3 = &changed_libname3;
%put nbr_changed_libnames = &nbr_changed_libnames;


                /*  Write shell script that will launch ACL permissions update for the changed SPDS libraries  */

                filename rungrid "&CODEDIR/ACL_Update.ksh";
                data _null_;
                length Date $17;
                Date = "`date +";
                Date = cats(Date,'%Y%m%d:%T`');
                file rungrid;
                put '#!/bin/ksh';
      /*          put "Date=`date +%Y%m%d:%T`";  */
                put 'Date=' Date;
                put 'alias rungrid="/sanpfs-sasusers/mydata/sasadmp/ADM_PRG/scripts/rungrid"';
                put "export Date=$Date";
                put " ";
                %do i = 1 %to &nbr_changed_libnames;
                  put "export SYSPARM=&&changed_libname&i";
                  put "rungrid &ACL_CODEDIR./Apply_ACLs_User_Libs_batch.sas";
                %end;
                run;

                /* Execute ACL udpate for changed SPDS libraries  */

                %sysexec &CODEDIR./ACL_Update.ksh;

                %if &SYSRC = 0 %then
                  %put ACL update complete;
                %else
                  %put ERROR - ACL Update Failed;
        %end;
%mend rungrid_file;

%rungrid_file;


