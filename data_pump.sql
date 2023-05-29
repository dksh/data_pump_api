declare
    h1 number;
    dir_name varchar2(30);
    l_date TIMESTAMP;
    file_name varchar2(50);
    create_dir varchar2(100);
    l_days INTEGER;
begin
    dir_name := 'DATA_PUMP'; --name of directory in db
	select 'create or replace directory '||dir_name||' as ''/oradata/'||instance_name||'/data_files/export''' into create_dir from v$instance; --create dynamic directory for DG switchover
    execute immediate create_dir;
    
    SELECT TO_NUMBER(VALUE) INTO l_days FROM DIDAR.SETTINGS where key = 'days_bigger_delete';  --number of days that are used need to delete
    
    for x in (
        select table_owner, table_name, partition_name,high_value
        from   dba_tab_partitions
        where  table_owner = 'DIDAR' and table_name in ('TABLE_1','TABLE_2','TABLE_3','TABLE_4','TABLE_5','TABLE_6','TABLE_7','TABLE_8','TABLE_9') --tables for dumping
        order  by table_owner, table_name, partition_position
    ) loop
    EXECUTE IMMEDIATE 'BEGIN :ret := '||x.high_value||';END;' USING OUT l_date; --takes date of partition 
    IF l_date < sysdate - l_days  --compare dates to get old special partitions	
    THEN
	  BEGIN
		select SUBSTR(x.high_value, 12,10) into file_name from dual;

		h1 := dbms_datapump.open (operation => 'EXPORT', job_mode => 'TABLE');
		dbms_datapump.add_file (
			handle => h1,
			filename => x.table_name||'_'||file_name||'.dmp',
			reusefile => 1, -- REUSE_DUMPFILES=Y
			directory => dir_name,
			filetype => DBMS_DATAPUMP.KU$_FILE_TYPE_DUMP_FILE);
		dbms_datapump.add_file (
			handle => h1,
			filename => 'exp_'||x.table_name||'_'||file_name||'.log',
			directory => dir_name,
			filetype => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE);
		dbms_datapump.set_parameter(
			handle => h1,
			name => 'compression',
			value => 'ALL');
		dbms_datapump.metadata_filter (
			handle => h1,
			name => 'SCHEMA_EXPR',
			value => 'IN ('''||x.table_owner||''')');
		dbms_datapump.metadata_filter (
			handle => h1,
			name => 'NAME_EXPR',
			value => 'IN ('''||x.table_name||''')');
		dbms_datapump.data_filter (
			handle => h1,
			name => 'PARTITION_LIST',
			value => x.partition_name,
			table_name => x.table_name,
			schema_name => x.table_owner);
		dbms_datapump.start_job (handle => h1);
		dbms_datapump.detach (handle => h1);
		END;
	END IF;
	end loop;
end;