CREATE OR REPLACE procedure SYS.DROP_PARTTIONS IS
	l_date TIMESTAMP;
	l_days INTEGER; --number of days for determining the age of partitions
BEGIN

	SELECT TO_NUMBER(VALUE) INTO l_days FROM DIDAR.SETTINGS where key = 'days_bigger_delete';  --takes number of days from setting table

	FOR a IN (
		SELECT PARTITION_NAME,HIGH_VALUE,TABLE_OWNER,TABLE_NAME
		FROM DBA_TAB_PARTITIONS 
		WHERE TABLE_OWNER = 'DIDAR' AND TABLE_NAME IN ('TABLE_1','TABLE_2','TABLE_3','TABLE_4','TABLE_5','TABLE_6','TABLE_7','TABLE_8','TABLE_9') --tables for dumping partitions
		AND PARTITION_NAME NOT IN ('P0') --the first partition cannot be drop
			 ) LOOP
		EXECUTE IMMEDIATE 'BEGIN :ret := '||a.HIGH_VALUE|'; END;' USING OUT l_date; --takes date of partition
		If l_date < sysdate - l_date THEN --compare dates to get old special partitions	
		BEGIN
			EXECUTE IMMEDIATE 'ALTER TABLE '||a.TABLE_OWNER||'.'||a.TABLE_NAME||' TRUNCATE PARTITION '||a.PARTITION_NAME||' UPDATE GLOBAL INDEXES PARALLEL 16'; --truncating partitons before droping to accelerate 
			EXECUTE IMMEDIATE 'ALTER TABLE '||a.TABLE_OWNER||'.'||a.TABLE_NAME||' DROP PARTITION '||a.PARTITION_NAME||' UPDATE GLOBAL INDEXES PARALLEL 16'; --droping partitons after truncate it will be fast
		End;
		End if; 
	End loop;
End;