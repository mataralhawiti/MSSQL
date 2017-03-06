CREATE PROC [dbo].[usp_Delete_Indexes]
	(@Tmp_Table NVARCHAR(128))
-- =============================================
-- Author:		Matar
-- Create date: Feb-26-2017
-- Description:	
				--> it takes 1 parameter (1 tables names), then drop all indexes on the given table
				--> most of the code was taken from (https://gist.github.com/LitKnd/2668396699c82220384d2ca2c19bbc32) with some
				   --modifications to make it more dynamic
-- =============================================
	AS
	DECLARE @sqlCmd VARCHAR(MAX);
	DECLARE INDEX_CURSOR CURSOR FOR 
		SELECT 
		CASE si.index_id WHEN 0 THEN N'/* No create statement (Heap) */'
		ELSE 
			CASE is_primary_key WHEN 1 THEN
				N'ALTER TABLE ' + QUOTENAME(sc.name) + N'.' + QUOTENAME(t.name) + N' DROP CONSTRAINT ' + QUOTENAME(si.name) + N'  ' +
				   CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N' '
				ELSE N'DROP ' + 
					N'INDEX ' + QUOTENAME(si.name) + N' ON ' + QUOTENAME(sc.name) + N'.' + QUOTENAME(t.name) + N' '
			END 

		END AS index_create_statement
   
	FROM sys.indexes AS si
	JOIN sys.tables AS t ON si.object_id=t.object_id
	JOIN sys.schemas AS sc ON t.schema_id=sc.schema_id
	LEFT JOIN sys.dm_db_index_usage_stats AS stat ON 
		stat.database_id = DB_ID() 
		and si.object_id=stat.object_id 
		and si.index_id=stat.index_id
	LEFT JOIN sys.partition_schemes AS psc ON si.data_space_id=psc.data_space_id
	LEFT JOIN sys.partition_functions AS pf ON psc.function_id=pf.function_id
	LEFT JOIN sys.filegroups AS fg ON si.data_space_id=fg.data_space_id
	WHERE 
		t.name = @Tmp_Table AND si.type IN (0,1,2) ; /* heap, clustered, nonclustered */


	OPEN INDEX_CURSOR
	FETCH NEXT FROM INDEX_CURSOR INTO @sqlCmd

	WHILE @@FETCH_STATUS = 0
	BEGIN
		EXEC(@sqlCmd)
		FETCH NEXT FROM INDEX_CURSOR INTO @sqlCmd
	END
	CLOSE INDEX_CURSOR
	DEALLOCATE INDEX_CURSOR
GO


