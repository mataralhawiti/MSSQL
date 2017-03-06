CREATE PROC [dbo].[usp_Replicate_Indexes]
-- =============================================
-- Author:		Matar
-- Create date: Feb-26-2017
-- Description:	
				--> it takes 2 parameters (2 tables names), then replicate indexes from table @Main_Table on @Tmp_Table
				--> most of the code was taken from (https://gist.github.com/LitKnd/2668396699c82220384d2ca2c19bbc32) with some
				   --modifications to make it more dynamic

-- =============================================

	(@Tmp_Table NVARCHAR(128),
	 @Main_Table NVARCHAR(128))
	AS
	DECLARE @sqlCmd VARCHAR(MAX);
	DECLARE INDEX_CURSOR CURSOR FOR 
		SELECT 
			CASE si.index_id WHEN 0 THEN N'/* No create statement (Heap) */'
			ELSE 
				CASE is_primary_key WHEN 1 THEN
					N'ALTER TABLE ' + QUOTENAME(sc.name) + N'.' + QUOTENAME(@Tmp_Table) + N' ADD CONSTRAINT ' + QUOTENAME('PK_'+@Tmp_Table) + N' PRIMARY KEY ' +
					   CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED '
					ELSE N'CREATE ' + 
						CASE WHEN si.is_unique = 1 then N'UNIQUE ' ELSE N'' END +
						CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED ' +
						N'INDEX ' + QUOTENAME(si.name) + N' ON ' + QUOTENAME(sc.name) + N'.' + QUOTENAME(@Tmp_Table) + N' '
				END +
				/* key def */ N'(' + key_definition + N')' +
				/* includes */ CASE WHEN include_definition IS NOT NULL THEN 
					N' INCLUDE (' + include_definition + N')'
					ELSE N''
				END +
				/* filters */ CASE WHEN filter_definition IS NOT NULL THEN 
					N' WHERE ' + filter_definition ELSE N''
				END +
				/* with clause - compression goes here */
				CASE WHEN row_compression_partition_list IS NOT NULL OR page_compression_partition_list IS NOT NULL 
					THEN N' WITH (' +
						CASE WHEN row_compression_partition_list IS NOT NULL THEN
							N'DATA_COMPRESSION = ROW ' + CASE WHEN psc.name IS NULL THEN N'' ELSE + N' ON PARTITIONS (' + row_compression_partition_list + N')' END
						ELSE N'' END +
						CASE WHEN row_compression_partition_list IS NOT NULL AND page_compression_partition_list IS NOT NULL THEN N', ' ELSE N'' END +
						CASE WHEN page_compression_partition_list IS NOT NULL THEN
							N'DATA_COMPRESSION = PAGE ' + CASE WHEN psc.name IS NULL THEN N'' ELSE + N' ON PARTITIONS (' + page_compression_partition_list + N')' END
						ELSE N'' END
					+ N')'
					ELSE N''
				END +
				/* ON where? filegroup? partition scheme? */
				' ON ' + CASE WHEN psc.name is null 
					THEN ISNULL(QUOTENAME(fg.name),N'')
					ELSE psc.name + N' (' + partitioning_column.column_name + N')' 
					END
				+ N';'
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
		/* Key list */ OUTER APPLY ( SELECT STUFF (
			(SELECT N', ' + QUOTENAME(c.name) +
				CASE ic.is_descending_key WHEN 1 then N' DESC' ELSE N'' END
			FROM sys.index_columns AS ic 
			JOIN sys.columns AS c ON 
				ic.column_id=c.column_id  
				and ic.object_id=c.object_id
			WHERE ic.object_id = si.object_id
				and ic.index_id=si.index_id
				and ic.key_ordinal > 0
			ORDER BY ic.key_ordinal FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS keys ( key_definition )
		/* Partitioning Ordinal */ OUTER APPLY (
			SELECT MAX(QUOTENAME(c.name)) AS column_name
			FROM sys.index_columns AS ic 
			JOIN sys.columns AS c ON 
				ic.column_id=c.column_id  
				and ic.object_id=c.object_id
			WHERE ic.object_id = si.object_id
				and ic.index_id=si.index_id
				and ic.partition_ordinal = 1) AS partitioning_column
		/* Include list */ OUTER APPLY ( SELECT STUFF (
			(SELECT N', ' + QUOTENAME(c.name)
			FROM sys.index_columns AS ic 
			JOIN sys.columns AS c ON 
				ic.column_id=c.column_id  
				and ic.object_id=c.object_id
			WHERE ic.object_id = si.object_id
				and ic.index_id=si.index_id
				and ic.is_included_column = 1
			ORDER BY c.name FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS includes ( include_definition )
		/* Partitions */ OUTER APPLY ( 
			SELECT 
				COUNT(*) AS partition_count,
				CAST(SUM(ps.in_row_reserved_page_count)*8./1024./1024. AS NUMERIC(32,1)) AS reserved_in_row_GB,
				CAST(SUM(ps.lob_reserved_page_count)*8./1024./1024. AS NUMERIC(32,1)) AS reserved_LOB_GB,
				SUM(ps.row_count) AS row_count
			FROM sys.partitions AS p
			JOIN sys.dm_db_partition_stats AS ps ON
				p.partition_id=ps.partition_id
			WHERE p.object_id = si.object_id
				and p.index_id=si.index_id
			) AS partition_sums
		/* row compression list by partition */ OUTER APPLY ( SELECT STUFF (
			(SELECT N', ' + CAST(p.partition_number AS VARCHAR(32))
			FROM sys.partitions AS p
			WHERE p.object_id = si.object_id
				and p.index_id=si.index_id
				and p.data_compression = 1
			ORDER BY p.partition_number FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS row_compression_clause ( row_compression_partition_list )
		/* data compression list by partition */ OUTER APPLY ( SELECT STUFF (
			(SELECT N', ' + CAST(p.partition_number AS VARCHAR(32))
			FROM sys.partitions AS p
			WHERE p.object_id = si.object_id
				and p.index_id=si.index_id
				and p.data_compression = 2
			ORDER BY p.partition_number FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS page_compression_clause ( page_compression_partition_list )
		WHERE 
			t.name = @Main_Table and si.type IN (0,1,2) /* heap, clustered, nonclustered */

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


