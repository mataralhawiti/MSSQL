CREATE PROC [dbo].[usp_Partition_Switch_Merge]
	(@Tmp_Table NVARCHAR(128),
	 @Main_Table NVARCHAR(128))
	AS
/**
-- =============================================
-- Author:		Matar
-- Create date: Feb-26-2017
-- Description:	
				--> it takes 2 parameters (2 tables names) then do the below :
					1. calls sp [usp_Replicate_Indexes], to create indexes on @Tmp_Table
					2. prepare, and execute paritioins switching script. from @Tmp_Table to @Main_Table
					3. prepare, then execute merge script to move remaining records after paritioin switching
					4. calls sp [usp_Delete_Indexes], drop indexes from Tmp_Table
-- =============================================
**/	
	DECLARE @sqlMerge	VARCHAR(max) = '';
	DECLARE @sqlIndex	VARCHAR(max) = '';
	DECLARE @sqlSwitch	VARCHAR(max) = '';
	DECLARE @sqlDropIX	VARCHAR(max) ;

	DECLARE @list1		VARCHAR(max) = '';
	DECLARE @list2		VARCHAR(max) = '';
	DECLARE @list3		VARCHAR(max) = '';
	DECLARE @list4		VARCHAR(max) = '';
	DECLARE @listIX		VARCHAR(max) = '';

	DECLARE @ps			VARCHAR(max) ;
	DECLARE @pf			VARCHAR(max) ;
	DECLARE @pc			VARCHAR(max) ;

	DECLARE @ErrorMsg	VARCHAR(4000);
	DECLARE @ErrorNo	INT ;
	

	-- start logging
	BEGIN TRY
		INSERT INTO [dbo].[SWITCH_MERGE_LOG] ([L_DATE],[TABLE_NAME])
		VALUES (CAST(GETDATE() AS DATE), @Main_Table);
	END TRY
	BEGIN CATCH
		--THROW
	END CATCH;


	-- # Prepare Switch ------------------------------------------------------------------------------------------------
	--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==
	SET @ps =	(SELECT PS.name AS ps_name
				FROM sys.indexes i
					INNER JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
					INNER JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
				WHERE i.object_id = OBJECT_ID(@Main_Table) AND i.[type] = 1 )


	SET @pc = (SELECT c.name  PartitioningColumnName   
				FROM sys.tables t  
					INNER JOIN sys.indexes i   
						ON i.object_id = t.object_id   
					INNER JOIN sys.index_columns ic  
						ON ic.index_id = i.index_id   
							AND ic.object_id = t.object_id  
					INNER JOIN sys.columns c  
						ON c.object_id = ic.object_id   
							AND c.column_id = ic.column_id  
				WHERE t.object_id  = object_id(@Main_Table) AND   
					ic.partition_ordinal = 1 AND i.[type] = 1 )


	SET @pf =	(SELECT pf.name AS pf_name
				FROM sys.indexes i
					INNER JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
					INNER JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
				WHERE i.object_id = OBJECT_ID(@Main_Table) AND i.[type] = 1)


     SET @sqlSwitch = 'DECLARE @max_p INT = 0 ;
					   SET @max_p = (SELECT MAX($PARTITION.' + @pf + '(' + @pc +')) FROM ' + @Tmp_Table +')
					   IF (@max_p IS NOT NULL)
							ALTER TABLE ' + @Tmp_Table + ' SWITCH PARTITION ' +'@max_p TO ' + @Main_Table + ' PARTITION @max_p ;' 
	


	
	-- # Prepare MERGE script ------------------------------------------------------------------------------------------
	--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==
	
	-- Get the join columns 
	select @list1 = @list1 + 'T.[' + c.COLUMN_NAME + '] = S.[' + c.COLUMN_NAME + '] AND '
	from INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ,
	INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
	where pk.TABLE_NAME = @Main_Table
	and CONSTRAINT_TYPE = 'PRIMARY KEY'
	and c.TABLE_NAME = pk.TABLE_NAME
	and c.CONSTRAINT_NAME = pk.CONSTRAINT_NAME
	
	SELECT @list1 = LEFT(@list1, LEN(@list1) -3)



	-- WHEN MATCHED 
	SELECT @list2 = ''

	SELECT @list2 = @list2 + ' T.[' + [name] + '] = S.[' + [name] +'],'
	from sys.columns
	where object_id = object_id(@Tmp_Table)
	-- don't update primary keys
	and [name] NOT IN (SELECT [column_name]
	from INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ,
	INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
	where pk.TABLE_NAME = @Tmp_Table
	and CONSTRAINT_TYPE = 'PRIMARY KEY'
	and c.TABLE_NAME = pk.TABLE_NAME
	and c.CONSTRAINT_NAME = pk.CONSTRAINT_NAME)
	-- and don't update identity columns
	and columnproperty(object_id(@Tmp_Table), [name], 'IsIdentity ') = 0 


	
	-- WHEN NOT MATCHED BY TARGET 
		-- Get the insert list
		SET @list3 = ''
	
		SELECT @list3 = @list3 + '[' + [name] +'], '
		from sys.columns
		where object_id = object_id(@Tmp_Table)
	
		SELECT @list3 = LEFT(@list3, LEN(@list3) - 1)
	
	
		-- get the values list
		SET @list4 = ''

		SELECT @list4 = @list4 + 'S.[' +[name] +'], '
		from sys.columns
		where object_id = object_id(@Tmp_Table)

		SELECT @list4 = LEFT(@list4, LEN(@list4) - 1)

	-- get sqlCMD
	SET @sqlMerge =  'MERGE [dbo].[' + @Main_Table + '] AS T ' + 'USING [dbo].[' + @Tmp_Table + '] as S ' +
					 'ON ( ' + @list1 + ') ' +
					 'WHEN MATCHED THEN UPDATE SET ' + left(@list2, len(@list2) -1 ) +
					 ' WHEN NOT MATCHED BY TARGET THEN ' + ' INSERT(' + @list3 + ') ' + ' VALUES(' + @list4 + ')' + ';'





	-- # EXECUTION ----------------------------------------------------------------------------------------------------
	--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==--==

	-- 1
	BEGIN TRY
		EXEC dbo.usp_Replicate_Indexes @Tmp_Table, @Main_Table ;
		
		UPDATE SWITCH_MERGE_LOG
		SET IX_CREATION = 'Indexes  were sucessfully created on Temp table'
		WHERE L_DATE = CAST(GETDATE() AS DATE) AND TABLE_NAME = @Main_Table;

	END TRY
	BEGIN CATCH
		SET @ErrorMsg = ERROR_MESSAGE();
		SET @ErrorNo = ERROR_NUMBER();

		UPDATE SWITCH_MERGE_LOG
		SET IX_CREATION = @ErrorMsg + CAST(@ErrorNo AS VARCHAR(5))
		WHERE L_DATE = CAST(GETDATE() AS DATE) AND TABLE_NAME = @Main_Table;
	END CATCH;

	---- 2
	BEGIN TRY
		EXEC(@sqlSwitch) ;
		
		UPDATE SWITCH_MERGE_LOG
		SET P_SWITCHING = 'Switching partitions was sucessful'
		WHERE L_DATE = CAST(GETDATE() AS DATE) AND TABLE_NAME = @Main_Table;
	END TRY
	BEGIN CATCH
		SET @ErrorMsg = ERROR_MESSAGE();
		SET @ErrorNo = ERROR_NUMBER();

		IF @ErrorNo = 4904
			BEGIN
				UPDATE SWITCH_MERGE_LOG
				SET P_SWITCHING = 'Switching partitions was skipped ' + CAST(@ErrorNo AS VARCHAR(5))
				WHERE L_DATE = CAST(GETDATE() AS DATE) AND TABLE_NAME = @Main_Table;
			END
		ELSE 
			BEGIN
				UPDATE SWITCH_MERGE_LOG
				SET P_SWITCHING = @ErrorMsg + CAST(@ErrorNo AS VARCHAR(5))
				WHERE L_DATE = CAST(GETDATE() AS DATE) AND TABLE_NAME = @Main_Table
				RETURN
			END
			

	END CATCH;

	-- 3
	BEGIN TRY
		EXEC(@sqlMerge) ;

		UPDATE SWITCH_MERGE_LOG
		SET I_U_MERGE = 'MERGE operation was sucessfull '
		WHERE L_DATE = CAST(GETDATE() AS DATE) AND TABLE_NAME = @Main_Table;

	END TRY
	BEGIN CATCH
		SET @ErrorMsg = ERROR_MESSAGE();
		SET @ErrorNo = ERROR_NUMBER();

		UPDATE SWITCH_MERGE_LOG
		SET I_U_MERGE = @ErrorMsg + CAST(@ErrorNo AS VARCHAR(5))
		WHERE L_DATE = CAST(GETDATE() AS DATE) AND TABLE_NAME = @Main_Table;
	END CATCH;

	--4
	BEGIN TRY
		EXEC dbo.usp_Delete_Indexes @Tmp_Table ;

		UPDATE SWITCH_MERGE_LOG
		SET IX_DROP = 'Indexes were sucessfully dropped from tmp table'
		WHERE L_DATE = CAST(GETDATE() AS DATE) AND TABLE_NAME = @Main_Table;
	END TRY
	BEGIN CATCH
		SET @ErrorMsg = ERROR_MESSAGE();
		SET @ErrorNo = ERROR_NUMBER();

		UPDATE SWITCH_MERGE_LOG
		SET IX_DROP = @ErrorMsg + CAST(@ErrorNo AS VARCHAR(5))
		WHERE L_DATE = CAST(GETDATE() AS DATE) AND TABLE_NAME = @Main_Table;
	END CATCH;

GO


