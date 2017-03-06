
--# 1 ================================================= PF, PS : GET MIN OF AS_OF_DATE
CREATE PARTITION FUNCTION pf_date_daily (DATE)
AS RANGE RIGHT FOR VALUES ('2005-01-01')
GO


CREATE PARTITION SCHEME ps_date_daily
AS PARTITION pf_date_daily ALL TO ([PRIMARY])
GO



--# 2 ================================================= CREATE NEW PARTIONES
-- This SP will created all required partitioin before going live for the following partitioin function (daily partitioin function)
CREATE PROCEDURE usp_New_Partition_Pre_GoLive
	@psName SYSNAME,
	@pfName SYSNAME,
	@targetDate DATE 
AS
	Declare @sqlcmd VARCHAR(4000)
	Declare @lastValue DATE
	Declare @nextDay DATE
	Declare @nextMonth DATE
	Declare @nextYear DATE
	Declare @today Date
	Declare @count INT
BEGIN
	-- Get the last values 
	SET @lastValue = cast((SELECT  TOP 1 [value] FROM [sys].[partition_range_values]
		WHERE function_id = (SELECT function_id FROM [sys].[partition_functions] WHERE name = @pfName)
		ORDER BY boundary_id DESC) AS DATE);


	-- get today's date
	SET @today = getdate()

	SET @count=0
	
	IF @pfName = 'pf_date_daily' AND @psName = 'ps_date_daily'
	BEGIN
	While @lastValue < = @targetDate
		BEGIN
			SET @nextDay = dateadd(day, 1, @lastValue)

				--Add a new partition
				SET @sqlcmd = 'Alter Partition Scheme ' + @psName + ' next used [PRIMARY]'
				EXEC (@sqlcmd)

				--Split the range
				SET @sqlcmd = 'Alter Partition Function ' + @pfName + '() split range ('''+ convert(varchar(10), @nextDay)+''')'
				EXEC (@sqlcmd)

				-- Reset the last partition
				SET @lastValue=@nextDay

				-- set count
					SET @count=@count+1
			END
	print convert( varchar(5), @count) + ' new partition has been created'
	END
END


--# 3 ================================================= EXECUTE SP MAUALLY FOR ONE TOME
EXEC usp_New_Partition_Pre_GoLive 'ps_date_daily', 'pf_date_daily', '2019-01-01'
GO

--# 4  ================================================= DROP SP 
DROP PROCEDURE [dbo].[usp_New_Partition_Pre_GoLive]
GO
