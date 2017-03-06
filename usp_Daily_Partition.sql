-- it will creat partitioins unitl the end of next month
CREATE PROCEDURE [dbo].[usp_Daily_Partition]
AS
	DECLARE	@psName					SYSNAME
	DECLARE @pfName					SYSNAME
	DECLARE @sqlcmd					VARCHAR(4000)
	DECLARE @lastValue				DATE
	DECLARE @nextDay				DATE
	DECLARE	@firstDayOfNextMonth	DATE
	DECLARE @lastDayOfNextMonth		DATE
	DECLARE @today					DATE
	DECLARE @count					INT
	
BEGIN
	-- Get the last values 
	SET @lastValue = cast((SELECT  TOP 1 [value] FROM [sys].[partition_range_values]
		WHERE function_id = (SELECT function_id FROM [sys].[partition_functions] WHERE name = 'pf_date_daily')
		ORDER BY boundary_id DESC) AS DATE);

	-- get today's date
	SET @today = getdate()

	-- get the first day of next month after @today
	SET @firstDayOfNextMonth =  CONVERT(VARCHAR(10),DATEADD(MONTH, DATEDIFF(MONTH,0,@today)+1,0),120) --****

	-- get the last day of the next month after @today
	SET @lastDayOfNextMonth = CONVERT(VARCHAR(10),dateadd(d,-(day(dateadd(m,1,@firstDayOfNextMonth))),dateadd(m,1,@firstDayOfNextMonth)),120)
	
	-- set partition f, p
	SET @pfName = 'pf_date_daily'
	SET @psName = 'ps_date_daily'

	SET @count=0

	-- Daily partition ------------------------------------------------------------------------------------------------------
	--------------------------------------------------------------------------------------------------------------------------
	BEGIN
	WHILE @lastValue < @lastDayOfNextMonth
		BEGIN
			SET @nextDay = dateadd(day, 1, @lastValue)

				--Add a new partition
				SET @sqlcmd = 'Alter Partition Scheme ' + @psName + ' next used [PRIMARY]'
				EXEC (@sqlcmd)

				--Split the range
				SET @sqlcmd = 'Alter Partition Function ' + @pfName + '() split range ('''+ convert(VARCHAR(10), @nextDay)+''')'
				EXEC (@sqlcmd)

				-- Reset the last partition
				SET @lastValue=@nextDay

				-- set count
				SET @count=@count+1
			END
	PRINT convert(VARCHAR(5), @count) + ' new partition has been created'
	END
	-- ====================================================================================================================

END
