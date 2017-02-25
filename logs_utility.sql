-- check logs, ldf, virtual logs
DBCC SQLPERF(logspace)

-- virtual logs inside transactional logs
DBCC LOGINFO

-- backup to null
BACKUP LOG DBUtil WITH NO_LOG


-- check open transactions
DBCC OPENTRAN

-- Store logs data
CREATE PROC dbo.spSQLPerf 
AS 
DBCC SQLPERF(logspace) 
GO 
 

CREATE TABLE dbo.logSpaceStats 
( 
id INT IDENTITY (1,1), 
logDate datetime DEFAULT GETDATE(), 
databaseName sysname, 
logSize decimal(18,5), 
logUsed decimal(18,5) 
) 
GO 
 


CREATE PROC dbo.spGetSQLPerfStats 
AS 
SET NOCOUNT ON 

CREATE TABLE #tFileList 
( 
databaseName sysname, 
logSize decimal(18,5), 
logUsed decimal(18,5), 
status INT 
) 

INSERT INTO #tFileList 
       EXEC spSQLPerf 

INSERT INTO logSpaceStats (databaseName, logSize, logUsed) 
SELECT databasename, logSize, logUsed 
FROM #tFileList 

DROP TABLE #tFileList 
GO

EXEC sp_getSQLPerfStats
GO

SELECT * FROM dbo.logSpaceStats
GO
