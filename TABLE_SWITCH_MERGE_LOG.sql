CREATE TABLE [dbo].[SWITCH_MERGE_LOG](
	[L_DATE] [date] NOT NULL,
	[TABLE_NAME] [varchar](128) NOT NULL,
	[IX_CREATION] [varchar](max) NULL,
	[P_SWITCHING] [varchar](max) NULL,
	[I_U_MERGE] [varchar](max) NULL,
	[IX_DROP] [varchar](max) NULL,
 CONSTRAINT [PK_SWITCH_MERGE_LOG] PRIMARY KEY CLUSTERED 
(
	[L_DATE] ASC,
	[TABLE_NAME] ASC
)
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO



