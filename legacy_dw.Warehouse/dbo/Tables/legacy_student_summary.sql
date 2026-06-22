CREATE TABLE [dbo].[legacy_student_summary] (

	[id_student] varchar(8000) NULL, 
	[gender] varchar(8000) NULL, 
	[region] varchar(8000) NULL, 
	[highest_education] varchar(8000) NULL, 
	[age_band] varchar(8000) NULL, 
	[imd_band] varchar(8000) NULL, 
	[date_registration] int NULL, 
	[is_withdrawn] int NOT NULL, 
	[avg_score] float NULL, 
	[total_clicks] int NULL
);