// Select the list of tables for which CDC is enabled. Important for the CDC table name
pub const SELECT_CDC_ENABLED_TABLES: &str = "
SELECT OBJECT_NAME(ct.object_id, DB_ID('#db')) as table_name, c.object_id, ct.capture_instance, ct.start_lsn, c.columns FROM [#db].[cdc].[change_tables] as ct JOIN (SELECT object_id, STRING_AGG(column_name, ',') as columns FROM [#db].[cdc].[captured_columns] GROUP BY object_id) as c ON c.object_id = ct.object_id;";
pub const SELECT_MIN_LSN: &str = "SELECT [#db].sys.fn_cdc_get_min_lsn('#') as lsn;";
pub const SELECT_MAX_LSN: &str = "SELECT [#db].sys.fn_cdc_get_max_lsn() as lsn;";
/*
pub const SELECT_ALL_CHANGES_FROM_TO: &str = r"
SELECT
	[__$start_lsn],
	[__$command_id],
	[__$seqval],
	[__$operation],
	[__$update_mask],
	[#columns],
	TODATETIMEOFFSET([#db].sys.fn_cdc_map_lsn_to_time([__$start_lsn]),
	DATEPART(TZOFFSET, SYSDATETIMEOFFSET()))
FROM [#db].cdc.[#table]
WHERE [__$start_lsn] > [LSN1]
AND [__$start_lsn] BETWEEN [LSN2] AND [LSN3]
ORDER BY [__$start_lsn] ASC, [__$command_id] ASC, [__$operation] ASC;";
*/
pub const SELECT_CAPTURED_COLUMNS: &str = r"
SELECT object_id, column_name
FROM [#db].cdc.captured_columns
ORDER BY object_id, column_id";

pub const DATABASE_PLACEHOLDER: &str = "\\[#db\\]";
pub const DATABASE_PARAMETER: &str = "#db";
pub const TABLE_PLACEHOLDER: &str = "\\[#table\\]";
pub const COLUMNS_PLACEHOLDER: &str = "\\[#columns\\]";
pub const LSN1: &str = "\\[LSN1\\]";
pub const LSN2: &str = "\\[LSN2\\]";
pub const LSN3: &str = "\\[LSN3\\]";

pub fn to_parameter(placeholder: &String) -> String {
	format!("[{}]", placeholder)
}



pub const SELECT_ALL_CHANGES_FROM_TO: &str = r"
SELECT TODATETIMEOFFSET(
		[#db].sys.fn_cdc_map_lsn_to_time([__$start_lsn]),
		DATEPART(TZOFFSET, SYSDATETIMEOFFSET())
	) as event_timestamp,
	[__$start_lsn]
    ,[__$end_lsn]
    ,[__$seqval]
    ,[__$update_mask]
	,min([__$operation]) as [__$operation]
	,[__$command_id]
    ,(
		SELECT
			[#columns]
		FROM [#db].[cdc].[#table] t_json
		WHERE t.[__$start_lsn] = t_json.[__$start_lsn] AND t.[__$update_mask] = t_json.[__$update_mask] AND t.[__$command_id]= t_json.[__$command_id]
		ORDER BY t_json.[__$command_id] ASC
		FOR JSON AUTO
	) as data
FROM [#db].cdc.[#table] t
WHERE [__$start_lsn] > [LSN1]
AND [__$start_lsn] BETWEEN [LSN2] AND [LSN3]
GROUP BY [__$start_lsn], [__$command_id], [__$seqval], [__$end_lsn], [__$update_mask]
ORDER BY [__$start_lsn] ASC, [__$command_id] ASC;";