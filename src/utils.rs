use regex::Regex;
use crate::connection::ResultSet;
use crate::state::LSN;

pub fn replace_all_instances(r: &str, original_string: &str, replacement: &str) -> String {
	Regex::new(r)
		.unwrap()
		.replace_all(original_string, String::from(replacement))
		.to_string()
}

/// Convert the ResultSet into the max LSN
/// Used for internal conversion for getting the max LSN
pub fn lsn_from_resultset(rows: ResultSet) -> Result<LSN, String> {
	let val: &[u8] = rows.data()[0].get(0).unwrap();
	let val: [u8; 10] = val.try_into().unwrap();
	Ok(val)
}
