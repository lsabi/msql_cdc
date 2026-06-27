// use chrono::NaiveDateTime;
use tiberius::{ColumnType, Row, Uuid};


pub fn sql_row_to_json(row: &Row) -> String {
	let mut res = Vec::new();

	for column in row.columns() {
		let column_name = column.name();
		let column_type = column.column_type();

		let value: String = match column_type {
			ColumnType::Bit => match row
				.get::<bool, _>(column_name) {
					Some(v) => format!("{:?}", v),
					None => "null".to_string()
				},
			ColumnType::Int1 => match row
				.get::<u8, _>(column_name) {
					Some(v) => format!("{:?}", v),
					None => "null".to_string()
				},
			ColumnType::Int2 => match row
				.get::<i16, _>(column_name) {
					Some(v) => format!("{:?}", v),
					None => "null".to_string()
				},
			ColumnType::Int4 => match row
				.get::<i32, _>(column_name) {
					Some(v) => format!("{:?}", v),
					None => "null".to_string()
				},
			ColumnType::Intn => match row
				.get::<i64, _>(column_name) {
					Some(v) => format!("{:?}", v),
					None => "null".to_string()
				},
			/*
			ColumnType::Intn => row
				.try_get::<i64, _>(column_name)
				.or_else(|_| {
					row.try_get::<i32, _>(column_name)
						.map(|v| v.map(|i| i as i64))
				})
				.or_else(|_| {
					row.try_get::<i16, _>(column_name)
						.map(|v| v.map(|i| i as i64))
				})
				.ok()
				.flatten()
				.map(|v| Value::Number(v.into()))
				.or(Some(Value::Number(0.into()))),
			*/
			ColumnType::Int8 => match row
				.get::<i64, _>(column_name)  {
					Some(v) => format!("{:?}", v),
					None => "null".to_string()
				},
			ColumnType::Float4 => match row
				.get::<f32, _>(column_name) {
					Some(v) => format!("{:?}", v),
					None => "null".to_string()
				},
			ColumnType::Float8 | ColumnType::Floatn => match row
				.get::<f64, _>(column_name) {
					Some(v) => format!("{:?}", v),
					None => "null".to_string()
				},
			ColumnType::Guid => match row
				.get::<Uuid, _>(column_name) {
					Some(v) => format!("\"{:?}\"", v),
					None => "null".to_string()
				},
			ColumnType::NVarchar
			| ColumnType::NChar
			| ColumnType::BigVarChar
			| ColumnType::BigChar
			| ColumnType::Text
			| ColumnType::NText => match row
				.get::<&str, _>(column_name) {
					Some(v) => format!("{:?}", v),
					None => "null".to_string()
				},
			/*
			ColumnType::Numericn | ColumnType::Decimaln => match row
				.get::<Decimal, _>(column_name) {
					Some(v) => format!("{:?}", v),
					None => "null".to_string()
				},
			*/
			/*
			ColumnType::Datetime
			| ColumnType::Datetime2
			| ColumnType::DatetimeOffsetn
			| ColumnType::Datetime4
			| ColumnType::Daten
			| ColumnType::Timen
			| ColumnType::Datetimen => match row
				.get::<NaiveDateTime, _>(column_name) {
					Some(v) => format!("{:?}", v),
					None => "null".to_string()
				},
			*/
			ColumnType::Null => "null".to_string(),
			ColumnType::Money | ColumnType::Money4 => match row
				.get::<f64, _>(column_name) {
					Some(v) => format!("{:?}", v),
					None => "null".to_string()
				},
			ColumnType::Bitn => match row
				.get::<bool, _>(column_name) {
					Some(v) => format!("{:?}", v),
					None => "null".to_string()
				},
			ColumnType::BigVarBin | ColumnType::BigBinary | ColumnType::Image => match row
				.get::<&[u8], _>(column_name) {
					Some(v) => format!("{:?}", v),
					None => "null".to_string()
				},
			ColumnType::Xml | ColumnType::Udt => match row
				.get::<&str, _>(column_name) {
					Some(v) => format!("\"{:?}\"", v),
					None => "null".to_string()
				},
			ColumnType::SSVariant => "null".to_string(),
			_ => "null".to_string()
		};
		if !column_name.is_empty() {
			res.push(format!("\"{}\": {}", column_name, value));
		}
	}
	format!("{}", res.join(", "))
}
