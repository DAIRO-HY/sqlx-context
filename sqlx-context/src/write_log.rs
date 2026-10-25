use serde_json::Value;
use sqlx::encode::IsNull;
use sqlx::sqlite::SqliteArgumentValue;
use sqlx::{Encode, Sqlite, Type};

/// 一条成功执行的写SQL记录:实际执行的SQL文本(占位符为 `?`) + 按顺序绑定的参数。
#[derive(Debug, Clone, PartialEq)]
pub struct WriteLog {
    /// 实际执行的SQL文本,占位符为 `?`
    pub sql: String,

    /// 按绑定顺序排列的参数
    pub params: Vec<Value>,
}

impl WriteLog {
    /// 参数序列化成紧凑的JSON数组字符串,如 `[1,"a",null]`
    pub fn params_json(&self) -> String {
        serde_json::to_string(&self.params).unwrap()
    }
}

/// 把一个可以绑定到SQLite的值转换成写日志用的JSON值。
///
/// 接受的类型和 `sqlx::Arguments::add` 一样(任何实现了 `Encode<Sqlite> + Type<Sqlite>` 的类型),
/// 直接复用sqlx自己的编码结果,所以和真正绑定到SQL里的值保持一致:
/// - 整数、浮点数 -> JSON number
/// - 字符串(以及chrono等最终编码成TEXT的类型) -> JSON string
/// - `None` -> `null`
/// - `bool` -> `true` / `false`
/// - `Vec<u8>` 等BLOB -> 标准base64字符串(带 `=` 填充)
pub fn log_value<'q, T>(value: T) -> Value
where
    T: Encode<'q, Sqlite> + Type<Sqlite>,
{
    let is_bool = T::type_info() == <bool as Type<Sqlite>>::type_info();
    let mut buf: Vec<SqliteArgumentValue<'q>> = Vec::with_capacity(1);
    match value.encode(&mut buf) {
        Ok(IsNull::No) => {}
        // 编码失败时真正的绑定也会失败,这里不会被记录;按null处理即可
        Ok(IsNull::Yes) | Err(_) => return Value::Null,
    }
    match buf.pop() {
        None | Some(SqliteArgumentValue::Null) => Value::Null,
        Some(SqliteArgumentValue::Int(v)) if is_bool => Value::Bool(v != 0),
        Some(SqliteArgumentValue::Int64(v)) if is_bool => Value::Bool(v != 0),
        Some(SqliteArgumentValue::Int(v)) => Value::from(v),
        Some(SqliteArgumentValue::Int64(v)) => Value::from(v),
        Some(SqliteArgumentValue::Double(v)) => Value::from(v), // NaN/Infinity 会变成 null
        Some(SqliteArgumentValue::Text(v)) => Value::String(v.into_owned()),
        Some(SqliteArgumentValue::Blob(v)) => Value::String(base64_encode(&v)),
    }
}

/// 标准base64编码(RFC 4648,带填充),与 Go 的 `json.Marshal([]byte)` 输出一致
fn base64_encode(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32;
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { TABLE[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { TABLE[n as usize & 63] as char } else { '=' });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn log_value_matches_go_json_marshal() {
        assert_eq!(log_value(&1i64), json!(1));
        assert_eq!(log_value(&-7i32), json!(-7));
        assert_eq!(log_value(&1.5f64), json!(1.5));
        assert_eq!(log_value(&"abc"), json!("abc"));
        assert_eq!(log_value(&"abc".to_string()), json!("abc"));
        assert_eq!(log_value(&true), json!(true));
        assert_eq!(log_value(&Some(false)), json!(false));
        assert_eq!(log_value(&None::<i64>), json!(null));
        assert_eq!(log_value(&None::<String>), json!(null));
        assert_eq!(log_value(&Some(3i64)), json!(3));
        assert_eq!(log_value(&vec![0u8, 1, 2, 255]), json!("AAEC/w=="));
    }

    #[test]
    fn base64_matches_std_encoding() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn params_json_is_compact() {
        let log = WriteLog {
            sql: "insert into t (a, b) values (?, ?)".into(),
            params: vec![json!(1), json!(null)],
        };
        assert_eq!(log.params_json(), "[1,null]");
    }
}
