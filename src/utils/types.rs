#[derive(Debug, Clone)]
pub enum Value {
    None,
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
}
impl Value {
    pub fn as_str(&self) -> Result<&str, DbError> {
        match self {
            Value::Text(v) => Ok(v.as_str()),
            _ => Err(DbError::InvalidType),
        }
    }

    pub fn as_i64(&self) -> Result<i64, DbError> {
        match self {
            Value::Int(v) => Ok(*v),
            _ => Err(DbError::InvalidType),
        }
    }

    pub fn as_f64(&self) -> Result<f64, DbError> {
        match self {
            Value::Float(v) => Ok(*v),
            _ => Err(DbError::InvalidType),
        }
    }

    pub fn as_bool(&self) -> Result<bool, DbError> {
        match self {
            Value::Bool(v) => Ok(*v),
            _ => Err(DbError::InvalidType),
        }
    }

    pub fn to_string_value(&self) -> String {
        match self {
            Value::Text(v) => v.clone(),
            Value::Int(v) => v.to_string(),
            Value::Float(v) => v.to_string(),
            Value::Bool(v) => {
                if *v {
                    "on".to_string()
                } else {
                    "off".to_string()
                }
            }
            Value::None => "".to_string(),
        }
    }
    pub fn to_int(&self) -> Result<i64, DbError> {
        match self {
            Value::Int(v) => Ok(*v),

            Value::Float(v) => Ok(*v as i64),

            Value::Bool(v) => {
                if *v {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }

            Value::Text(v) => v.parse::<i64>().map_err(|_| DbError::InvalidType),

            Value::None => Err(DbError::InvalidType),
        }
    }
    pub fn to_float(&self) -> Result<f64, DbError> {
        match self {
            Value::Float(v) => Ok(*v),

            Value::Int(v) => Ok(*v as f64),

            Value::Bool(v) => {
                if *v {
                    Ok(1.0)
                } else {
                    Ok(0.0)
                }
            }

            Value::Text(v) => v.parse::<f64>().map_err(|_| DbError::InvalidType),

            Value::None => Err(DbError::InvalidType),
        }
    }
    pub fn to_bool(&self) -> Result<bool, DbError> {
        match self {
            Value::Bool(v) => Ok(*v),

            Value::Int(v) => Ok(*v != 0),

            Value::Float(v) => Ok(*v != 0.0),

            Value::Text(v) => match v.to_lowercase().as_str() {
                "true" | "on" | "1" | "yes" => Ok(true),
                "false" | "off" | "0" | "no" => Ok(false),
                _ => Err(DbError::InvalidType),
            },

            Value::None => Err(DbError::InvalidType),
        }
    }

    pub fn as_str_or<'a>(&'a self, default: &'a str) -> &'a str {
        match self {
            Value::Text(v) => v.as_str(),
            _ => default,
        }
    }

    pub fn as_i64_or(&self, default: i64) -> i64 {
        match self {
            Value::Int(v) => *v,
            _ => default,
        }
    }

    pub fn as_f64_or(&self, default: f64) -> f64 {
        match self {
            Value::Float(v) => *v,
            _ => default,
        }
    }

    pub fn as_bool_or(&self, default: bool) -> bool {
        match self {
            Value::Bool(v) => *v,
            _ => default,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string_value())
    }
}

#[derive(Debug)]
pub enum DbError {
    Sqlite(sqlite::Error),
    NotFound,
    InvalidType,
    DbInitialised,
}
impl From<sqlite::Error> for DbError {
    fn from(err: sqlite::Error) -> Self {
        DbError::Sqlite(err)
    }
}
