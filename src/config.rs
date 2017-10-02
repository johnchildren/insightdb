#[derive(Debug, Deserialize)]
pub struct DbConfig {
    pub name: String,
    pub tables: Vec<TableConfig>,
}

#[derive(Debug, Deserialize)]
pub struct TableConfig {
    pub name: String,
    pub columns: Vec<ColumnConfig>,
    pub size: usize,
}

#[derive(Debug, Deserialize)]
pub struct ColumnConfig {
    pub name: String,
    pub col_type: String,
}