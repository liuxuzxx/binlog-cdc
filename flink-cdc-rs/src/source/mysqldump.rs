use std::{
    collections::HashMap,
    fmt::Display,
    fs::File,
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
};

use base64::{Engine, engine::general_purpose};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde_json::{Map, Value, json};
use sqlparser::{
    ast::{CreateTable, DataType, ObjectNamePart, Statement},
    dialect::MySqlDialect,
    parser::Parser as SqlParser,
};
use tracing::{debug, info, warn};
use walkdir::WalkDir;
use zip::ZipArchive;

use crate::{
    config::{
        CdcConfig,
        source::{Mysqldump, Source},
    },
    pipeline::formatter::{DebeziumFormat, MessageKey},
    pipeline::message::PipelineRecord,
};

///
/// mysqldump zip/file as source.
///
pub struct MysqldumpSource<'a> {
    config: &'a CdcConfig,
    channels: Vec<crossbeam_channel::Sender<PipelineRecord>>,
    table_cache: HashMap<String, Table>,
}

type SqlLine = Vec<u8>;

impl<'a> MysqldumpSource<'a> {
    pub fn create(
        cdc: &'a CdcConfig,
        channels: Vec<crossbeam_channel::Sender<PipelineRecord>>,
    ) -> Self {
        Self {
            config: cdc,
            channels,
            table_cache: HashMap::new(),
        }
    }

    pub async fn read(&mut self) {
        let source_config = self.source_config();
        for path in self.walk_sql_files(source_config.filepath()) {
            match path.extension().and_then(|ext| ext.to_str()) {
                Some("zip") => {
                    if let Err(err) = self.read_zip_file(&path) {
                        warn!(error = %err, "read zip file:{} failed!", path.display());
                    }
                }
                Some("sql") => {
                    if let Err(err) = self.read_sql_file(&path) {
                        warn!(error = %err, "read sql file:{} failed!", path.display());
                    }
                }
                _ => {
                    warn!("unknown file:{} type!", path.display());
                }
            }
        }
    }

    fn source_config(&self) -> &Mysqldump {
        match self.config.source() {
            Source::MysqlDump(source) => source,
            _ => panic!("mysqldump source need mysqldump config"),
        }
    }

    fn walk_sql_files(&self, filepath: &str) -> Vec<PathBuf> {
        let path = Path::new(filepath);
        if path.is_file() {
            return vec![path.to_path_buf()];
        }

        WalkDir::new(path)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file())
            .filter(|entry| {
                matches!(
                    entry.path().extension().and_then(|ext| ext.to_str()),
                    Some("zip" | "sql")
                )
            })
            .map(|entry| entry.into_path())
            .collect()
    }

    fn read_sql_file(&mut self, path: &Path) -> Result<(), String> {
        info!("start read sql file:{}", path.display());
        let file = File::open(path).map_err(|err| format!("open sql file failed:{err}"))?;
        let reader = BufReader::new(file);
        self.process_sql_file(reader);
        Ok(())
    }

    fn read_zip_file(&mut self, path: &Path) -> Result<(), String> {
        info!("start read zip file:{}", path.display());
        let file = File::open(path).map_err(|err| format!("open zip file failed:{err}"))?;
        let mut reader =
            ZipArchive::new(file).map_err(|err| format!("process zip file failed:{err}"))?;

        let filenames = reader
            .file_names()
            .map(|filename| filename.to_string())
            .collect::<Vec<String>>();

        for filename in filenames {
            let path = Path::new(&filename);
            if path.is_dir() {
                info!("skip dir:{}", filename);
                continue;
            }

            info!("start read  sql file:{} in zip!", filename);
            let mut file_reader = reader
                .by_name(filename.as_str())
                .map_err(|err| format!("unzip zip file failed:{err}"))?;
            let reader = BufReader::new(&mut file_reader);
            self.process_sql_file(reader);
        }

        Ok(())
    }

    fn process_sql_file<R: Read>(&mut self, mut reader: BufReader<R>) {
        let mut line_bytes = Vec::new();
        let mut sql_line: SqlLine = Vec::new();
        let mut sql_buffer: Vec<SqlLine> = Vec::new();
        let capacity = self
            .config
            .pipeline()
            .map(|pipeline| pipeline.capacity() as usize)
            .unwrap_or(2000);

        while reader.read_until(b'\n', &mut line_bytes).unwrap_or(0) > 0 {
            if line_bytes.len() <= 2 {
                debug!("empty line, discard");
                line_bytes.clear();
                continue;
            }

            if (line_bytes[0] == b'/' && line_bytes[1] == b'*')
                || (line_bytes[0] == b'-' && line_bytes[1] == b'-')
            {
                debug!(
                    "comment line, discard:{}",
                    String::from_utf8_lossy(&line_bytes)
                );
                line_bytes.clear();
                continue;
            }

            sql_line.append(&mut line_bytes);

            if sql_line.ends_with(&[b';', b'\n']) || sql_line.ends_with(&[b';']) {
                debug!(
                    "found actual SQL statement:{}",
                    String::from_utf8_lossy(&sql_line[0..sql_line.len().min(10)])
                );
                sql_buffer.push(sql_line);

                if sql_buffer.len() >= capacity {
                    self.parallel_parse(sql_buffer);
                    sql_buffer = Vec::new();
                }

                sql_line = Vec::new();
            }
        }

        if !sql_buffer.is_empty() {
            self.parallel_parse(sql_buffer);
        }
    }

    fn parallel_parse(&mut self, sql_buffer: Vec<SqlLine>) {
        let mut diversion = sql_buffer
            .into_iter()
            .fold(HashMap::new(), |mut map, sql_line| {
                let sql_type = classify_sql(&sql_line);
                map.entry(sql_type).or_insert_with(Vec::new).push(sql_line);
                map
            });

        if let Some(sql_lines) = diversion.remove(&SqlType::CreateTable) {
            let dialect = MySqlDialect {};
            sql_lines.into_iter().for_each(|line| {
                let line = String::from_utf8(line).expect("Create Table语句不是UTF-8");
                let statement = SqlParser::new(&dialect)
                    .try_with_sql(&line)
                    .expect("解析Create table语句失败")
                    .parse_statement()
                    .expect("生成SQL的Statement失败");

                if let Statement::CreateTable(event) = statement {
                    self.parse_create_table(&event);
                } else {
                    warn!("unexpected statement, not a Create Table statement");
                }
            });
        }

        if let Some(sql_lines) = diversion.remove(&SqlType::Insert) {
            let insert_count = sql_lines.len();
            let total_bytes = sql_lines.iter().map(|line| line.len()).sum::<usize>();
            info!(
                "start parsing insert statements in parallel, count={}, total_size={} bytes",
                insert_count, total_bytes
            );

            sql_lines
                .into_par_iter()
                .filter_map(|line| self.parse_insert(line))
                .for_each(|debeziums| {
                    let rows = debeziums.len();
                    info!("start sending Debezium batch to channel, rows={}", rows);

                    debeziums
                        .into_iter()
                        .enumerate()
                        .for_each(|(index, debezium)| {
                            if index == 0 {
                                info!("sending first message of Debezium batch to channel");
                            }
                            self.send_debezium(debezium);
                            let sent = index + 1;
                            if sent % 1000 == 0 || sent == rows {
                                info!("Debezium batch send progress, sent={}, rows={}", sent, rows);
                            }
                        });

                    info!("Debezium batch sent, rows={}", rows);
                });

            info!(
                "insert statements parsed and sent, count={}, total_size={} bytes",
                insert_count, total_bytes
            );
        }
    }

    fn parse_create_table(&mut self, event: &CreateTable) {
        let name = &event.name.0.get(0).expect("Create Table语句缺少表名");
        match name {
            ObjectNamePart::Identifier(ident) => {
                info!("table name:{}", ident.value);
                let columns = self.parse_columns(event);
                let table = Table::new(ident.value.clone(), columns);
                self.table_cache.insert(ident.value.clone(), table);
            }
            _ => warn!("ignore unsupported Create Table name format"),
        }
    }

    fn parse_columns(&self, event: &CreateTable) -> Vec<Column> {
        event
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Column::new(index, column.name.value.clone(), column.data_type.clone())
            })
            .collect()
    }

    fn parse_insert(&self, sql: Vec<u8>) -> Option<Vec<DebeziumFormat>> {
        info!("start parsing insert statement, size={} bytes", sql.len());
        let insert = parse_insert_sql(&sql).expect("解析Insert into语句失败");
        info!(
            "insert statement parsed, table={}, rows={}",
            insert.table_name(),
            insert.rows.len()
        );

        if let Some(table) = self.table_cache.get(insert.table_name()) {
            let values = insert.row_values();
            info!(
                "start building Debezium data, table={}, rows={}",
                table.name(),
                values.len()
            );
            let debezium = table.build_debezium(values);
            info!(
                "Debezium data built, table={}, rows={}",
                table.name(),
                debezium.len()
            );
            Some(debezium)
        } else {
            warn!(
                "table exists in sql dump file, but corresponding fields cannot be found:{}!",
                insert.table_name()
            );
            None
        }
    }

    fn send_debezium(&self, debezium: DebeziumFormat) {
        let key = debezium.keys();
        let table = debezium.source_table().unwrap_or_default().to_string();
        let record = PipelineRecord::create_mysqldump(debezium, String::from(""), table);
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let index = hasher.finish() as usize % self.channels.len();
        if let Err(err) = self.channels[index].send(record) {
            warn!(
                "failed to send mysqldump Debezium data to channel:{:?}",
                err
            );
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Eq, Hash)]
enum SqlType {
    Insert,
    CreateTable,
    DropTable,
    LockTables,
    UnlockTables,
    Other,
}

fn classify_sql(sql: &[u8]) -> SqlType {
    let start = sql
        .iter()
        .position(|&char| !char.is_ascii_whitespace())
        .unwrap_or(0);

    if start >= sql.len() {
        return SqlType::Other;
    }

    let sql_upper = sql[start..]
        .iter()
        .map(|char| char.to_ascii_uppercase())
        .collect::<Vec<u8>>();

    if sql_upper.starts_with(b"INSERT INTO") {
        return SqlType::Insert;
    }

    if sql_upper.starts_with(b"CREATE")
        && sql_upper.contains(&b'T')
        && sql_upper.contains(&b'A')
        && sql_upper.contains(&b'B')
        && sql_upper.contains(&b'L')
        && sql_upper.contains(&b'E')
    {
        return SqlType::CreateTable;
    }

    if sql_upper.starts_with(b"DROP")
        && sql_upper.contains(&b'T')
        && sql_upper.contains(&b'A')
        && sql_upper.contains(&b'B')
        && sql_upper.contains(&b'L')
        && sql_upper.contains(&b'E')
    {
        return SqlType::DropTable;
    }

    if sql_upper.starts_with(b"LOCK")
        && sql_upper.contains(&b'T')
        && sql_upper.contains(&b'A')
        && sql_upper.contains(&b'B')
        && sql_upper.contains(&b'L')
        && sql_upper.contains(&b'E')
    {
        return SqlType::LockTables;
    }

    if sql_upper.starts_with(b"UNLOCK")
        && sql_upper.contains(&b'T')
        && sql_upper.contains(&b'A')
        && sql_upper.contains(&b'B')
        && sql_upper.contains(&b'L')
        && sql_upper.contains(&b'E')
    {
        return SqlType::UnlockTables;
    }

    SqlType::Other
}

#[derive(Debug)]
pub struct Table {
    name: String,
    columns: Vec<Column>,
}

impl Table {
    pub fn new(name: String, mut columns: Vec<Column>) -> Self {
        columns.sort_by_key(|column| column.index);
        Self { name, columns }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn columns_by_index(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    fn build_debezium(&self, rows: Vec<Vec<Value>>) -> Vec<DebeziumFormat> {
        rows.into_iter()
            .map(|row| self.single_row_debezium(row))
            .collect()
    }

    fn single_row_debezium(&self, row: Vec<Value>) -> DebeziumFormat {
        let mut map = Map::with_capacity(row.len());

        for (index, data) in row.into_iter().enumerate() {
            if let Some(column) = self.columns_by_index(index) {
                map.insert(column.name().to_string(), data);
            }
        }

        DebeziumFormat::insert(
            json!(map),
            "",
            self.name(),
            MessageKey::new(self.create_key(&map)),
        )
    }

    fn create_key(&self, row: &Map<String, Value>) -> Map<String, Value> {
        let mut key = Map::with_capacity(2);
        if let Some(column) = self.columns.first() {
            if let Some(primary) = row.get(column.name()) {
                key.insert(column.name().to_string(), primary.clone());
            }
        }
        key.insert("TableId".to_string(), json!(self.name()));
        key
    }
}

#[derive(Debug)]
pub struct Column {
    index: usize,
    name: String,
    data_type: DataType,
}

impl Column {
    pub fn new(index: usize, name: String, data_type: DataType) -> Self {
        Column {
            index,
            name,
            data_type,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Column {{ index: {}, name: {}, data_type: {:?} }}",
            self.index, self.name, self.data_type
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Insert,
    Into,
    Values,
    Identifier(Vec<u8>),
    StringLiteral(Vec<u8>),
    Number(Vec<u8>),
    Null,
    LeftParen,
    RightParen,
    Comma,
    Semicolon,
    Backtick,
    Dot,
    BinaryPrefix,
    EOF,
}

struct InsertLexer {
    input: Vec<u8>,
    pos: usize,
}

impl InsertLexer {
    fn new(input: Vec<u8>) -> Self {
        InsertLexer { input, pos: 0 }
    }

    fn tokenize(&mut self) -> Result<Vec<Token>, String> {
        let mut tokens = Vec::new();

        while self.pos < self.input.len() {
            self.skip_whitespace();

            if self.pos >= self.input.len() {
                break;
            }

            let char = self.input[self.pos];
            match char {
                b'(' => {
                    tokens.push(Token::LeftParen);
                    self.pos += 1;
                }
                b')' => {
                    tokens.push(Token::RightParen);
                    self.pos += 1;
                }
                b',' => {
                    tokens.push(Token::Comma);
                    self.pos += 1;
                }
                b';' => {
                    tokens.push(Token::Semicolon);
                    self.pos += 1;
                }
                b'`' => {
                    tokens.push(Token::Backtick);
                    self.pos += 1;
                }
                b'.' => {
                    tokens.push(Token::Dot);
                    self.pos += 1;
                }
                b'\'' => {
                    let value = self.read_string_literal()?;
                    tokens.push(Token::StringLiteral(value));
                }
                b'0'..=b'9' | b'-' | b'+' => {
                    let number = self.read_number();
                    tokens.push(Token::Number(number));
                }
                b'A'..=b'Z' | b'a'..=b'z' | b'_' => {
                    let ident = self.read_identifier();
                    tokens.push(self.match_keyword(&ident));
                }
                _ => {
                    return Err(format!(
                        "Unexpected character at position {}: 0x{:02x}",
                        self.pos, char
                    ));
                }
            }
        }

        tokens.push(Token::EOF);
        Ok(tokens)
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() {
            let char = self.input[self.pos];
            if char == b' ' || char == b'\t' || char == b'\n' || char == b'\r' {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn read_string_literal(&mut self) -> Result<Vec<u8>, String> {
        self.pos += 1;
        let mut result = Vec::new();

        while self.pos < self.input.len() {
            let char = self.input[self.pos];

            if char == b'\'' {
                self.pos += 1;
                if self.pos < self.input.len() && self.input[self.pos] == b'\'' {
                    result.push(b'\'');
                    self.pos += 1;
                } else {
                    break;
                }
            } else if char == b'\\' {
                self.pos += 1;
                if self.pos < self.input.len() {
                    let escaped = self.input[self.pos];
                    match escaped {
                        b'0' => result.push(0x00),
                        b'\'' => result.push(b'\''),
                        b'"' => result.push(b'"'),
                        b'\\' => result.push(b'\\'),
                        b'n' => result.push(b'\n'),
                        b'r' => result.push(b'\r'),
                        b't' => result.push(b'\t'),
                        b'b' => result.push(0x08),
                        b'Z' => result.push(0x1A),
                        b'%' => result.push(b'%'),
                        b'_' => result.push(b'_'),
                        b'x' => {
                            self.pos += 1;
                            if self.pos + 1 < self.input.len() {
                                let high = self.input[self.pos] as char;
                                let low = self.input[self.pos + 1] as char;
                                if let (Some(high), Some(low)) =
                                    (high.to_digit(16), low.to_digit(16))
                                {
                                    result.push(((high << 4) | low) as u8);
                                }
                                self.pos += 1;
                            }
                        }
                        _ => result.push(escaped),
                    }
                    self.pos += 1;
                }
            } else {
                result.push(char);
                self.pos += 1;
            }
        }

        Ok(result)
    }

    fn read_number(&mut self) -> Vec<u8> {
        let start = self.pos;
        while self.pos < self.input.len() {
            let char = self.input[self.pos];
            if char.is_ascii_digit()
                || char == b'.'
                || char == b'-'
                || char == b'+'
                || char == b'e'
                || char == b'E'
            {
                self.pos += 1;
            } else {
                break;
            }
        }
        self.input[start..self.pos].to_vec()
    }

    fn read_identifier(&mut self) -> Vec<u8> {
        let start = self.pos;
        while self.pos < self.input.len() {
            let char = self.input[self.pos];
            if char.is_ascii_alphanumeric() || char == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        self.input[start..self.pos].to_vec()
    }

    fn match_keyword(&self, ident: &[u8]) -> Token {
        let upper = ident
            .iter()
            .map(|char| char.to_ascii_uppercase())
            .collect::<Vec<u8>>();

        match upper.as_slice() {
            b"INSERT" => Token::Insert,
            b"INTO" => Token::Into,
            b"VALUES" => Token::Values,
            b"_BINARY" => Token::BinaryPrefix,
            b"NULL" => Token::Null,
            _ => Token::Identifier(ident.to_vec()),
        }
    }
}

#[derive(Debug)]
struct InsertStatement {
    table_name: String,
    rows: Vec<Row>,
}

impl InsertStatement {
    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn row_values(&self) -> Vec<Vec<Value>> {
        self.rows
            .iter()
            .map(|row| row.convert_serde_value())
            .collect()
    }
}

#[derive(Debug, Clone)]
struct Row {
    values: Vec<InsertValue>,
}

impl Row {
    fn convert_serde_value(&self) -> Vec<Value> {
        self.values
            .iter()
            .map(|value| match value {
                InsertValue::String(data) => {
                    if data.len() == 1 {
                        return json!(data[0]);
                    }
                    let result = String::from_utf8(data.to_vec())
                        .unwrap_or_else(|_| general_purpose::STANDARD.encode(data));
                    json!(result)
                }
                InsertValue::Number(data) => {
                    let number = data.parse::<i64>().unwrap_or(0);
                    json!(number)
                }
                InsertValue::Binary(data) => json!(general_purpose::STANDARD.encode(data)),
                InsertValue::Null => Value::Null,
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
enum InsertValue {
    String(Vec<u8>),
    Number(String),
    Null,
    Binary(Vec<u8>),
}

struct InsertParser {
    tokens: Vec<Token>,
    pos: usize,
}

impl InsertParser {
    fn new(tokens: Vec<Token>) -> Self {
        InsertParser { tokens, pos: 0 }
    }

    fn parse(&mut self) -> Result<InsertStatement, String> {
        self.expect_token(Token::Insert)?;
        self.expect_token(Token::Into)?;
        let table_name = self.parse_table_name()?;
        self.expect_token(Token::Values)?;
        let rows = self.parse_rows()?;
        self.match_token(Token::Semicolon);

        Ok(InsertStatement { table_name, rows })
    }

    fn parse_table_name(&mut self) -> Result<String, String> {
        let has_backtick = self.match_token(Token::Backtick);

        match self.current_token() {
            Some(Token::Identifier(name)) => {
                let table_name = String::from_utf8_lossy(name).to_string();
                self.pos += 1;

                if has_backtick {
                    self.expect_token(Token::Backtick)?;
                }

                Ok(table_name)
            }
            _ => Err(format!(
                "Expected table name, found {:?}",
                self.current_token()
            )),
        }
    }

    fn parse_rows(&mut self) -> Result<Vec<Row>, String> {
        let mut rows = Vec::new();
        self.expect_token(Token::LeftParen)?;

        loop {
            let row = self.parse_row()?;
            rows.push(row);
            self.expect_token(Token::RightParen)?;

            if !self.match_token(Token::Comma) {
                break;
            }

            self.expect_token(Token::LeftParen)?;
        }

        Ok(rows)
    }

    fn parse_row(&mut self) -> Result<Row, String> {
        let mut values = Vec::new();
        let mut is_binary = false;

        loop {
            if self.match_token(Token::BinaryPrefix) {
                is_binary = true;
            }

            match self.current_token() {
                Some(Token::StringLiteral(value)) => {
                    let value = value.clone();
                    self.pos += 1;
                    if is_binary {
                        values.push(InsertValue::Binary(value));
                        is_binary = false;
                    } else {
                        values.push(InsertValue::String(value));
                    }
                }
                Some(Token::Number(number)) => {
                    let number = String::from_utf8_lossy(number).to_string();
                    self.pos += 1;
                    values.push(InsertValue::Number(number));
                }
                Some(Token::Null) => {
                    self.pos += 1;
                    values.push(InsertValue::Null);
                }
                Some(Token::RightParen) => break,
                Some(Token::EOF) => return Err("Unexpected end of input".to_string()),
                Some(_) => {
                    return Err(format!(
                        "Unexpected token in row: {:?}",
                        self.current_token()
                    ));
                }
                None => return Err("Unexpected end of input".to_string()),
            }

            if !self.match_token(Token::Comma) {
                break;
            }
        }

        Ok(Row { values })
    }

    fn current_token(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn match_token(&mut self, token: Token) -> bool {
        if let Some(current) = self.current_token() {
            if std::mem::discriminant(current) == std::mem::discriminant(&token) {
                self.pos += 1;
                return true;
            }
        }
        false
    }

    fn expect_token(&mut self, token: Token) -> Result<(), String> {
        if let Some(current) = self.current_token() {
            if std::mem::discriminant(current) == std::mem::discriminant(&token) {
                self.pos += 1;
                return Ok(());
            }
        }

        Err(format!(
            "Expected {:?}, found {:?}",
            token,
            self.current_token()
        ))
    }
}

fn parse_insert_sql(sql: &[u8]) -> Result<InsertStatement, String> {
    let mut lexer = InsertLexer::new(sql.to_vec());
    let tokens = lexer.tokenize()?;
    let mut parser = InsertParser::new(tokens);
    parser.parse()
}
