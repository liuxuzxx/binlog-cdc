use regex::Regex;
use serde_json::{Map, Value, json};
use sqlparser::{
    ast::{Expr, ObjectNamePart, SelectItem, Statement, UnaryOperator, Value as SqlValue},
    dialect::GenericDialect,
    parser::Parser,
};
use tracing::{info, warn};

use crate::{config::cdc::Transform, transform::functions::TransformFunction};

///
/// 需要构建一个struct，存放: table以及对应的transform的projection
/// 这个地方其实是不是可以有一个预编译的操作，就是已经知道了具备的函数以及生成的字段，是否可以直接转成
/// 自己的内部数据结构，然后直接map生成，而不是每次都需要去解析和处理，即使这个地方省略了parse环节，但是整个eval
/// 的过程还是很长的
///

pub struct ProjectionHandler {
    parseres: Option<Vec<StatementParser>>,
}

impl ProjectionHandler {
    pub fn create(trans: Option<&Vec<Transform>>) -> Self {
        return trans
            .map(|trans| {
                let parsers = trans
                    .iter()
                    .map(|tran| StatementParser::new(tran))
                    .collect();
                return Some(parsers);
            })
            .map(|parsers| {
                return ProjectionHandler { parseres: parsers };
            })
            .unwrap_or(ProjectionHandler { parseres: None });
    }

    pub fn eval(&self, qualified_table_name: &str, source: &mut Map<String, Value>) {
        if let Some(parseres) = &self.parseres {
            let parser = parseres
                .iter()
                .find(|ele| ele.table_name_reg.is_match(qualified_table_name));

            match parser {
                Some(parser) => {
                    parser.eval(source);
                }
                None => {}
            }
        }
    }
}

pub struct StatementParser {
    // 其实这个地方只需要处理Query的语句就可以了
    statement: Statement,
    table_name_reg: Regex,
}

impl StatementParser {
    pub fn new(trans: &Transform) -> Self {
        let reg = Regex::new(trans.source_table()).expect(
            format!(
                "regex transform source-table:{} error!",
                trans.source_table()
            )
            .as_str(),
        );
        let extend_sql = format!("select {} from sts", trans.projection());
        let dialect = GenericDialect {};
        let statement = Parser::new(&dialect)
            .try_with_sql(&extend_sql)
            .expect(format!("parse sql:{} error!", trans.projection()).as_str())
            .parse_statement()
            .expect(format!("parse sql:{} to statement error!", trans.projection()).as_str());

        return StatementParser {
            statement: statement,
            table_name_reg: reg,
        };
    }

    ///
    /// 对于sqlparser解析出来的任何元素，都可以使用visit的模式进行遍历，目前使用常规的操作处理
    pub fn eval(&self, source: &mut Map<String, Value>) {
        if let Statement::Query(query) = &self.statement {
            query
                .body
                .as_select()
                .unwrap()
                .projection
                .iter()
                .for_each(|item| match item {
                    SelectItem::UnnamedExpr(expr) => match expr {
                        Expr::Identifier(_ident) => {
                            info!("常量");
                        }
                        _ => {
                            warn!(
                                "暂时不支持的expr类型:{}",
                                serde_json::to_string(&expr).unwrap()
                            );
                        }
                    },
                    SelectItem::ExprWithAlias { expr, alias } => match expr {
                        Expr::Function(function) => {
                            function.name.0.iter().for_each(|part| {
                                if let ObjectNamePart::Identifier(ident) = part {
                                    let func_name = &ident.value;
                                    match func_name.to_uppercase().as_str() {
                                        "LOCALTIMESTAMP" => {
                                            let result =
                                                TransformFunction::LOCALTIMESTAMP.transform();
                                            source.insert(alias.value.to_string(), result);
                                        }
                                        _ => {
                                            warn!("暂时不支持的transform类型");
                                        }
                                    }
                                }
                            });
                        }
                        Expr::Value(value) => match &value.value {
                            SqlValue::SingleQuotedString(value) => {
                                source.insert(alias.value.to_string(), json!(value));
                            }
                            SqlValue::Number(number, _flag) => {
                                let number = number.parse::<i64>().unwrap();
                                source.insert(alias.value.to_string(), json!(number));
                            }
                            _ => {
                                warn!(
                                    "暂时不支持的value类型:{}",
                                    serde_json::to_string(&value.value).unwrap()
                                );
                            }
                        },
                        Expr::UnaryOp { op, expr } => match &**expr {
                            Expr::Value(value) => match &value.value {
                                SqlValue::Number(number, _flag) => match op {
                                    UnaryOperator::Minus => {
                                        let number = number.parse::<i64>().unwrap();
                                        source.insert(alias.value.to_string(), json!(-1 * number));
                                    }
                                    _ => {
                                        warn!("暂时不支持的op类型");
                                    }
                                },
                                _ => {
                                    warn!(
                                        "暂时不支持的value类型,什么类型:{}",
                                        serde_json::to_string(&value.value).unwrap()
                                    );
                                }
                            },
                            _ => {
                                warn!("暂时不支持的expr类型");
                            }
                        },
                        _ => {
                            warn!(
                                "暂时不支持的expr类型 expr with alias:{}",
                                serde_json::to_string(&expr).unwrap()
                            );
                        }
                    },
                    _ => {
                        warn!("暂时不支持的select item类型");
                    }
                });
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, json};
    use tracing::info;

    use crate::{LocalTimer, config::cdc::Transform, transform::parser::ProjectionHandler};

    fn init() {
        tracing_subscriber::fmt().with_timer(LocalTimer).init();
    }

    #[test]
    fn test_eval() {
        init();

        let mut user_map = Map::new();
        user_map.insert("user_name".to_string(), json!("liuruishan"));

        let mut order_map = Map::new();
        order_map.insert("ticket_id".to_string(), json!(123456));

        let trans = vec![
            Transform::new(
                "sys_user",
                "'' as hostname,-1 as port, '' as password, 0 as count",
            ),
            Transform::new(
                "order_db.order_list_[0-9]+",
                "LOCALTIMESTAMP as update_time",
            ),
        ];

        let handle = ProjectionHandler::create(Some(&trans));
        handle.eval("sys_user", &mut user_map);

        handle.eval("order_db.order_list_20051010", &mut order_map);

        info!(
            "eval result:{} {}",
            serde_json::to_string(&user_map).unwrap(),
            serde_json::to_string(&order_map).unwrap()
        );
    }
}
