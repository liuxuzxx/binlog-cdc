use regex::Regex;
use serde::{Deserialize, Serialize};

///
/// 增加route的路由的配置
///
#[derive(Debug, Deserialize, Serialize)]
pub struct Route {
    #[serde(rename = "source-table")]
    source: String,
    #[serde(rename = "sink-table")]
    sink: String,
    #[serde(rename = "replace-symbol")]
    replace_symbol: Option<String>,
    description: Option<String>,

    #[serde(skip)]
    source_reg: Option<Regex>,
}

impl Route {
    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn sink(&self) -> &str {
        &self.sink
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn source_reg(&mut self) -> Option<&Regex> {
        if self.source_reg.is_none() {
            let source = format!("^{}$", self.source());
            if let Ok(reg) = Regex::new(source.as_str()) {
                self.source_reg = Some(reg);
                return self.source_reg.as_ref();
            } else {
                return None;
            }
        } else {
            return self.source_reg.as_ref();
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Router {
    route: Vec<Route>,
}

impl Router {
    pub fn find(&mut self, source: &str) -> Option<&Route> {
        let index = self
            .route
            .iter()
            .position(|ele| ele.source().eq(source))
            .or_else(|| {
                self.route.iter_mut().position(|ele| {
                    ele.source_reg()
                        .map(|reg| reg.is_match(source))
                        .unwrap_or(false)
                })
            });

        if let Some(index) = index {
            return self.route.get(index);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{Route, Router};

    fn route(source: &str, sink: &str) -> Route {
        Route {
            source: source.to_string(),
            sink: sink.to_string(),
            replace_symbol: None,
            description: None,
            source_reg: None,
        }
    }

    #[test]
    fn find_returns_exact_source_match_first() {
        let mut router = Router {
            route: vec![
                route("app_db\\..*", "regex_sink"),
                route("app_db.users", "exact_sink"),
            ],
        };

        let matched = router.find("app_db.users").unwrap();

        assert_eq!(matched.source(), "app_db.users");
        assert_eq!(matched.sink(), "exact_sink");
    }

    #[test]
    fn find_returns_regex_source_match_when_exact_missing() {
        let mut router = Router {
            route: vec![route("app_db\\..*", "regex_sink")],
        };

        let matched = router.find("app_db.orders").unwrap();

        assert_eq!(matched.source(), "app_db\\..*");
        assert_eq!(matched.sink(), "regex_sink");
    }

    #[test]
    fn find_returns_none_when_no_route_matches() {
        let mut router = Router {
            route: vec![route("app_db.users", "exact_sink")],
        };

        assert!(router.find("app_db.orders").is_none());
    }
}
