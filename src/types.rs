//! Basic types for api-query

use std::{
    convert::{TryFrom, TryInto},
    fmt::Display,
    ops::Range,
    str::FromStr,
};

use anyhow::{anyhow, Context, Result};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Query<'s> {
    /// e.g. line from the queries file, or all of stdin
    pub string: &'s str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueryReference {
    /// entry (line) in queries file, 0-based
    pub query_index: u32,
}

/// Show line number, 1-based
impl Display for QueryReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.query_index + 1)
    }
}

impl FromStr for QueryReference {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let n = u64::from_str(s).context("parsing line number")?;
        Ok(QueryReference {
            query_index: n
                .checked_sub(1)
                .with_context(|| anyhow!("line number must be at least 1: {n}"))?
                .try_into()
                .context("parsing line number")?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct QueryReferenceWithRepetition {
    pub query_reference: QueryReference,
    pub repetition: u32,
}

#[test]
fn t_sizes() {
    assert_eq!(size_of::<Query>(), 16);
    assert_eq!(size_of::<[Query; 2]>(), 32);
    assert_eq!(size_of::<QueryReference>(), 4);
    assert_eq!(size_of::<[QueryReference; 2]>(), 8);
}

#[ouroboros::self_referencing]
pub struct Queries {
    queries_string: String,
    #[borrows(queries_string)]
    #[covariant]
    pub queries: Vec<Query<'this>>,
}

impl Queries {
    fn _new(queries_string: String, split: bool) -> Result<Self> {
        Self::try_new(queries_string, |queries_string| -> Result<_> {
            let queries: Vec<Query> = if split {
                let mut queries: Vec<Query> = queries_string
                    .split('\n')
                    .map(|string| Query { string })
                    .collect();
                if queries
                    .last()
                    .expect("split always gives at least 1 empty string item")
                    .string
                    .is_empty()
                {
                    queries.pop();
                }
                queries
            } else {
                vec![Query {
                    string: queries_string,
                }]
            };
            (|| -> Option<_> {
                let maxline: usize = queries.len().checked_add(1)?;
                let _maxline: u32 = u32::try_from(maxline).ok()?;
                Some(())
            })()
            .ok_or_else(|| anyhow!(">= u32 lines in file"))?;
            Ok(queries)
        })
    }

    pub fn from_lines_string(queries_string: String) -> Result<Self> {
        Self::_new(queries_string, true)
    }

    pub fn from_single_query(queries_string: String) -> Result<Self> {
        Self::_new(queries_string, false)
    }

    fn get_query(&self, i: u32) -> Query<'_> {
        self.borrow_queries()[usize::try_from(i).expect("correct index generation")].clone()
    }

    pub fn query_index_range(&self) -> Range<usize> {
        0..self.borrow_queries().len()
    }
}

impl QueryReferenceWithRepetition {
    pub fn query<'q>(&self, queries: &'q Queries) -> Query<'q> {
        queries.get_query(self.query_reference.query_index)
    }

    /// The file name is the line number (1-based) of the queries
    /// file, 0-padded for easy sorting, and the repetition count
    /// (0-based) for that query if a non-1 repetition count was
    /// requested.
    pub fn output_file_name(&self, show_repetition: bool) -> String {
        let line = u64::from(self.query_reference.query_index) + 1;
        if show_repetition {
            let repetition = self.repetition;
            format!("{line:06}-{repetition:06}")
        } else {
            format!("{line:06}")
        }
    }
}
