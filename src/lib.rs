use ensure::ensure;
use ensure::CheckEnsureResult::*;
use log::*;
use odbc_iter::{DefaultConfiguration, Executed, Handle, ResultSet, TryFromValueRow, ValueRow};
use problem::prelude::*;
use std::error::Error;
use std::fmt;

pub type Sql = String;

#[derive(Debug, PartialEq, Eq)]
pub enum SchemaState {
    Ok,
    Changed,
}

#[derive(Debug)]
pub enum SchemaStateError {
    CheckError(String, Problem),
    MeetError(String, Problem),
}

impl fmt::Display for SchemaStateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SchemaStateError::CheckError(name, problem) => {
                write!(f, "error checking schema state for '{}': {}", name, problem)
            }
            SchemaStateError::MeetError(name, problem) => {
                write!(f, "error meeting schema state for '{}': {}", name, problem)
            }
        }
    }
}

impl Error for SchemaStateError {}

/// Represents database operations needed to potentially initialize some schema object.
pub struct EnsureSchema {
    /// Reference name of the schema object.
    pub name: String,
    /// Query to run to see if we need to do anything; rows provided by this query are passed to
    /// ensure function.
    check_query: Sql,
    /// This is run with output of check_query to determine what needs to be done; if empty Vec is
    /// returned then nothing needs to be done otherwise each returned query is executed.
    ensure: Box<
        dyn for<'h, 'c> Fn(
            ResultSet<'h, 'c, ValueRow, Executed, DefaultConfiguration>,
        ) -> Result<Vec<Sql>, Problem>,
    >,
    /// If there are queries to be run then this Schemas are ensured first.
    meet_require: Vec<EnsureSchema>,
}

impl fmt::Debug for EnsureSchema {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("EnsureSchema")
            .field("name", &self.name)
            .field("check_query", &self.check_query)
            .field("meet_require", &self.meet_require)
            .finish()
    }
}

impl EnsureSchema {
    /// Creates `EnsureSchema` given name, SQL query strings that will be run to verify if the object is
    /// initialized and block of code that will get resulting check rows to
    /// return one or more initialization SQL query strings if needed.
    pub fn new(
        name: String,
        check_query: Sql,
        ensure: impl for<'h, 'c> Fn(
                ResultSet<'h, 'c, ValueRow, Executed, DefaultConfiguration>,
            ) -> Result<Vec<Sql>, Problem>
            + 'static,
    ) -> EnsureSchema {
        EnsureSchema {
            name,
            check_query,
            ensure: Box::new(ensure),
            meet_require: Vec::new(),
        }
    }

    /// Creates `EnsureSchema` given name, SQL query string that needs to produce one row with
    /// BOOLEAN/BIT value indicating if initialisation is needed (0/false) or not (1/true) and list
    /// of SQL query strings that need to be run to initialize the object.
    pub fn with_bool_check(name: String, check_query: Sql, meet_queries: Vec<Sql>) -> EnsureSchema {
        Self::new(name, check_query, move |rows| {
            let result: bool = TryFromValueRow::try_from_value_row(rows.single()?)?;
            Ok(if result { vec![] } else { meet_queries.clone() })
        })
    }

    /// Makes sure that another object is initialized before this one if this one needs to be
    /// initialized.
    pub fn with_meet_require(mut self, schema: EnsureSchema) -> EnsureSchema {
        self.meet_require.push(schema);
        self
    }

    /// Makes sure that the object is initialized by performing a check and necessary actions to
    /// initialize the object accordingly to check result.
    pub fn ensure(self, database: &mut Handle<'_>) -> Result<SchemaState, SchemaStateError> {
        self.ensure_with_dry_run(database, false)
    }

    /// Same as `ensure` but if `dry_run` is set to `true` no actual initialization queries are
    /// executed.
    pub fn ensure_with_dry_run(
        self,
        database: &mut Handle<'_>,
        dry_run: bool,
    ) -> Result<SchemaState, SchemaStateError> {
        ensure(move || {
            let Self {
                name,
                check_query,
                ensure,
                meet_require,
            } = self;
            debug!("[?] Ensuring schema state for: {}", name);

            let meet_queries = (|| {
                if dry_run {
                    info!("[check]: {}", check_query);
                }

                let check_rows = database.query(&check_query)?;
                Ok(ensure(check_rows)?)
            })()
            .map_err(|err| SchemaStateError::CheckError(name.clone(), err))?;

            Ok(if meet_queries.is_empty() {
                debug!("[+] Schema state is met for: {}", name);
                Met(SchemaState::Ok)
            } else {
                EnsureAction(move || {
                    for required in meet_require {
                        required.ensure_with_dry_run(database, dry_run)?;
                    }

                    info!("[!] Meeting schema state for: {}", name);
                    || -> Result<_, Problem> {
                        if !dry_run {
                            for meet_query in meet_queries {
                                database.query::<()>(&meet_query)?.no_result()?;
                            }

                            let check_rows = database.query(&check_query)?;
                            debug!("[~] Verifying schema state is met for: {}", name);
                            if !ensure(check_rows)?.is_empty() {
                                return problem!("Verification failed for schema state: {}", name);
                            }

                            Ok(SchemaState::Changed)
                        } else {
                            for meet_query in meet_queries {
                                info!("[would meet]: {}", meet_query);
                            }
                            Ok(SchemaState::Ok)
                        }
                    }()
                    .map_err(|err| SchemaStateError::MeetError(name, err))
                })
            })
        })
    }
}
