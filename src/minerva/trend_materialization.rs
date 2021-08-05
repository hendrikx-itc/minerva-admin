use std::fmt;
use std::time::Duration;
use serde::{Deserialize, Serialize};
use serde_yaml;
use glob::glob;

use postgres::{Client, types::ToSql};

use humantime::format_duration;

use super::change::Change;
use super::interval::parse_interval;
use super::super::error::{Error, RuntimeError, DatabaseError};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrendMaterializationSource {
    pub trend_store_part: String,
    pub mapping_function: String,
} 

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrendViewMaterialization {
    pub target_trend_store_part: String,
    pub enabled: bool,
    #[serde(with = "humantime_serde")]
    pub processing_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub stability_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub reprocessing_period: Duration,
    pub sources: Vec<TrendMaterializationSource>,
    pub view: String,
    pub fingerprint_function: String,
}

impl TrendViewMaterialization {
    fn define_materialization(&self, client: &mut Client) -> Result<(), String> {
        let query = concat!(
            "SELECT trend_directory.define_view_materialization(",
            "id, $1::text::interval, $2::text::interval, $3::text::interval, $4::text::regclass",
            ") ",
            "FROM trend_directory.trend_store_part WHERE name = $5",
        );

        let query_args: &[&(dyn ToSql + Sync)] = &[
            &format_duration(self.processing_delay).to_string(),
            &format_duration(self.stability_delay).to_string(),
            &format_duration(self.reprocessing_period).to_string(),
            &format!("trend.\"{}\"", &self.view_name()),
            &self.target_trend_store_part
        ];

        match client.query(query, &query_args) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error defining view materialization: {}", e))
        }
    }

    fn view_name(&self) -> String {
        format!("_{}", &self.target_trend_store_part)
    }

    pub fn drop_view(&self, client: &mut Client) -> Result<(), String> {
        let query = format!(
            "DROP VIEW IF EXISTS {}",
            format!("\"trend\".\"{}\"", &self.view_name()),
        );

        match client.execute(query.as_str(), &[]) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error dropping view: {}", e))
        }

    }

    pub fn create_view(&self, client: &mut Client) -> Result<(), String> {
        let query = format!(
            "CREATE VIEW {} AS {}",
            format!("\"trend\".\"{}\"", &self.view_name()),
            self.view,
        );

        match client.execute(query.as_str(), &[]) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error creating view: {}", e))
        }
    }

    fn create(&self, client: &mut Client) -> Result<(), String> {
        match self.create_view(client) {
            Ok(_) => {},
            Err(e) => {
                return Err(e)
            }
        }

        match self.define_materialization(client) {
            Ok(_) => {},
            Err(e) => {
                return Err(e)
            }
        }

        Ok(())
    }

    fn fingerprint_function_name(&self) -> String {
        format!("{}_fingerprint", self.target_trend_store_part)
    }

    fn create_fingerprint_function(&self, client: &mut Client) -> Result<(), String> {
        let query = format!(concat!(
            "CREATE FUNCTION trend.\"{}\"(timestamp with time zone) RETURNS trend_directory.fingerprint AS $$\n",
            "{}\n",
            "$$ LANGUAGE sql STABLE\n"
        ), self.fingerprint_function_name(), self.fingerprint_function);

        match client.query(query.as_str(), &[]) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error creating fingerprint function: {}", e))
        }
    }

    fn drop_fingerprint_function(&self, client: &mut Client) -> Result<(), String> {
        let query = format!(
            "DROP FUNCTION IF EXISTS trend.\"{}\"(timestamp with time zone)",
            self.fingerprint_function_name()
        );
        
        match client.query(query.as_str(), &[]) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error dropping fingerprint function: {}", e))
        }
    }   

    pub fn diff(&self, other: &TrendViewMaterialization) -> Vec<Box<dyn Change>> {
        let mut changes: Vec<Box<dyn Change>> = Vec::new();

        // Comparing a view from the database with a view definition is not
        // really usefull because PostgreSQL rewrites the SQL.
        //if self.view != other.view {
        //    changes.push(Box::new(UpdateView { trend_view_materialization: self.clone() }));
        //}

        if self.enabled != other.enabled || self.processing_delay != other.processing_delay || self.stability_delay != other.stability_delay || self.reprocessing_period != other.reprocessing_period {
            changes.push(Box::new(UpdateTrendViewMaterializationAttributes { trend_view_materialization: self.clone() }));
        }

        changes
    }

    fn update(&self, client: &mut Client) -> Result<(), String> {
        self.drop_fingerprint_function(client).unwrap();
        self.create_fingerprint_function(client).unwrap();

        Ok(())
    }
}

pub struct UpdateTrendViewMaterializationAttributes {
    pub trend_view_materialization: TrendViewMaterialization,
}

impl Change for UpdateTrendViewMaterializationAttributes {
    fn apply(&self, _client: &mut Client) -> Result<String, Error> {
        Ok(String::from(format!("Updated view {}", self.trend_view_materialization.view_name())))
    }
}

impl fmt::Display for UpdateTrendViewMaterializationAttributes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "UpdateTrendViewMaterializationAttributes({})",
            &self.trend_view_materialization.target_trend_store_part,
        )
    }
}

pub struct UpdateView {
    pub trend_view_materialization: TrendViewMaterialization,
}

impl Change for UpdateView {
    fn apply(&self, client: &mut Client) -> Result<String, Error> {
        self.trend_view_materialization.drop_view(client).unwrap();
        self.trend_view_materialization.create_view(client).unwrap();

        Ok(String::from(format!("Updated view {}", self.trend_view_materialization.view_name())))
    }
}

impl fmt::Display for UpdateView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "UpdateView({}, {})",
            &self.trend_view_materialization.target_trend_store_part,
            &self.trend_view_materialization.view_name()
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrendMaterializationFunction {
    pub return_type: String,
    pub src: String,
    pub language: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrendFunctionMaterialization {
    pub target_trend_store_part: String,
    pub enabled: bool,
    #[serde(with = "humantime_serde")]
    pub processing_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub stability_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub reprocessing_period: Duration,
    pub sources: Vec<TrendMaterializationSource>,
    pub function: TrendMaterializationFunction,
    pub fingerprint_function: String,
}

impl TrendFunctionMaterialization {
    fn define_materialization(&self, client: &mut Client) -> Result<(), String> {
        let query = concat!(
            "SELECT trend_directory.define_function_materialization(",
            "id, $1::text::interval, $2::text::interval, $3::text::interval, $4::regprocedure",
            ") ",
            "FROM trend_directory.trend_store_part WHERE name = $5",
        );

        let query_args: &[&(dyn ToSql + Sync)] = &[
            &format_duration(self.processing_delay).to_string(),
            &format_duration(self.stability_delay).to_string(),
            &format_duration(self.reprocessing_period).to_string(),
            &format!("trend.\"{}\"(timestamp with time zone)", &self.target_trend_store_part),
            &self.target_trend_store_part
        ];

        match client.query(query, &query_args) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error defining function materialization: {}", e))
        }
    }

    fn create_function(&self, client: &mut Client) -> Result<(), String> {
        let query = format!(
            "CREATE FUNCTION {} AS $function$\n{}\n$function$ LANGUAGE {}",
            format!("\"trend\".\"{}\"(timestamp with time zone)", &self.target_trend_store_part),
            &self.function.src,
            &self.function.language,
        );

        match client.execute(query.as_str(), &[]) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error creating view: {}", e))
        }
    }

    fn create(&self, client: &mut Client) -> Result<(), String> {
        match self.create_function(client) {
            Ok(_) => {},
            Err(e) => {
                return Err(e)
            }
        }

        match self.define_materialization(client) {
            Ok(_) => {},
            Err(e) => {
                return Err(e)
            }
        }

        Ok(())
    }

    fn fingerprint_function_name(&self) -> String {
        format!("{}_fingerprint", self.target_trend_store_part)
    }

    fn create_fingerprint_function(&self, client: &mut Client) -> Result<(), String> {
        let query = format!(concat!(
            "CREATE FUNCTION trend.\"{}\"(timestamp with time zone) RETURNS trend_directory.fingerprint AS $$\n",
            "{}\n",
            "$$ LANGUAGE sql STABLE\n"
        ), self.fingerprint_function_name(), self.fingerprint_function);

        match client.query(query.as_str(), &[]) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error creating fingerprint function: {}", e))
        }
    }

    fn drop_fingerprint_function(&self, client: &mut Client) -> Result<(), String> {
        let query = format!(
            "DROP FUNCTION IF EXISTS trend.\"{}\"(timestamp with time zone)",
            self.fingerprint_function_name()
        );
        
        match client.query(query.as_str(), &[]) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error dropping fingerprint function: {}", e))
        }
    }

    pub fn diff(&self, _other: &TrendFunctionMaterialization) -> Vec<Box<dyn Change>> {
        let changes: Vec<Box<dyn Change>> = Vec::new();

        changes
    }

    fn update(&self, client: &mut Client) -> Result<(), String> {
        self.drop_fingerprint_function(client).unwrap();
        self.create_fingerprint_function(client).unwrap();

        Ok(())
    }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum TrendMaterialization {
    View(TrendViewMaterialization),
    Function(TrendFunctionMaterialization),
}

impl fmt::Display for TrendMaterialization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrendMaterialization::View(_) => write!(f, "TrendMaterialization(View)"),
            TrendMaterialization::Function(_) => write!(f, "TrendMaterialization(Function)"),
        }
    }
}

impl TrendMaterialization {
    pub fn name(&self) -> &str {
        match self {
            TrendMaterialization::View(m) => &m.target_trend_store_part,
            TrendMaterialization::Function(m) => &m.target_trend_store_part,
        }
    }

    pub fn update(&self, client: &mut Client) -> Result<(), String> {
        match self {
            TrendMaterialization::View(m) => m.update(client),
            TrendMaterialization::Function(m) => m.update(client),
        }
    }

    pub fn create(&self, client: &mut Client) -> Result<(), String> {
        match self {
            TrendMaterialization::View(m) => m.create(client),
            TrendMaterialization::Function(m) => m.create(client),
        }
    }

    pub fn diff(&self, other: &TrendMaterialization) -> Vec<Box<dyn Change>> {
        match self {
            TrendMaterialization::View(m) => {
                match other {
                    TrendMaterialization::View(other_m) => m.diff(other_m),
                    TrendMaterialization::Function(_) => panic!("Incompatible materialization types"),
                }
            },
            TrendMaterialization::Function(m) => {
                match other {
                    TrendMaterialization::View(_) => panic!("Incompatible materialization types"),
                    TrendMaterialization::Function(other_m) => m.diff(other_m),
                }
            },
        }
    }
}

pub fn trend_materialization_from_config(path: &std::path::PathBuf) -> Result<TrendMaterialization, String> {
    let f = std::fs::File::open(&path).unwrap();
    let deserialize_result: Result<TrendMaterialization, serde_yaml::Error> = serde_yaml::from_reader(f);

    match deserialize_result {
        Ok(materialization) => Ok(materialization),
        Err(e) => {
            Err(format!("Error deserializing yaml: {}", e))
        }
    }
}

pub fn load_materializations_from(minerva_instance_root: &str) -> impl Iterator<Item = TrendMaterialization> {
    let glob_path = format!("{}/materialization/*.yaml", minerva_instance_root);

    glob(&glob_path)
        .expect("Failed to read glob pattern")
        .filter_map(|entry| match entry {
            Ok(path) => {
                match trend_materialization_from_config(&path) {
                    Ok(materialization) => Some(materialization),
                    Err(e) => {
                        println!("Error loading materialization '{}': {}", &path.display(), e);
                        None
                    }
                }
            }
            Err(_) => None,
        })
}

pub fn load_materializations(conn: &mut Client) -> Result<Vec<TrendMaterialization>, Error> {
    let mut trend_materializations: Vec<TrendMaterialization> = Vec::new();

    let query = concat!(
        "SELECT m.id, m.processing_delay::text, m.stability_delay::text, m.reprocessing_period::text, m.enabled, tsp.name, vm.src_view, fm.src_function ",
        "FROM trend_directory.materialization AS m ",
        "JOIN trend_directory.trend_store_part AS tsp ON tsp.id = m.dst_trend_store_part_id ",
        "LEFT JOIN trend_directory.view_materialization AS vm ON vm.materialization_id = m.id ",
        "LEFT JOIN trend_directory.function_materialization AS fm ON fm.materialization_id = m.id ",
    );

    let result = conn.query(query, &[]).map_err(|e| {
        DatabaseError::from_msg(format!("Error loading trend materializations: {}", e))
    })?;

    for row in result {
        let materialization_id: i32 = row.get(0);
        let processing_delay: String = row.get(1);
        let stability_delay: String = row.get(2);
        let reprocessing_period: String = row.get(3);
        let enabled: bool = row.get(4);
        let target_trend_store_part: String = row.get(5);
        let src_view: Option<String> = row.get(6);
        let src_function: Option<String> = row.get(7);

        if let Some(view) = src_view {
            let view_def = get_view_def(conn, &view).unwrap();
            let sources = load_sources(conn, materialization_id)?;

            let view_materialization = TrendViewMaterialization {
                target_trend_store_part: target_trend_store_part,
                enabled: enabled,
                fingerprint_function: String::from(""),
                processing_delay: parse_interval(&processing_delay).unwrap(),
                reprocessing_period: parse_interval(&reprocessing_period).unwrap(),
                sources: sources,
                stability_delay: parse_interval(&stability_delay).unwrap(),
                view: view_def,
            };

            let trend_materialization = TrendMaterialization::View(view_materialization);

            trend_materializations.push(trend_materialization);
        }

        if let Some(function) =  src_function {
            println!("{}", function);
        }
    }

    Ok(trend_materializations)
}

fn load_sources(conn: &mut Client, materialization_id: i32) -> Result<Vec<TrendMaterializationSource>, Error> {
    let mut sources: Vec<TrendMaterializationSource> = Vec::new();

    let query = concat!(
        "SELECT tsp.name, mtsl.timestamp_mapping_func::text ",
        "FROM trend_directory.materialization_trend_store_link mtsl ",
        "JOIN trend_directory.trend_store_part tsp ON tsp.id = mtsl.trend_store_part_id ",
        "WHERE mtsl.materialization_id = $1"
    );

    let result = conn.query(query, &[&materialization_id]).map_err(|e| {
        DatabaseError::from_msg(format!("Error loading trend materializations: {}", e))
    })?;

    for row in result {
        let trend_store_part: String = row.get(0);
        let mapping_function: String = row.get(1);

        sources.push(TrendMaterializationSource {
            trend_store_part: trend_store_part,
            mapping_function: mapping_function,
        });
    }

    Ok(sources)
}

// Load the body of a function by specifying it's full name
pub fn get_view_def(client: &mut Client, view: &str) -> Option<String> {
    let query = format!(concat!(
        "SELECT pg_get_viewdef('{}'::regclass::oid);"
    ), view);

    match client.query_one(query.as_str(), &[]) {
        Ok(row) => row.get(0),
        Err(_) => None,
    }
}

pub struct AddTrendMaterialization {
    pub trend_materialization: TrendMaterialization,
}

impl fmt::Display for AddTrendMaterialization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AddTrendMaterialization({})", &self.trend_materialization)
    }
}

impl Change for AddTrendMaterialization {
    fn apply(&self, client: &mut Client) -> Result<String, Error> {
        match self.trend_materialization.create(client) {
            Ok(_) => Ok(format!("Added trend materialization {}", &self.trend_materialization)),
            Err(e) => Err(Error::Runtime(RuntimeError { msg: format!("Error adding trend materialization {}: {}", &self.trend_materialization, e) }))
        }
    }
}

impl From<TrendMaterialization> for AddTrendMaterialization {
    fn from(trend_materialization: TrendMaterialization) -> Self {
        AddTrendMaterialization {
            trend_materialization: trend_materialization
        }
    }
}