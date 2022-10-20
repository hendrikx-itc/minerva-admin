use glob::glob;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_yaml;
use std::fmt;
use std::path::Path;
use std::time::Duration;

use postgres_protocol::escape::escape_identifier;
use tokio_postgres::{types::ToSql, Client};

use humantime::format_duration;

use async_trait::async_trait;

use super::change::{Change, ChangeResult};
use super::error::{DatabaseError, Error, RuntimeError};
use super::interval::parse_interval;

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
    pub description: Value,
}

impl TrendViewMaterialization {
    async fn define_materialization(&self, client: &mut Client) -> Result<(), Error> {
        let query = format!(
            concat!(
		"SELECT trend_directory.define_view_materialization(",
		"id, $1::text::interval, $2::text::interval, $3::text::interval, $4::text::regclass, ",
		"{}::jsonb) ",
		"FROM trend_directory.trend_store_part WHERE name = $5",
            ),
            &self.description.to_string()
        );

        let query_args: &[&(dyn ToSql + Sync)] = &[
            &format_duration(self.processing_delay).to_string(),
            &format_duration(self.stability_delay).to_string(),
            &format_duration(self.reprocessing_period).to_string(),
            &format!("trend.{}", escape_identifier(&self.view_name())),
            &self.target_trend_store_part,
        ];

        match client.query(&query, query_args).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Database(DatabaseError::from_msg(format!(
                "Error defining view materialization: {}",
                e
            )))),
        }
    }

    fn view_name(&self) -> String {
        format!("_{}", &self.target_trend_store_part)
    }

    pub async fn drop_view(&self, client: &mut Client) -> Result<(), Error> {
        let query = format!(
            "DROP VIEW IF EXISTS trend.{}",
            &escape_identifier(&self.view_name()),
        );

        match client.execute(query.as_str(), &[]).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Database(DatabaseError::from_msg(format!(
                "Error dropping view: {}",
                e
            )))),
        }
    }

    pub async fn create_view(&self, client: &mut Client) -> Result<(), Error> {
        let query = format!(
            "CREATE VIEW trend.{} AS {}",
            &escape_identifier(&self.view_name()),
            self.view,
        );

        match client.execute(query.as_str(), &[]).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Database(DatabaseError::from_msg(format!(
                "Error creating view: {}",
                e
            )))),
        }
    }

    async fn create(&self, client: &mut Client) -> Result<(), Error> {
        self.create_view(client).await?;
        self.define_materialization(client).await?;
        self.create_fingerprint_function(client).await?;

        Ok(())
    }

    fn fingerprint_function_name(&self) -> String {
        format!("{}_fingerprint", self.target_trend_store_part)
    }

    async fn create_fingerprint_function(&self, client: &mut Client) -> Result<(), Error> {
        let query = format!(concat!(
            "CREATE FUNCTION trend.{}(timestamp with time zone) RETURNS trend_directory.fingerprint AS $$\n",
            "{}\n",
            "$$ LANGUAGE sql STABLE\n"
        ), escape_identifier(&self.fingerprint_function_name()), self.fingerprint_function);

        match client.query(query.as_str(), &[]).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Database(DatabaseError::from_msg(format!(
                "Error creating fingerprint function: {}",
                e
            )))),
        }
    }

    async fn drop_fingerprint_function(&self, client: &mut Client) -> Result<(), Error> {
        let query = format!(
            "DROP FUNCTION IF EXISTS trend.{}(timestamp with time zone)",
            escape_identifier(&self.fingerprint_function_name())
        );

        match client.query(query.as_str(), &[]).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Database(DatabaseError::from_msg(format!(
                "Error dropping fingerprint function: {}",
                e
            )))),
        }
    }

    pub fn diff<'a>(&self, other: &TrendViewMaterialization) -> Vec<Box<dyn Change + Send>> {
        let mut changes: Vec<Box<dyn Change + Send>> = Vec::new();

        // Comparing a view from the database with a view definition is not
        // really usefull because PostgreSQL rewrites the SQL.
        //if self.view != other.view {
        //    changes.push(Box::new(UpdateView { trend_view_materialization:
        // self.clone() }));
        //}

        if self.enabled != other.enabled
            || self.processing_delay != other.processing_delay
            || self.stability_delay != other.stability_delay
            || self.reprocessing_period != other.reprocessing_period
        {
            changes.push(Box::new(UpdateTrendViewMaterializationAttributes {
                trend_view_materialization: other.clone(),
            }));
        }

        changes
    }

    async fn update(&self, client: &mut Client) -> Result<(), Error> {
        self.drop_fingerprint_function(client).await?;
        self.drop_view(client).await?;

        self.create_view(client).await?;
        self.create_fingerprint_function(client).await?;

        self.update_attributes(client).await?;

        Ok(())
    }

    async fn update_attributes(&self, client: &mut Client) -> Result<(), Error> {
        let query = format!(
            concat!(
                "UPDATE trend_directory.materialization ",
                "SET processing_delay = $1::text::interval, ",
                "stability_delay = $2::text::interval, ",
                "reprocessing_period = $3::text::interval, ",
                "enabled = $4, ",
                "description = '{}'::jsonb ",
                "WHERE materialization::text = $5",
            ),
            &self.description.to_string()
        );

        let query_args: &[&(dyn ToSql + Sync)] = &[
            &format_duration(self.processing_delay).to_string(),
            &format_duration(self.stability_delay).to_string(),
            &format_duration(self.reprocessing_period).to_string(),
            &self.enabled,
            &self.target_trend_store_part,
        ];

        match client.execute(&query, query_args).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Database(DatabaseError::from_msg(format!(
                "Error updating view materialization attributes: {}",
                e
            )))),
        }
    }
}

pub struct UpdateTrendViewMaterializationAttributes {
    pub trend_view_materialization: TrendViewMaterialization,
}

#[async_trait]
impl Change for UpdateTrendViewMaterializationAttributes {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        self.trend_view_materialization
            .update_attributes(client)
            .await?;

        Ok("Updated attributes of view materialization".into())
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

#[async_trait]
impl Change for UpdateView {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        self.trend_view_materialization
            .drop_view(client)
            .await
            .unwrap();
        self.trend_view_materialization
            .create_view(client)
            .await
            .unwrap();

        Ok(format!(
            "Updated view {}",
            self.trend_view_materialization.view_name()
        ))
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
    pub description: Value,
}

impl TrendFunctionMaterialization {
    async fn define_materialization(&self, client: &mut Client) -> Result<(), Error> {
        let query = format!(
            concat!(
		"SELECT trend_directory.define_function_materialization(",
		"id, $1::text::interval, $2::text::interval, $3::text::interval, $4::text::regprocedure, ",
		"'{}'::jsonb) ",
		"FROM trend_directory.trend_store_part WHERE name = $5",
            ),
            &self.description.to_string(),
        );

        let query_args: &[&(dyn ToSql + Sync)] = &[
            &format_duration(self.processing_delay).to_string(),
            &format_duration(self.stability_delay).to_string(),
            &format_duration(self.reprocessing_period).to_string(),
            &format!(
                "trend.{}(timestamp with time zone)",
                escape_identifier(&self.target_trend_store_part)
            ),
            &self.target_trend_store_part,
        ];

        match client.query(&query, query_args).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Database(DatabaseError::from_msg(format!(
                "Error defining function materialization: {}",
                e
            )))),
        }
    }

    async fn drop_function(&self, client: &mut Client) -> Result<(), Error> {
        let query = format!(
            "DROP FUNCTION IF EXISTS trend.{}(timestamp with time zone)",
            &escape_identifier(&self.target_trend_store_part),
        );

        match client.execute(query.as_str(), &[]).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Database(DatabaseError::from_msg(format!(
                "Error dropping function: {}",
                e
            )))),
        }
    }

    async fn create_function(&self, client: &mut Client) -> Result<(), Error> {
        let query = format!(
            "CREATE FUNCTION trend.{}(timestamp with time zone) RETURNS {} AS $function$\n{}\n$function$ LANGUAGE {}",
            &escape_identifier(&self.target_trend_store_part),
            &self.function.return_type,
            &self.function.src,
            &self.function.language,
        );

        match client.execute(query.as_str(), &[]).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Database(DatabaseError::from_msg(format!(
                "Error creating function: {}",
                e
            )))),
        }
    }

    async fn create(&self, client: &mut Client) -> Result<(), Error> {
        self.create_function(client).await?;
        self.create_fingerprint_function(client).await?;
        self.define_materialization(client).await?;
	if self.enabled { self.do_enable(client).await? };
        self.connect_sources(client).await?;

        Ok(())
    }

    fn fingerprint_function_name(&self) -> String {
        format!("{}_fingerprint", self.target_trend_store_part)
    }

    async fn create_fingerprint_function(&self, client: &mut Client) -> Result<(), Error> {
        let query = format!(concat!(
            "CREATE FUNCTION trend.{}(timestamp with time zone) RETURNS trend_directory.fingerprint AS $$\n",
            "{}\n",
            "$$ LANGUAGE sql STABLE\n"
        ), escape_identifier(&self.fingerprint_function_name()), self.fingerprint_function);

        match client.query(query.as_str(), &[]).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Database(DatabaseError::from_msg(format!(
                "Error creating fingerprint function: {}",
                e
            )))),
        }
    }

    async fn drop_fingerprint_function(&self, client: &mut Client) -> Result<(), Error> {
        let query = format!(
            "DROP FUNCTION IF EXISTS trend.{}(timestamp with time zone)",
            escape_identifier(&self.fingerprint_function_name())
        );

        match client.query(query.as_str(), &[]).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Database(DatabaseError::from_msg(format!(
                "Error dropping fingerprint function: {}",
                e
            )))),
        }
    }

    async fn do_enable(&self, client: &mut Client) -> Result<(), Error> {
	let query = concat!(
	    "UPDATE trend_directory.materialization AS m ",
	    "SET enabled = true ",
	    "FROM trend_directory.trend_store_part AS dtsp ",
	    "WHERE m.dst_trend_store_part_id = dtsp.id ",
	    "AND dtsp.name = $1"
	);
	match client.query(query, &[&self.target_trend_store_part]).await {
	    Ok(_) => Ok(()),
	    Err(e) => Err(Error::Database(DatabaseError::from_msg(format!(
		"Unable to enable materialization: {}",
		e
	    ))))
	}
    }
    
    async fn connect_sources(&self, client: &mut Client) -> Result<(), Error> {
        let mut result: Result<(), Error> = Ok(());
        for source in &self.sources {
            let query = format!(
                concat!(
		    "INSERT INTO trend_directory.materialization_trend_store_link ",
		    "SELECT m.id AS materialization_id, ",
		    "stsp.id AS trend_store_part_id, ",
		    "'{}' AS timestamp_mapping_func ",
		    "FROM trend_directory.materialization m JOIN trend_directory.trend_store_part dstp ",
		    "ON m.dst_trend_store_part_id = dstp.id, ",
		    "trend_directory.trend_store_part stsp ",
		    "WHERE dstp.name = '{}' AND stsp.name = '{}'"
		),
                &source.mapping_function, &self.target_trend_store_part, &source.trend_store_part
            );
            match client.query(&query, &[]).await {
                Ok(_) => {}
                Err(e) => {
                    result = Err(Error::Database(DatabaseError::from_msg(format!(
                        "Error connecting sources: {}",
                        e
                    ))))
                }
            }
        }
        result
    }

    async fn drop_sources(&self, client: &mut Client) -> Result<(), Error> {
        let query = format!(
            concat!(
		"DELETE FROM trend_directory.materialization_trend_store_link tsl ",
		"USING trend_directory.materialization m JOIN trend_directory.trend_store_part dstp ",
		"ON m.dst_trend_store_part_id = dstp.id ",
		"WHERE dstp.name = '{}' AND tsl.materialization_id = m.id"
	    ),
            &self.target_trend_store_part
        );
        match client.query(&query, &[]).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Database(DatabaseError::from_msg(format!(
                "Error removing old sources: {}",
                e
            )))),
        }
    }

    pub fn diff<'a>(&self, _other: &TrendFunctionMaterialization) -> Vec<Box<dyn Change + Send>> {
        let changes = Vec::new();

        changes
    }

    async fn update_attributes(&self, client: &mut Client) -> Result<(), Error> {
        let query = format!(
            concat!(
                "UPDATE trend_directory.materialization ",
                "SET processing_delay = $1::text::interval, ",
                "stability_delay = $2::text::interval, ",
                "reprocessing_period = $3::text::interval, ",
                "enabled = $4, ",
                "description = '{}'::jsonb ",
                "WHERE materialization::text = $5",
            ),
            &self.description.to_string()
        );

        let query_args: &[&(dyn ToSql + Sync)] = &[
            &format_duration(self.processing_delay).to_string(),
            &format_duration(self.stability_delay).to_string(),
            &format_duration(self.reprocessing_period).to_string(),
            &self.enabled,
            &self.target_trend_store_part,
        ];

        match client.execute(&query, query_args).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Database(DatabaseError::from_msg(format!(
                "Error updating view materialization attributes: {}",
                e
            )))),
        }
    }

    async fn update(&self, client: &mut Client) -> Result<(), Error> {
        self.drop_fingerprint_function(client).await?;
        self.drop_function(client).await?;
        self.drop_sources(client).await?;

        self.update_attributes(client).await?;
        self.create_function(client).await?;
        self.create_fingerprint_function(client).await?;
        self.connect_sources(client).await?;

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
            TrendMaterialization::View(view_materialization) => write!(
                f,
                "TrendViewMaterialization('{}')",
                &view_materialization.target_trend_store_part
            ),
            TrendMaterialization::Function(function_materialization) => write!(
                f,
                "TrendFunctionMaterialization('{}')",
                &function_materialization.target_trend_store_part
            ),
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

    pub async fn update(&self, client: &mut Client) -> Result<(), Error> {
        match self {
            TrendMaterialization::View(m) => m.update(client).await,
            TrendMaterialization::Function(m) => m.update(client).await,
        }
    }

    pub async fn create(&self, client: &mut Client) -> Result<(), Error> {
        match self {
            TrendMaterialization::View(m) => m.create(client).await,
            TrendMaterialization::Function(m) => m.create(client).await,
        }
    }

    pub fn diff<'a>(&self, other: &TrendMaterialization) -> Vec<Box<dyn Change + Send>> {
        match self {
            TrendMaterialization::View(m) => match other {
                TrendMaterialization::View(other_m) => m.diff(other_m),
                TrendMaterialization::Function(_) => panic!("Incompatible materialization types"),
            },
            TrendMaterialization::Function(m) => match other {
                TrendMaterialization::View(_) => panic!("Incompatible materialization types"),
                TrendMaterialization::Function(other_m) => m.diff(other_m),
            },
        }
    }
}

pub fn trend_materialization_from_config(
    path: &std::path::PathBuf,
) -> Result<TrendMaterialization, Error> {
    let f = std::fs::File::open(&path).map_err(|e| {
        Error::Runtime(RuntimeError::from_msg(format!(
            "could not open definition file: {}",
            e
        )))
    })?;
    let deserialize_result: Result<TrendMaterialization, serde_yaml::Error> =
        serde_yaml::from_reader(f);

    match deserialize_result {
        Ok(materialization) => Ok(materialization),
        Err(e) => Err(Error::Runtime(RuntimeError::from_msg(format!(
            "could not deserialize materialization: {}",
            e
        )))),
    }
}

pub fn load_materializations_from(
    minerva_instance_root: &Path,
) -> impl Iterator<Item = TrendMaterialization> {
    let glob_path = format!(
        "{}/materialization/*.yaml",
        minerva_instance_root.to_string_lossy()
    );

    glob(&glob_path)
        .expect("Failed to read glob pattern")
        .filter_map(|entry| match entry {
            Ok(path) => match trend_materialization_from_config(&path) {
                Ok(materialization) => Some(materialization),
                Err(e) => {
                    println!("Error loading materialization '{}': {}", &path.display(), e);
                    None
                }
            },
            Err(_) => None,
        })
}

pub async fn load_materializations(conn: &mut Client) -> Result<Vec<TrendMaterialization>, Error> {
    let mut trend_materializations: Vec<TrendMaterialization> = Vec::new();

    let query = concat!(
        "SELECT m.id, m.processing_delay::text, m.stability_delay::text, m.reprocessing_period::text, m.enabled, m.description, tsp.name, vm.src_view, fm.src_function ",
        "FROM trend_directory.materialization AS m ",
        "JOIN trend_directory.trend_store_part AS tsp ON tsp.id = m.dst_trend_store_part_id ",
        "LEFT JOIN trend_directory.view_materialization AS vm ON vm.materialization_id = m.id ",
        "LEFT JOIN trend_directory.function_materialization AS fm ON fm.materialization_id = m.id ",
    );

    let result = conn.query(query, &[]).await.map_err(|e| {
        DatabaseError::from_msg(format!("Error loading trend materializations: {}", e))
    })?;

    for row in result {
        let materialization_id: i32 = row.get(0);
        let processing_delay: String = row.get(1);
        let stability_delay: String = row.get(2);
        let reprocessing_period: String = row.get(3);
        let enabled: bool = row.get(4);
        let description: Value = row.get(5);
        let target_trend_store_part: String = row.get(6);
        let src_view: Option<String> = row.get(7);
        let src_function: Option<String> = row.get(8);

        if let Some(view) = src_view {
            let view_def = get_view_def(conn, &view).await.unwrap();
            let sources = load_sources(conn, materialization_id).await?;

            let view_materialization = TrendViewMaterialization {
                target_trend_store_part: target_trend_store_part.clone(),
                enabled,
                fingerprint_function: String::from(""),
                processing_delay: parse_interval(&processing_delay).unwrap(),
                reprocessing_period: parse_interval(&reprocessing_period).unwrap(),
                sources,
                stability_delay: parse_interval(&stability_delay).unwrap(),
                view: view_def,
                description: description.clone(),
            };

            let trend_materialization = TrendMaterialization::View(view_materialization);

            trend_materializations.push(trend_materialization);
        }

        if let Some(function) = src_function {
            let function_def = get_function_def(conn, &function)
                .await
                .unwrap_or("failed getting sources".into());
            let sources = load_sources(conn, materialization_id).await?;

            let function_materialization = TrendFunctionMaterialization {
                target_trend_store_part: target_trend_store_part.clone(),
                enabled,
                fingerprint_function: String::from(""),
                processing_delay: parse_interval(&processing_delay).unwrap(),
                reprocessing_period: parse_interval(&reprocessing_period).unwrap(),
                sources,
                stability_delay: parse_interval(&stability_delay).unwrap(),
                function: TrendMaterializationFunction {
                    return_type: "".into(),
                    src: function_def,
                    language: "plpgsql".into(),
                },
                description: description.clone(),
            };

            let trend_materialization = TrendMaterialization::Function(function_materialization);

            trend_materializations.push(trend_materialization);
        }
    }

    Ok(trend_materializations)
}

async fn load_sources(
    conn: &mut Client,
    materialization_id: i32,
) -> Result<Vec<TrendMaterializationSource>, Error> {
    let mut sources: Vec<TrendMaterializationSource> = Vec::new();

    let query = concat!(
        "SELECT tsp.name, mtsl.timestamp_mapping_func::text ",
        "FROM trend_directory.materialization_trend_store_link mtsl ",
        "JOIN trend_directory.trend_store_part tsp ON tsp.id = mtsl.trend_store_part_id ",
        "WHERE mtsl.materialization_id = $1"
    );

    let result = conn
        .query(query, &[&materialization_id])
        .await
        .map_err(|e| {
            DatabaseError::from_msg(format!("Error loading trend materializations: {}", e))
        })?;

    for row in result {
        let trend_store_part: String = row.get(0);
        let mapping_function: String = row.get(1);

        sources.push(TrendMaterializationSource {
            trend_store_part,
            mapping_function,
        });
    }

    Ok(sources)
}

// Load the body of a function by specifying it's full name
pub async fn get_view_def(client: &mut Client, view: &str) -> Option<String> {
    let query = format!(concat!("SELECT pg_get_viewdef('{}'::regclass::oid);"), view);

    match client.query_one(query.as_str(), &[]).await {
        Ok(row) => row.get(0),
        Err(_) => None,
    }
}

pub async fn get_function_def(client: &mut Client, function: &str) -> Option<String> {
    let query = format!("SELECT prosrc FROM pg_proc WHERE proname = $1");

    match client.query_one(query.as_str(), &[&function]).await {
        Ok(row) => row.get(0),
        Err(_) => None,
    }
}

pub struct AddTrendMaterialization {
    pub trend_materialization: TrendMaterialization,
}

impl fmt::Display for AddTrendMaterialization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AddTrendMaterialization({})",
            &self.trend_materialization
        )
    }
}

#[async_trait]
impl Change for AddTrendMaterialization {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        match self.trend_materialization.create(client).await {
            Ok(_) => Ok(format!(
                "Added trend materialization '{}'",
                &self.trend_materialization
            )),
            Err(e) => Err(Error::Runtime(RuntimeError {
                msg: format!(
                    "Error adding trend materialization '{}': {}",
                    &self.trend_materialization, e
                ),
            })),
        }
    }
}

impl From<TrendMaterialization> for AddTrendMaterialization {
    fn from(trend_materialization: TrendMaterialization) -> Self {
        AddTrendMaterialization {
            trend_materialization,
        }
    }
}

pub struct UpdateTrendMaterialization {
    pub trend_materialization: TrendMaterialization,
}

impl fmt::Display for UpdateTrendMaterialization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "UpdateTrendMaterialization({})",
            &self.trend_materialization
        )
    }
}

#[async_trait]
impl Change for UpdateTrendMaterialization {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        match self.trend_materialization.update(client).await {
            Ok(_) => Ok(format!(
                "Updated trend materialization '{}'",
                &self.trend_materialization
            )),
            Err(e) => Err(Error::Runtime(RuntimeError {
                msg: format!(
                    "Error updating trend materialization '{}': {}",
                    &self.trend_materialization, e
                ),
            })),
        }
    }
}
