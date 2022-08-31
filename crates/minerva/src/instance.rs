use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use glob::glob;

use tokio_postgres::Client;

use super::attribute_store::{load_attribute_stores, AddAttributeStore, AttributeStore};
use super::change::Change;
use super::error::Error;
use super::notification_store::{
    load_notification_stores, AddNotificationStore, NotificationStore,
};
use super::relation::{load_relation_from_file, AddRelation, Relation};
use super::trend_materialization::{
    load_materializations, load_materializations_from, AddTrendMaterialization,
    TrendMaterialization,
};
use super::trend_store::{
    load_trend_store_from_file, load_trend_stores, AddTrendStore, TrendStore,
};
use super::virtual_entity::{load_virtual_entity_from_file, AddVirtualEntity, VirtualEntity};

pub struct MinervaInstance {
    pub instance_root: Option<PathBuf>,
    pub trend_stores: Vec<TrendStore>,
    pub attribute_stores: Vec<AttributeStore>,
    pub notification_stores: Vec<NotificationStore>,
    pub virtual_entities: Vec<VirtualEntity>,
    pub relations: Vec<Relation>,
    pub trend_materializations: Vec<TrendMaterialization>,
}

impl MinervaInstance {
    pub async fn load_from_db(client: &mut Client) -> Result<MinervaInstance, Error> {
        let attribute_stores = load_attribute_stores(client).await?;

        let trend_stores = load_trend_stores(client).await?;

        let notification_stores = load_notification_stores(client).await?;

        //let virtual_entities = load_virtual_entities(client)?;

        let virtual_entities = Vec::new();

        //let relations = load_relations(client)?;

        let relations = Vec::new();

        let trend_materializations = load_materializations(client).await?;

        Ok(MinervaInstance {
            instance_root: None,
            trend_stores,
            attribute_stores,
            notification_stores,
            virtual_entities,
            relations,
            trend_materializations,
        })
    }

    pub fn load_from(minerva_instance_root: &Path) -> MinervaInstance {
        let trend_stores = load_trend_stores_from(minerva_instance_root).collect();
        let notification_stores = load_notification_stores_from(minerva_instance_root).collect();
        let attribute_stores = load_attribute_stores_from(minerva_instance_root).collect();
        let virtual_entities = load_virtual_entities_from(minerva_instance_root).collect();
        let relations = load_relations_from(minerva_instance_root).collect();
        let trend_materializations = load_materializations_from(minerva_instance_root).collect();

        MinervaInstance {
            instance_root: Some(PathBuf::from(minerva_instance_root)),
            trend_stores,
            attribute_stores,
            notification_stores,
            virtual_entities,
            relations,
            trend_materializations,
        }
    }

    pub async fn initialize(&self, client: &mut Client) {
        if let Some(instance_root) = &self.instance_root {
            initialize_custom(
                client,
                &format!("{}/custom/pre-init/*/*", instance_root.to_string_lossy()),
            )
            .await
        }

        initialize_attribute_stores(client, &self.attribute_stores).await;

        initialize_trend_stores(client, &self.trend_stores).await;

        initialize_notification_stores(client, &self.notification_stores).await;

        initialize_virtual_entities(client, &self.virtual_entities).await;

        initialize_relations(client, &self.relations).await;

        initialize_trend_materializations(client, &self.trend_materializations).await;

        if let Some(instance_root) = &self.instance_root {
            initialize_custom(
                client,
                &format!("{}/custom/post-init/*/*", instance_root.to_string_lossy()),
            )
            .await
        }
    }

    pub fn diff<'a>(&self, other: &MinervaInstance) -> Vec<Box<dyn Change + Send>> {
        let mut changes: Vec<Box<dyn Change + Send>> = Vec::new();

        // Check for changes in trend stores
        for other_trend_store in &other.trend_stores {
            match self.trend_stores.iter().find(|my_trend_store| {
                my_trend_store.data_source == other_trend_store.data_source
                    && my_trend_store.entity_type == other_trend_store.entity_type
                    && my_trend_store.granularity == other_trend_store.granularity
            }) {
                Some(my_trend_store) => {
                    changes.append(&mut my_trend_store.diff(other_trend_store));
                }
                None => {
                    changes.push(Box::new(AddTrendStore {
                        trend_store: other_trend_store.clone(),
                    }));
                }
            }
        }

        // Check for changes in attribute stores
        for other_attribute_store in &other.attribute_stores {
            match self.attribute_stores.iter().find(|my_attribute_store| {
                my_attribute_store.data_source == other_attribute_store.data_source
                    && my_attribute_store.entity_type == other_attribute_store.entity_type
            }) {
                Some(my_attribute_store) => {
                    changes.append(&mut my_attribute_store.diff(other_attribute_store));
                }
                None => {
                    changes.push(Box::new(AddAttributeStore {
                        attribute_store: other_attribute_store.clone(),
                    }));
                }
            }
        }

        // Check for changes in notification stores
        for other_notification_store in &other.notification_stores {
            match self.notification_stores.iter().find(|my_attribute_store| {
                my_attribute_store.data_source == other_notification_store.data_source
            }) {
                Some(my_attribute_store) => {
                    changes.append(&mut my_attribute_store.diff(other_notification_store));
                }
                None => {
                    changes.push(Box::new(AddNotificationStore {
                        notification_store: other_notification_store.clone(),
                    }));
                }
            }
        }

        // Check for changes in trend materializations
        for other_trend_materialization in &other.trend_materializations {
            match self
                .trend_materializations
                .iter()
                .find(|my_trend_materialization| {
                    my_trend_materialization.name() == other_trend_materialization.name()
                }) {
                Some(my_trend_materialization) => {
                    changes.append(&mut my_trend_materialization.diff(other_trend_materialization));
                }
                None => changes.push(Box::new(AddTrendMaterialization::from(
                    other_trend_materialization.clone(),
                ))),
            }
        }

        changes
    }

    pub async fn update(&self, client: &mut Client, other: &MinervaInstance) -> Result<(), Error> {
        let changes = self.diff(other);

        println!("Applying changes:");

        for change in changes {
            println!("* {}", change);

            match change.apply(client).await {
                Ok(message) => println!("> {}", &message),
                Err(err) => println!("! Error applying change: {}", &err),
            }
        }

        // Materializations have no diff mechanism yet, so just update
        for materialization in &other.trend_materializations {
            let result = materialization.update(client).await;

            if let Err(e) = result {
                println!("Erro updating trend materialization: {}", e);
            }
        }

        Ok(())
    }
}

pub async fn dump(client: &mut Client) {
    let minerva_instance: MinervaInstance = match MinervaInstance::load_from_db(client).await {
        Ok(i) => i,
        Err(e) => {
            println!("Error loading instance from database: {}", e);
            return;
        }
    };

    for attribute_store in minerva_instance.attribute_stores {
        println!("{:?}", &attribute_store);
    }

    for trend_store in minerva_instance.trend_stores {
        println!("{:?}", &trend_store);
    }
}

fn load_attribute_stores_from(
    minerva_instance_root: &Path,
) -> impl Iterator<Item = AttributeStore> {
    let glob_path = format!(
        "{}/attribute/*.yaml",
        minerva_instance_root.to_string_lossy()
    );

    glob(&glob_path)
        .expect("Failed to read glob pattern")
        .filter_map(|entry| match entry {
            Ok(path) => {
                let f = std::fs::File::open(&path).unwrap();
                let attribute_store: AttributeStore = serde_yaml::from_reader(f).unwrap();

                Some(attribute_store)
            }
            Err(_) => None,
        })
}

async fn initialize_attribute_stores(client: &mut Client, attribute_stores: &Vec<AttributeStore>) {
    for attribute_store in attribute_stores {
        let change = AddAttributeStore {
            attribute_store: attribute_store.clone(),
        };

        let result = change.apply(client).await;

        match result {
            Ok(message) => {
                println!("{}", message);
            }
            Err(e) => {
                println!("Error creating attribute store: {}", e);
            }
        }
    }
}

fn load_notification_stores_from(
    minerva_instance_root: &Path,
) -> impl Iterator<Item = NotificationStore> {
    let glob_path = format!(
        "{}/notification/*.yaml",
        minerva_instance_root.to_string_lossy()
    );

    glob(&glob_path)
        .expect("Failed to read glob pattern")
        .filter_map(|entry| match entry {
            Ok(path) => {
                let f = std::fs::File::open(&path).unwrap();
                let notification_store: NotificationStore = serde_yaml::from_reader(f).unwrap();

                Some(notification_store)
            }
            Err(_) => None,
        })
}

async fn initialize_notification_stores(
    client: &mut Client,
    notification_stores: &Vec<NotificationStore>,
) {
    for notification_store in notification_stores {
        let change = AddNotificationStore {
            notification_store: notification_store.clone(),
        };

        match change.apply(client).await {
            Ok(message) => {
                println!("{}", message);
            }
            Err(e) => {
                println!("Error creating notification store: {}", e);
            }
        }
    }
}

fn load_trend_stores_from(minerva_instance_root: &Path) -> impl Iterator<Item = TrendStore> {
    let yaml_paths = glob(&format!(
        "{}/trend/*.yaml",
        minerva_instance_root.to_string_lossy()
    ))
    .expect("Failed to read glob pattern");

    let json_paths = glob(&format!(
        "{}/trend/*.json",
        minerva_instance_root.to_string_lossy()
    ))
    .expect("Failed to read glob pattern");

    yaml_paths
        .chain(json_paths)
        .filter_map(|entry| match entry {
            Ok(path) => match load_trend_store_from_file(&path) {
                Ok(trend_store) => Some(trend_store),
                Err(e) => {
                    println!("Error loading trend store definition: {}", e);
                    None
                }
            },
            Err(_) => None,
        })
}

async fn initialize_trend_stores(client: &mut Client, trend_stores: &Vec<TrendStore>) {
    for trend_store in trend_stores {
        let change = AddTrendStore {
            trend_store: trend_store.clone(),
        };

        match change.apply(client).await {
            Ok(message) => {
                println!("{}", message);
            }
            Err(e) => {
                println!("Error creating trend store: {}", e);
            }
        }
    }
}

fn load_virtual_entities_from(minerva_instance_root: &Path) -> impl Iterator<Item = VirtualEntity> {
    let sql_paths = glob(&format!(
        "{}/virtual-entity/*.sql",
        minerva_instance_root.to_string_lossy()
    ))
    .expect("Failed to read glob pattern");

    sql_paths.filter_map(|entry| match entry {
        Ok(path) => match load_virtual_entity_from_file(&path) {
            Ok(virtual_entity) => Some(virtual_entity),
            Err(e) => {
                println!("Error loading virtual entity definition: {}", e);
                None
            }
        },
        Err(_) => None,
    })
}

fn load_relations_from(minerva_instance_root: &Path) -> impl Iterator<Item = Relation> {
    let yaml_paths = glob(&format!(
        "{}/relation/*.yaml",
        minerva_instance_root.to_string_lossy()
    ))
    .expect("Failed to read glob pattern");

    let json_paths = glob(&format!(
        "{}/relation/*.json",
        minerva_instance_root.to_string_lossy()
    ))
    .expect("Failed to read glob pattern");

    yaml_paths
        .chain(json_paths)
        .filter_map(|entry| match entry {
            Ok(path) => match load_relation_from_file(&path) {
                Ok(trend_store) => Some(trend_store),
                Err(e) => {
                    println!("Error loading relation definition: {}", e);
                    None
                }
            },
            Err(_) => None,
        })
}

async fn initialize_virtual_entities(client: &mut Client, virtual_entities: &Vec<VirtualEntity>) {
    for virtual_entity in virtual_entities {
        let change: AddVirtualEntity = AddVirtualEntity::from(virtual_entity.clone());

        match change.apply(client).await {
            Ok(message) => println!("{}", message),
            Err(e) => print!("Error creating virtual entity: {}", e),
        }
    }
}

async fn initialize_relations(client: &mut Client, relations: &Vec<Relation>) {
    for relation in relations {
        let change: AddRelation = AddRelation::from(relation.clone());

        match change.apply(client).await {
            Ok(message) => println!("{}", message),
            Err(e) => print!("Error creating relation: {}", e),
        }
    }
}

async fn initialize_trend_materializations(
    client: &mut Client,
    trend_materializations: &Vec<TrendMaterialization>,
) {
    for materialization in trend_materializations {
        let change = AddTrendMaterialization::from(materialization.clone());

        match change.apply(client).await {
            Ok(message) => println!("{}", message),
            Err(e) => println!("Error creating trend materialization: {}", e),
        }
    }
}

async fn load_sql<'a>(client: &'a mut Client, path: &PathBuf) -> Result<(), String> {
    let mut f = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(e) => {
            return Err(format!(
                "Could not open sql file '{}': {}",
                &path.to_string_lossy(),
                e
            ))
        }
    };

    let mut sql = String::new();

    if let Err(e) = f.read_to_string(&mut sql) {
        return Err(format!(
            "Could not read virtual entity definition file: {}",
            e
        ));
    }

    if let Err(e) = client.batch_execute(&sql).await {
        return Err(format!("Error creating relation materialized view: {}", e));
    }

    Ok(())
}

fn load_psql<'a>(path: &PathBuf) -> Result<String, String> {
    let cmd = Command::new("psql").arg("-f").arg(path).output();

    match cmd {
        Ok(output) => match output.status.success() {
            true => {
                let stdout = std::str::from_utf8(&output.stderr).unwrap();

                return Ok(stdout.into());
            }
            false => {
                let stderr = std::str::from_utf8(&output.stderr).unwrap();

                return Err(stderr.into());
            }
        },
        Err(e) => return Err(format!("Could not run psql command: {}", e)),
    }
}

fn execute_custom<'a>(path: &PathBuf) -> Result<String, String> {
    let cmd = Command::new(path).output();

    match cmd {
        Ok(output) => match output.status.success() {
            true => {
                let stdout = std::str::from_utf8(&output.stderr).unwrap();

                return Ok(stdout.into());
            }
            false => {
                let stderr = std::str::from_utf8(&output.stderr).unwrap();

                return Err(stderr.into());
            }
        },
        Err(e) => return Err(format!("Could not run command: {}", e)),
    }
}

async fn initialize_custom<'a>(client: &'a mut Client, glob_pattern: &'a str) {
    let paths = glob(glob_pattern).expect("Failed to read glob pattern");

    for entry in paths {
        match entry {
            Ok(path) => {
                if path.is_dir() {
                    println!("Directory '{}'", &path.to_string_lossy());
                } else {
                    match path.extension() {
                        Some(ext) => {
                            let ext_str = ext.to_str().unwrap_or("");
                            match ext_str {
                                "sql" => match load_sql(client, &path).await {
                                    Ok(_) => println!("Executed sql '{}'", &path.to_string_lossy()),
                                    Err(e) => {
                                        println!(
                                            "Error executing sql '{}': {}",
                                            &path.to_string_lossy(),
                                            e
                                        )
                                    }
                                },
                                "psql" => match load_psql(&path) {
                                    Ok(msg) => {
                                        println!(
                                            "Executed '{}' with psql: {}",
                                            &path.to_string_lossy(),
                                            msg
                                        )
                                    }
                                    Err(e) => {
                                        println!(
                                            "Error executing '{}' with psql: {}",
                                            &path.to_string_lossy(),
                                            e
                                        )
                                    }
                                },
                                _ => {
                                    let metadata_result = path.metadata();
    
                                    match metadata_result {
                                        Err(e) => {
                                            println!(
                                                "Error retrieving meta data for '{}': {}",
                                                &path.to_string_lossy(),
                                                e
                                            )
                                        }
                                        Ok(metadata) => {
                                            if (metadata.permissions().mode() & 0o111) != 0 {
                                                match execute_custom(&path) {
                                                    Ok(msg) => {
                                                        println!(
                                                            "Executed '{}': {}",
                                                            &path.to_string_lossy(),
                                                            msg
                                                        )
                                                    }
                                                    Err(e) => {
                                                        println!(
                                                            "Error executing '{}': {}",
                                                            &path.to_string_lossy(),
                                                            e
                                                        )
                                                    }
                                                }
                                            } else {
                                                println!(
                                                    "Skipping non-executable file '{}'",
                                                    path.to_string_lossy()
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        None => {
                            println!(
                                "A file without an extension should not have matched the glob patterns",
                            )
                        }
                    }
                }
            }
            Err(_) => println!("No path"),
        }
    }
}
