use glob::glob;

use postgres::Client;

use super::attribute_store::{load_attribute_stores, AddAttributeStore, AttributeStore};
use super::change::Change;
use super::trend_store::{load_trend_stores, AddTrendStore, TrendStore, load_trend_store_from_file};
use super::trend_materialization::{TrendMaterialization, AddTrendMaterialization, load_materializations_from, load_materializations};
use super::virtual_entity::{VirtualEntity, load_virtual_entity_from_file, AddVirtualEntity};
use super::relation::{Relation, load_relation_from_file, AddRelation};
use super::error::{Error};

pub struct MinervaInstance {
    pub trend_stores: Vec<TrendStore>,
    pub attribute_stores: Vec<AttributeStore>,
    pub trend_materializations: Vec<TrendMaterialization>,
}

impl MinervaInstance {
    pub fn load_from_db(client: &mut Client) -> Result<MinervaInstance, Error> {
        let attribute_stores = load_attribute_stores(client)?;

        let trend_stores = load_trend_stores(client)?;

        let trend_materializations = load_materializations(client)?;

        Ok(MinervaInstance {
            trend_stores: trend_stores,
            attribute_stores: attribute_stores,
            trend_materializations: trend_materializations,
        })
    }

    pub fn load_from(minerva_instance_root: &str) -> MinervaInstance {
        let trend_stores = load_trend_stores_from(minerva_instance_root).collect();
        let attribute_stores = load_attribute_stores_from(minerva_instance_root).collect();
        let trend_materializations = load_materializations_from(minerva_instance_root).collect();

        MinervaInstance {
            trend_stores: trend_stores,
            attribute_stores: attribute_stores,
            trend_materializations: trend_materializations,
        }
    }

    pub fn initialize_from(client: &mut Client, minerva_instance_root: &str) {
        println!(
            "Initializing Minerva instance from {}",
            minerva_instance_root
        );

        initialize_attribute_stores(client, &minerva_instance_root);

        initialize_trend_stores(client, &minerva_instance_root);

        initialize_virtual_entities(client, &minerva_instance_root);

        initialize_relations(client, &minerva_instance_root);

        initialize_trend_materializations(client, &minerva_instance_root);
    }

    pub fn diff(&self, other: &MinervaInstance) -> Vec<Box<dyn Change>> {
        let mut changes: Vec<Box<dyn Change>> = Vec::new();

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

        // Check for changes in trend materializations
        for other_trend_materialization in &other.trend_materializations {
            match self.trend_materializations.iter().find(|my_trend_materialization| {
                my_trend_materialization.name() == other_trend_materialization.name()
            }) {
                Some(my_trend_materialization) => {
                    changes.append(&mut my_trend_materialization.diff(other_trend_materialization));
                },
                None => {
                    changes.push(Box::new(AddTrendMaterialization::from(other_trend_materialization.clone())))
                }
            }
        }

        changes
    }

    pub fn update(&self, client: &mut Client, other: &MinervaInstance) -> Result<(), Error> {
        let changes = self.diff(other);

        println!("Applying changes:");

        for change in changes {
            println!("{}", change);

            let message = change.apply(client)?;

            println!("{}", &message);
        }

        // Materializations have no diff mechanism yet, so just update
        for materialization in &other.trend_materializations {
            let result = materialization.update(client);

            if let Err(e) = result {
                println!("Erro updating trend materialization: {}", e);
            }
        }

        Ok(())
    }
}

pub fn dump(client: &mut Client) {
    let minerva_instance: MinervaInstance = match MinervaInstance::load_from_db(client) {
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

fn load_attribute_stores_from(minerva_instance_root: &str) -> impl Iterator<Item = AttributeStore> {
    let glob_path = format!("{}/attribute/*.yaml", minerva_instance_root);

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

fn initialize_attribute_stores(client: &mut Client, minerva_instance_root: &str) {
    for attribute_store in load_attribute_stores_from(minerva_instance_root) {
        let change = AddAttributeStore {
            attribute_store: attribute_store,
        };

        let result = change.apply(client);

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

fn load_trend_stores_from(minerva_instance_root: &str) -> impl Iterator<Item = TrendStore> {
    let yaml_paths = glob(
        &format!("{}/trend/*.yaml", minerva_instance_root)
    ).expect("Failed to read glob pattern");

    let json_paths = glob(
        &format!("{}/trend/*.json", minerva_instance_root)
    ).expect("Failed to read glob pattern");

    yaml_paths.chain(json_paths)
        .filter_map(|entry| match entry {
            Ok(path) => {
                match load_trend_store_from_file(&path) {
                    Ok(trend_store) => Some(trend_store),
                    Err(e) => {
                        println!("Error loading trend store definition: {}", e);
                        None
                    }
                }
            }
            Err(_) => None,
        })
}

fn initialize_trend_stores(client: &mut Client, minerva_instance_root: &str) {
    for trend_store in load_trend_stores_from(minerva_instance_root) {
        let change = AddTrendStore {
            trend_store: trend_store,
        };

        let result = change.apply(client);

        match result {
            Ok(message) => {
                println!("{}", message);
            }
            Err(e) => {
                println!("Error creating trend store: {}", e);
            }
        }
    }
}

fn load_virtual_entities_from(minerva_instance_root: &str) -> impl Iterator<Item = VirtualEntity> {
    let sql_paths = glob(
        &format!("{}/virtual-entity/*.sql", minerva_instance_root)
    ).expect("Failed to read glob pattern");

    sql_paths.filter_map(|entry| match entry {
        Ok(path) => {
            match load_virtual_entity_from_file(&path) {
                Ok(virtual_entity) => Some(virtual_entity),
                Err(e) => {
                    println!("Error loading virtual entity definition: {}", e);
                    None
                }
            }
        },
        Err(_) => None,
    })  
}

fn load_relations_from(minerva_instance_root: &str) -> impl Iterator<Item = Relation> {
    let yaml_paths = glob(
        &format!("{}/relation/*.yaml", minerva_instance_root)
    ).expect("Failed to read glob pattern");

    let json_paths = glob(
        &format!("{}/relation/*.json", minerva_instance_root)
    ).expect("Failed to read glob pattern");

    yaml_paths.chain(json_paths)
        .filter_map(|entry| match entry {
            Ok(path) => {
                match load_relation_from_file(&path) {
                    Ok(trend_store) => Some(trend_store),
                    Err(e) => {
                        println!("Error loading relation definition: {}", e);
                        None
                    }
                }
            }
            Err(_) => None,
        })
}

fn initialize_virtual_entities(client: &mut Client, minerva_instance_root: &str) {
    for virtual_entity in load_virtual_entities_from(minerva_instance_root) {
        let change: AddVirtualEntity = AddVirtualEntity::from(virtual_entity);

        match change.apply(client) {
            Ok(message) => println!("{}", message),
            Err(e) => print!("Error creating virtual entity: {}", e),
        }
    }
}

fn initialize_relations(client: &mut Client, minerva_instance_root: &str) {
    for relation in load_relations_from(minerva_instance_root) {
        let change: AddRelation = AddRelation::from(relation);

        match change.apply(client) {
            Ok(message) => println!("{}", message),
            Err(e) => print!("Error creating relation: {}", e),
        }
    }
}

fn initialize_trend_materializations(client: &mut Client, minerva_instance_root: &str) {
    for materialization in load_materializations_from(minerva_instance_root) {
        let change = AddTrendMaterialization::from(materialization);

        match change.apply(client) {
            Ok(message) => println!("{}", message),
            Err(e) => println!("Error creating trend materialization: {}", e),
        }
    }
}