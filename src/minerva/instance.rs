use glob::glob;

use postgres::Client;

use super::attribute_store::{load_attribute_stores, AddAttributeStore, AttributeStore};
use super::change::Change;
use super::trend_store::{load_trend_stores, AddTrendStore, TrendStore};

pub struct MinervaInstance {
    pub trend_stores: Vec<TrendStore>,
    pub attribute_stores: Vec<AttributeStore>,
}

impl MinervaInstance {
    pub fn load_from_db(client: &mut Client) -> Result<MinervaInstance, String> {
        let attribute_stores = load_attribute_stores(client);

        let trend_stores = match load_trend_stores(client) {
            Ok(s) => s,
            Err(e) => {
                return Err(format!("Error loading trend stores: {}", e));
            }
        };

        Ok(MinervaInstance {
            trend_stores: trend_stores,
            attribute_stores: attribute_stores,
        })
    }

    pub fn load_from(minerva_instance_root: &str) -> MinervaInstance {
        let trend_stores = load_trend_stores_from(minerva_instance_root).collect();
        let attribute_stores = load_attribute_stores_from(minerva_instance_root).collect();

        MinervaInstance {
            trend_stores: trend_stores,
            attribute_stores: attribute_stores,
        }
    }

    pub fn initialize_from(client: &mut Client, minerva_instance_root: &str) {
        println!(
            "Initializing Minerva instance from {}",
            minerva_instance_root
        );

        initialize_attribute_stores(client, &minerva_instance_root);

        initialize_trend_stores(client, &minerva_instance_root);
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

        changes
    }

    pub fn update(&self, client: &mut Client, other: &MinervaInstance) -> Result<(), String> {
        let changes = self.diff(other);

        println!("Applying changes:");

        for change in changes {
            println!("{}", change);

            let result = change.apply(client);

            match result {
                Ok(_) => {}
                Err(e) => {
                    return Err(e);
                }
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
            Ok(_) => {
                println!("Created attribute store");
            }
            Err(e) => {
                println!("Error creating attribute store: {}", e);
            }
        }
    }
}

fn load_trend_stores_from(minerva_instance_root: &str) -> impl Iterator<Item = TrendStore> {
    let glob_path = format!("{}/trend/*.yaml", minerva_instance_root);

    glob(&glob_path)
        .expect("Failed to read glob pattern")
        .filter_map(|entry| match entry {
            Ok(path) => {
                let f = std::fs::File::open(&path).unwrap();
                let trend_store: TrendStore = serde_yaml::from_reader(f).unwrap();

                Some(trend_store)
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
            Ok(_) => {
                println!("Created trend store");
            }
            Err(e) => {
                println!("Error creating trend store: {}", e);
            }
        }
    }
}
