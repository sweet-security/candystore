use candystore::{CandyStore, Config};
use proptest::prelude::*;
use std::collections::BTreeMap;
use tempfile::TempDir;

#[derive(Debug, Clone)]
enum Op {
    Set(String, String),
    Get(String),
    Remove(String),
    CleanShutdown,
    SimulateCrash,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    // Narrow key space to highly encourage collisions (overwrites, deletes of existing keys)
    let key_strat = "[a-d]{1,2}";
    // Variable size payload to occasionally trigger rotation in small stores
    let val_strat = "[a-zA-Z0-9]{0,50}";

    prop_oneof![
        // Weight probabilities so we mostly mutate state, check it, and occasionally restart
        40 => (key_strat, val_strat).prop_map(|(k, v)| Op::Set(k, v)),
        40 => key_strat.prop_map(Op::Get),
        20 => key_strat.prop_map(Op::Remove),
        8 => Just(Op::CleanShutdown),
        2 => Just(Op::SimulateCrash),
    ]
}

proptest! {
    // 200 randomized sequences with up to 2000 operations each for a deeper stress test
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn test_candystore_state_machine(ops in proptest::collection::vec(op_strategy(), 1..2000)) {
        let dir = TempDir::new().unwrap();

        // Small file size so we generate many data files, rotations, and splits within 200 operations
        let config = Config {
            max_data_file_size: 1024 * 4, // 4KB boundaries
            ..Default::default()
        };

        // The authoritative reference state
        let mut oracle = BTreeMap::new();

        let mut db_opt = Some(CandyStore::open(dir.path(), config).unwrap());

        for op in ops {
            match op {
                Op::Set(k, v) => {
                    oracle.insert(k.clone(), v.clone());
                    let db = db_opt.as_ref().unwrap();
                    let _ = db.set(k.as_bytes(), v.as_bytes()).unwrap();
                }
                Op::Get(k) => {
                    let db = db_opt.as_ref().unwrap();
                    let expected = oracle.get(&k);
                    let actual = db.get(k.as_bytes()).unwrap();

                    match expected {
                        Some(v) => assert_eq!(Some(v.as_bytes()), actual.as_deref()),
                        None => assert_eq!(None, actual),
                    }
                }
                Op::Remove(k) => {
                    oracle.remove(&k);
                    let db = db_opt.as_ref().unwrap();
                    let _ = db.remove(k.as_bytes()).unwrap();
                }
                Op::CleanShutdown => {
                    // Close the current DB instance by dropping it, then reopen
                    drop(db_opt.take().unwrap());
                    db_opt = Some(CandyStore::open(dir.path(), config).unwrap());
                }
                Op::SimulateCrash => {
                    // Force a rebuild
                    db_opt.take().unwrap()._abort_for_testing();
                    db_opt = Some(CandyStore::open(dir.path(), config).unwrap());
                }
            }
        }

        // Final verification pass: check the oracle exact matches internal state
        let db = db_opt.as_ref().unwrap();

        // Verify every key that should exist, DOES exist
        for (k, v) in oracle.iter() {
            let actual = db.get(k.as_bytes()).unwrap().expect("Key should exist in store");
            assert_eq!(v.as_bytes(), actual.as_slice());
        }
    }
}
