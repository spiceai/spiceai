use std::{fs::File, io::Write};

use app::AppBuilder;
use datafusion::assert_batches_eq;
use runtime::Runtime;
use spicepod::component::dataset::{acceleration::Acceleration, Dataset};

pub fn make_delta_lake_dataset(path: &str, name: &str, accelerated: bool) -> Dataset {
    let mut dataset = Dataset::new(format!("delta_lake:{path}"), name.to_string());
    if accelerated {
        dataset.acceleration = Some(Acceleration::default());
    }
    dataset
}

struct FileCleanup {
    path: String,
}

impl Drop for FileCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

// Once https://github.com/spiceai/spiceai/issues/2355 is resolved, this test should be re-enabled
#[ignore]
#[tokio::test]
async fn query_delta_lake_with_partitions() -> Result<(), String> {
    let tmp_dir = std::env::temp_dir();
    let path = format!("{}/nation", tmp_dir.display());
    let _hook = FileCleanup { path: path.clone() };
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::create_dir(&path);

    let resp =
        reqwest::get("https://public-data.spiceai.org/delta-lake-nation-with-partitionkey.zip")
            .await
            .expect("request failed");
    let mut out = File::create(format!("{path}/nation.zip")).expect("failed to create file");
    let _ = out.write_all(&resp.bytes().await.expect("failed to read bytes"));
    let _ = std::process::Command::new("unzip")
        .stdin(std::process::Stdio::null())
        .arg(format!("{path}/nation.zip"))
        .arg("-d")
        .arg(&path)
        .spawn()
        .expect("unzip failed");

    let app = AppBuilder::new("delta_lake_partition_test")
        .with_dataset(make_delta_lake_dataset(
            &format!("{path}/nation"),
            "test",
            false,
        ))
        .build();

    let rt = Runtime::builder().with_app(app).build().await;

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err("Timed out waiting for datasets to load".to_string());
        }
        () = rt.load_components() => {}
    }

    let query = "SELECT * from test order by n_nationkey";
    let data = rt
        .datafusion()
        .ctx
        .sql(query)
        .await
        .map_err(|e| format!("query `{query}` to plan: {e}"))?
        .collect()
        .await
        .map_err(|e| format!("query `{query}` to results: {e}"))?;

    let expected_results = [
        "+-------------+----------------+--------------------------------------------------------------------------------------------------------------------+-------------+",
        "| n_nationkey | n_name         | n_comment                                                                                                          | n_regionkey |",
        "+-------------+----------------+--------------------------------------------------------------------------------------------------------------------+-------------+",
        "| 0           | ALGERIA        | furiously regular requests. platelets affix furious                                                                | 0           |",
        "| 1           | ARGENTINA      | instructions wake quickly. final deposits haggle. final, silent theodolites                                        | 1           |",
        "| 2           | BRAZIL         | asymptotes use fluffily quickly bold instructions. slyly bold dependencies sleep carefully pending accounts        | 1           |",
        "| 3           | CANADA         | ss deposits wake across the pending foxes. packages after the carefully bold requests integrate caref              | 1           |",
        "| 4           | EGYPT          | usly ironic, pending foxes. even, special instructions nag. sly, final foxes detect slyly fluffily                 | 4           |",
        "| 5           | ETHIOPIA       | regular requests sleep carefull                                                                                    | 0           |",
        "| 6           | FRANCE         | oggedly. regular packages solve across                                                                             | 3           |",
        "| 7           | GERMANY        | ong the regular requests: blithely silent pinto beans hagg                                                         | 3           |",
        "| 8           | INDIA          | uriously unusual deposits about the slyly final pinto beans could                                                  | 2           |",
        "| 9           | INDONESIA      | d deposits sleep quickly according to the dogged, regular dolphins. special excuses haggle furiously special reque | 2           |",
        "| 10          | IRAN           | furiously idle platelets nag. express asymptotes s                                                                 | 4           |",
        "| 11          | IRAQ           | pendencies; slyly express foxes integrate carefully across the reg                                                 | 4           |",
        "| 12          | JAPAN          |  quickly final packages. furiously i                                                                               | 2           |",
        "| 13          | JORDAN         | the slyly regular ideas. silent Tiresias affix slyly fu                                                            | 4           |",
        "| 14          | KENYA          | lyly special foxes. slyly regular deposits sleep carefully. carefully permanent accounts slee                      | 0           |",
        "| 15          | MOROCCO        | ct blithely: blithely express accounts nag carefully. silent packages haggle carefully abo                         | 0           |",
        "| 16          | MOZAMBIQUE     |  beans after the carefully regular accounts r                                                                      | 0           |",
        "| 17          | PERU           | ly final foxes. blithely ironic accounts haggle. regular foxes about the regular deposits are furiously ir         | 1           |",
        "| 18          | CHINA          | ckly special packages cajole slyly. unusual, unusual theodolites mold furiously. slyly sile                        | 2           |",
        "| 19          | ROMANIA        | sly blithe requests. thinly bold deposits above the blithely regular accounts nag special, final requests. care    | 3           |",
        "| 20          | SAUDI ARABIA   | se slyly across the blithely regular deposits. deposits use carefully regular                                      | 4           |",
        "| 21          | VIETNAM        | lly across the quickly even pinto beans. caref                                                                     | 2           |",
        "| 22          | RUSSIA         | uctions. furiously unusual instructions sleep furiously ironic packages. slyly                                     | 3           |",
        "| 23          | UNITED KINGDOM | carefully pending courts sleep above the ironic, regular theo                                                      | 3           |",
        "| 24          | UNITED STATES  | ly ironic requests along the slyly bold ideas hang after the blithely special notornis; blithely even accounts     | 1           |",
        "+-------------+----------------+--------------------------------------------------------------------------------------------------------------------+-------------+",
    ];
    assert_batches_eq!(expected_results, &data);

    Ok(())
}
