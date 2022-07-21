use std::collections::BTreeMap;
use std::path::Path;

use datacake_data::blocking::BlockingExecutor;
use datacake_data::value::{Document, FixedStructureValue, JsonNumber, JsonValue};

use crate::SegmentWriter;

// TODO: Move to dedicated test utils crate.
pub(crate) fn get_random_doc() -> Document {
    use rand::random;

    let mut big_object = BTreeMap::new();
    big_object.insert("name".to_string(), JsonValue::String("Jeremy".to_string()));
    big_object.insert(
        "age".to_string(),
        JsonValue::Number(JsonNumber::Float(random())),
    );
    big_object.insert(
        "entries".to_string(),
        JsonValue::Array(vec![
            JsonValue::String("Hello, world!".to_string()),
            JsonValue::Number(JsonNumber::Float(random())),
            JsonValue::Bool(random()),
            JsonValue::Number(JsonNumber::PosInt(random())),
            JsonValue::Null,
            JsonValue::Number(JsonNumber::NegInt(random())),
        ]),
    );

    let mut inner = BTreeMap::new();

    if random() {
        inner.insert(
            "data".to_string(),
            FixedStructureValue::MultiU64(vec![12, 1234, 23778235723, 823572875]),
        );
    }

    if random() {
        inner.insert(
            "names".to_string(),
            FixedStructureValue::MultiString(vec![
                "bob".to_string(),
                "jerry".to_string(),
                "julian".to_string(),
            ]),
        );
    }

    if random() {
        inner.insert(
            "json-data".to_string(),
            FixedStructureValue::Dynamic(big_object),
        );
    }

    Document::from(inner)
}

pub(crate) async fn get_populated_segment_writer(
    executor: BlockingExecutor,
    file: &Path,
) -> SegmentWriter {
    let doc = get_random_doc();

    let mut writer = SegmentWriter::create(0, executor, file)
        .await
        .expect("Successful segment creation");

    writer
        .add_document(1, &doc)
        .await
        .expect("Successful doc addition");

    writer
}
