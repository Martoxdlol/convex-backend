//! Tests `#[convex(text_index(...))]` and `#[convex(vector_index(...))]`.
//!
//! Per `IMPLEMENTATION_PLAN.md` step 5.3.

#[allow(unused_imports)]
use convex_native::document::ConvexDocument as _;
use convex_native::ConvexDocument;

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "docs")]
#[convex(index(name = "by_title", fields = ["title"]))]
#[convex(text_index(
    name = "by_body",
    search_field = "body",
    filter_fields = ["category", "author"]
))]
#[convex(vector_index(
    name = "by_embedding",
    vector_field = "embedding",
    dimensions = 1536,
    filter_fields = ["category"]
))]
pub struct Doc {
    pub title: String,
    pub body: String,
    pub category: String,
    pub author: String,
    pub embedding: Vec<f64>,
}

#[test]
fn text_and_vector_indexes_land_in_table_definition() {
    let def = Doc::table_definition();
    // Standard db index.
    assert_eq!(def.indexes.len(), 1);
    assert!(def.indexes.keys().any(|d| d.as_str() == "by_title"));

    // Text index.
    assert_eq!(def.text_indexes.len(), 1);
    let text = def
        .text_indexes
        .iter()
        .find(|(d, _)| d.as_str() == "by_body")
        .expect("by_body text index");
    assert_eq!(text.1.filter_fields.len(), 2);

    // Vector index.
    assert_eq!(def.vector_indexes.len(), 1);
    let vector = def
        .vector_indexes
        .iter()
        .find(|(d, _)| d.as_str() == "by_embedding")
        .expect("by_embedding vector index");
    assert_eq!(u32::from(vector.1.dimension), 1536);
    assert_eq!(vector.1.filter_fields.len(), 1);
}
