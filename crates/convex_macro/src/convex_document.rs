//! `#[derive(ConvexDocument)]` — generates the glue required to use a Rust
//! struct as a Convex table row.
//!
//! Emitted for a struct `Foo { .. }` with `#[convex(table = "foos")]`:
//! - `impl ::convex_native_core::ConvexDocument for Foo`
//! - `pub enum FooField` (one variant per struct field, `impl FieldReference`)
//! - `pub enum FooIndex` (one variant per `#[convex(index(...))]`; `Never` if
//!   none — still `impl IndexReference`)
//! - `pub struct FooPatch` (every field wrapped in `Option`, `Default`)
//! - `pub struct FooWithId { pub id: Id<Foo>, pub doc: Foo }`
//! - `inventory::submit!(::convex_native_core::TableRegistration { .. })`

use heck::{
    ToPascalCase,
    ToSnakeCase,
};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{
    format_ident,
    quote,
};
use syn::{
    punctuated::Punctuated,
    spanned::Spanned,
    Attribute,
    Data,
    DataStruct,
    DeriveInput,
    Expr,
    ExprLit,
    Fields,
    Ident,
    Lit,
    Meta,
    Token,
};

struct IndexSpec {
    variant: Ident,
    name: String,
    fields: Vec<String>,
}

struct TextIndexSpec {
    name: String,
    search_field: String,
    filter_fields: Vec<String>,
}

struct VectorIndexSpec {
    name: String,
    vector_field: String,
    dimensions: u32,
    filter_fields: Vec<String>,
}

struct FieldSpec {
    variant: Ident,
    ident: Ident,
    name: String,
    ty: syn::Type,
}

pub fn derive_convex_document(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as DeriveInput);
    match expand(&input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let DeriveInput {
        attrs,
        ident,
        data,
        generics,
        ..
    } = input;

    if !generics.params.is_empty() {
        return Err(syn::Error::new(
            generics.span(),
            "#[derive(ConvexDocument)] does not yet support generic types",
        ));
    }

    let data_struct = match data {
        Data::Struct(s) => s,
        _ => {
            return Err(syn::Error::new(
                ident.span(),
                "#[derive(ConvexDocument)] only supports structs",
            ));
        },
    };

    let table_name = parse_table_attr(attrs)?;
    let indexes = parse_index_attrs(attrs)?;
    let text_indexes = parse_text_index_attrs(attrs)?;
    let vector_indexes = parse_vector_index_attrs(attrs)?;
    let fields = parse_fields(data_struct, ident)?;

    // Compile-time validation: each index field must reference a
    // declared struct field (matching by snake_case name, which is
    // what we use on the wire).
    validate_index_fields(&indexes, &fields)?;
    validate_text_index_fields(&text_indexes, &fields)?;
    validate_vector_index_fields(&vector_indexes, &fields)?;

    let struct_ident = ident;
    let field_enum_ident = format_ident!("{struct_ident}Field");
    let index_enum_ident = format_ident!("{struct_ident}Index");
    let patch_ident = format_ident!("{struct_ident}Patch");
    let with_id_ident = format_ident!("{struct_ident}WithId");

    let field_enum = build_field_enum(&field_enum_ident, &fields);
    let index_enum = build_index_enum(&index_enum_ident, &indexes);
    let patch_struct = build_patch(&patch_ident, struct_ident, &fields);
    let with_id_struct = build_with_id(&with_id_ident, struct_ident);
    let trait_impl = build_trait_impl(
        struct_ident,
        &field_enum_ident,
        &index_enum_ident,
        &patch_ident,
        &table_name,
        &fields,
        &indexes,
        &text_indexes,
        &vector_indexes,
    );
    let registration = build_registration(struct_ident, &table_name);

    let convert_impls = build_convert_impls(struct_ident);
    let schema_impl = build_schema_impl(struct_ident, &fields);

    Ok(quote! {
        #field_enum
        #index_enum
        #patch_struct
        #with_id_struct
        #trait_impl
        #convert_impls
        #schema_impl
        #registration
    })
}

// ── Attribute parsing ─────────────────────────────────────────────

fn parse_table_attr(attrs: &[Attribute]) -> syn::Result<String> {
    let mut table = None;
    for attr in attrs {
        if !attr.path().is_ident("convex") {
            continue;
        }
        // Accept both `#[convex(table = "..")]` (target) and skip
        // `#[convex(index(...))]` (handled by parse_index_attrs).
        let nested = attr
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .map_err(|e| syn::Error::new(attr.span(), format!("malformed #[convex] attr: {e}")))?;
        for meta in nested {
            if let Meta::NameValue(nv) = &meta
                && nv.path.is_ident("table")
            {
                let Expr::Lit(ExprLit {
                    lit: Lit::Str(s), ..
                }) = &nv.value
                else {
                    return Err(syn::Error::new(nv.span(), "table = must be a string"));
                };
                if table.is_some() {
                    return Err(syn::Error::new(nv.span(), "duplicate table = ..."));
                }
                table = Some(s.value());
            }
        }
    }
    table.ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "missing #[convex(table = \"...\")] attribute on struct",
        )
    })
}

fn parse_index_attrs(attrs: &[Attribute]) -> syn::Result<Vec<IndexSpec>> {
    let mut indexes = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("convex") {
            continue;
        }
        let nested = attr
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .map_err(|e| syn::Error::new(attr.span(), format!("malformed #[convex] attr: {e}")))?;
        for meta in nested {
            let Meta::List(list) = &meta else { continue };
            if !list.path.is_ident("index") {
                continue;
            }
            let inner = list
                .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                .map_err(|e| {
                    syn::Error::new(list.span(), format!("malformed index() args: {e}"))
                })?;
            let mut name = None;
            let mut fields: Option<Vec<String>> = None;
            for entry in inner {
                let Meta::NameValue(nv) = entry else {
                    continue;
                };
                if nv.path.is_ident("name") {
                    let Expr::Lit(ExprLit {
                        lit: Lit::Str(s), ..
                    }) = nv.value
                    else {
                        return Err(syn::Error::new(nv.path.span(), "name = must be a string"));
                    };
                    name = Some(s.value());
                } else if nv.path.is_ident("fields") {
                    let Expr::Array(arr) = nv.value else {
                        return Err(syn::Error::new(
                            nv.path.span(),
                            "fields = must be an array of strings",
                        ));
                    };
                    let mut collected = Vec::new();
                    for element in arr.elems {
                        let Expr::Lit(ExprLit {
                            lit: Lit::Str(s), ..
                        }) = element
                        else {
                            return Err(syn::Error::new(
                                arr.bracket_token.span.span(),
                                "fields = must be an array of string literals",
                            ));
                        };
                        collected.push(s.value());
                    }
                    fields = Some(collected);
                }
            }
            let name = name.ok_or_else(|| {
                syn::Error::new(list.span(), "index(...) requires name = \"...\"")
            })?;
            let fields = fields
                .ok_or_else(|| syn::Error::new(list.span(), "index(...) requires fields = [..]"))?;
            let variant = format_ident!("{}", name.to_pascal_case());
            indexes.push(IndexSpec {
                variant,
                name,
                fields,
            });
        }
    }
    Ok(indexes)
}

fn parse_text_index_attrs(attrs: &[Attribute]) -> syn::Result<Vec<TextIndexSpec>> {
    let mut out = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("convex") {
            continue;
        }
        let nested = attr
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .map_err(|e| syn::Error::new(attr.span(), format!("malformed #[convex] attr: {e}")))?;
        for meta in nested {
            let Meta::List(list) = &meta else {
                continue;
            };
            if !list.path.is_ident("text_index") {
                continue;
            }
            let inner = list
                .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                .map_err(|e| {
                    syn::Error::new(list.span(), format!("malformed text_index() args: {e}"))
                })?;
            let mut name = None;
            let mut search_field = None;
            let mut filter_fields: Option<Vec<String>> = None;
            for entry in inner {
                let Meta::NameValue(nv) = entry else {
                    continue;
                };
                if nv.path.is_ident("name") {
                    let Expr::Lit(ExprLit {
                        lit: Lit::Str(s), ..
                    }) = nv.value
                    else {
                        return Err(syn::Error::new(nv.path.span(), "name = must be a string"));
                    };
                    name = Some(s.value());
                } else if nv.path.is_ident("search_field") {
                    let Expr::Lit(ExprLit {
                        lit: Lit::Str(s), ..
                    }) = nv.value
                    else {
                        return Err(syn::Error::new(
                            nv.path.span(),
                            "search_field = must be a string",
                        ));
                    };
                    search_field = Some(s.value());
                } else if nv.path.is_ident("filter_fields") {
                    let Expr::Array(arr) = nv.value else {
                        return Err(syn::Error::new(
                            nv.path.span(),
                            "filter_fields = must be an array of strings",
                        ));
                    };
                    let mut collected = Vec::new();
                    for element in arr.elems {
                        let Expr::Lit(ExprLit {
                            lit: Lit::Str(s), ..
                        }) = element
                        else {
                            return Err(syn::Error::new(
                                arr.bracket_token.span.span(),
                                "filter_fields = must be an array of string literals",
                            ));
                        };
                        collected.push(s.value());
                    }
                    filter_fields = Some(collected);
                }
            }
            let name = name.ok_or_else(|| {
                syn::Error::new(list.span(), "text_index(...) requires name = \"...\"")
            })?;
            let search_field = search_field.ok_or_else(|| {
                syn::Error::new(
                    list.span(),
                    "text_index(...) requires search_field = \"...\"",
                )
            })?;
            out.push(TextIndexSpec {
                name,
                search_field,
                filter_fields: filter_fields.unwrap_or_default(),
            });
        }
    }
    Ok(out)
}

fn parse_vector_index_attrs(attrs: &[Attribute]) -> syn::Result<Vec<VectorIndexSpec>> {
    let mut out = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("convex") {
            continue;
        }
        let nested = attr
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .map_err(|e| syn::Error::new(attr.span(), format!("malformed #[convex] attr: {e}")))?;
        for meta in nested {
            let Meta::List(list) = &meta else {
                continue;
            };
            if !list.path.is_ident("vector_index") {
                continue;
            }
            let inner = list
                .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                .map_err(|e| {
                    syn::Error::new(list.span(), format!("malformed vector_index() args: {e}"))
                })?;
            let mut name = None;
            let mut vector_field = None;
            let mut dimensions = None;
            let mut filter_fields: Option<Vec<String>> = None;
            for entry in inner {
                let Meta::NameValue(nv) = entry else {
                    continue;
                };
                if nv.path.is_ident("name") {
                    let Expr::Lit(ExprLit {
                        lit: Lit::Str(s), ..
                    }) = nv.value
                    else {
                        return Err(syn::Error::new(nv.path.span(), "name = must be a string"));
                    };
                    name = Some(s.value());
                } else if nv.path.is_ident("vector_field") {
                    let Expr::Lit(ExprLit {
                        lit: Lit::Str(s), ..
                    }) = nv.value
                    else {
                        return Err(syn::Error::new(
                            nv.path.span(),
                            "vector_field = must be a string",
                        ));
                    };
                    vector_field = Some(s.value());
                } else if nv.path.is_ident("dimensions") {
                    let Expr::Lit(ExprLit {
                        lit: Lit::Int(n), ..
                    }) = nv.value
                    else {
                        return Err(syn::Error::new(
                            nv.path.span(),
                            "dimensions = must be an integer literal",
                        ));
                    };
                    dimensions = Some(n.base10_parse::<u32>()?);
                } else if nv.path.is_ident("filter_fields") {
                    let Expr::Array(arr) = nv.value else {
                        return Err(syn::Error::new(
                            nv.path.span(),
                            "filter_fields = must be an array of strings",
                        ));
                    };
                    let mut collected = Vec::new();
                    for element in arr.elems {
                        let Expr::Lit(ExprLit {
                            lit: Lit::Str(s), ..
                        }) = element
                        else {
                            return Err(syn::Error::new(
                                arr.bracket_token.span.span(),
                                "filter_fields = must be an array of string literals",
                            ));
                        };
                        collected.push(s.value());
                    }
                    filter_fields = Some(collected);
                }
            }
            let name = name.ok_or_else(|| {
                syn::Error::new(list.span(), "vector_index(...) requires name = \"...\"")
            })?;
            let vector_field = vector_field.ok_or_else(|| {
                syn::Error::new(
                    list.span(),
                    "vector_index(...) requires vector_field = \"...\"",
                )
            })?;
            let dimensions = dimensions.ok_or_else(|| {
                syn::Error::new(list.span(), "vector_index(...) requires dimensions = <int>")
            })?;
            out.push(VectorIndexSpec {
                name,
                vector_field,
                dimensions,
                filter_fields: filter_fields.unwrap_or_default(),
            });
        }
    }
    Ok(out)
}

fn validate_text_index_fields(indexes: &[TextIndexSpec], fields: &[FieldSpec]) -> syn::Result<()> {
    let declared: std::collections::BTreeSet<&str> =
        fields.iter().map(|f| f.name.as_str()).collect();
    for idx in indexes {
        let top = idx
            .search_field
            .split('.')
            .next()
            .unwrap_or(&idx.search_field);
        if !declared.contains(top) {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!(
                    "text_index {:?} references unknown search_field {:?}",
                    idx.name, idx.search_field,
                ),
            ));
        }
        for f in &idx.filter_fields {
            let top = f.split('.').next().unwrap_or(f);
            if !declared.contains(top) {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    format!(
                        "text_index {:?} references unknown filter_field {:?}",
                        idx.name, f,
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn validate_vector_index_fields(
    indexes: &[VectorIndexSpec],
    fields: &[FieldSpec],
) -> syn::Result<()> {
    let declared: std::collections::BTreeSet<&str> =
        fields.iter().map(|f| f.name.as_str()).collect();
    for idx in indexes {
        let top = idx
            .vector_field
            .split('.')
            .next()
            .unwrap_or(&idx.vector_field);
        if !declared.contains(top) {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!(
                    "vector_index {:?} references unknown vector_field {:?}",
                    idx.name, idx.vector_field,
                ),
            ));
        }
        for f in &idx.filter_fields {
            let top = f.split('.').next().unwrap_or(f);
            if !declared.contains(top) {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    format!(
                        "vector_index {:?} references unknown filter_field {:?}",
                        idx.name, f,
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn parse_fields(ds: &DataStruct, struct_ident: &Ident) -> syn::Result<Vec<FieldSpec>> {
    let Fields::Named(named) = &ds.fields else {
        return Err(syn::Error::new(
            struct_ident.span(),
            "#[derive(ConvexDocument)] requires named fields",
        ));
    };
    named
        .named
        .iter()
        .map(|f| {
            let ident = f.ident.clone().expect("named field always has an ident");
            let name = ident.to_string();
            let variant = format_ident!("{}", name.to_pascal_case());
            Ok(FieldSpec {
                variant,
                ident,
                name,
                ty: f.ty.clone(),
            })
        })
        .collect()
}

/// Reject index declarations that reference fields the struct doesn't
/// declare. We look up by exact name (snake_case, as emitted on the
/// wire). Nested field paths like `"profile.name"` are allowed and
/// validated against the top-level segment only — the nested struct
/// is assumed to carry the rest.
fn validate_index_fields(indexes: &[IndexSpec], fields: &[FieldSpec]) -> syn::Result<()> {
    let declared: std::collections::BTreeSet<&str> =
        fields.iter().map(|f| f.name.as_str()).collect();
    for idx in indexes {
        for field_ref in &idx.fields {
            let top = field_ref.split('.').next().unwrap_or(field_ref);
            if !declared.contains(top) {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    format!(
                        "index {:?} references unknown field {:?}. Declared fields: {:?}",
                        idx.name,
                        field_ref,
                        declared.iter().copied().collect::<Vec<_>>(),
                    ),
                ));
            }
        }
    }
    Ok(())
}

// ── Codegen helpers ──────────────────────────────────────────────

fn build_field_enum(enum_ident: &Ident, fields: &[FieldSpec]) -> TokenStream2 {
    let variants = fields.iter().map(|f| &f.variant);
    let match_arms = fields.iter().map(|f| {
        let v = &f.variant;
        let n = &f.name;
        quote! { Self::#v => #n }
    });
    quote! {
        #[allow(dead_code)]
        #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
        pub enum #enum_ident {
            #(#variants,)*
        }

        impl ::convex_native_core::FieldReference for #enum_ident {
            fn as_str(&self) -> &'static str {
                match self {
                    #(#match_arms,)*
                }
            }
        }
    }
}

fn build_index_enum(enum_ident: &Ident, indexes: &[IndexSpec]) -> TokenStream2 {
    if indexes.is_empty() {
        // Uninhabited enum still implements `IndexReference`. Typed query
        // builders reject `with_index(...)` calls on documents without any
        // indexes because there's no constructible `Self`.
        return quote! {
            #[allow(dead_code)]
            #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
            pub enum #enum_ident {}

            impl ::convex_native_core::IndexReference for #enum_ident {
                fn as_str(&self) -> &'static str {
                    match *self {}
                }
                fn fields(&self) -> &'static [&'static str] {
                    match *self {}
                }
            }
        };
    }
    let variants = indexes.iter().map(|i| &i.variant);
    let name_arms = indexes.iter().map(|i| {
        let v = &i.variant;
        let n = &i.name;
        quote! { Self::#v => #n }
    });
    let field_arms = indexes.iter().map(|i| {
        let v = &i.variant;
        let field_literals = i.fields.iter().map(|s| s.as_str());
        quote! { Self::#v => &[#(#field_literals),*] }
    });
    quote! {
        #[allow(dead_code)]
        #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
        pub enum #enum_ident {
            #(#variants,)*
        }

        impl ::convex_native_core::IndexReference for #enum_ident {
            fn as_str(&self) -> &'static str {
                match self {
                    #(#name_arms,)*
                }
            }
            fn fields(&self) -> &'static [&'static str] {
                match self {
                    #(#field_arms,)*
                }
            }
        }
    }
}

fn build_patch(patch_ident: &Ident, struct_ident: &Ident, fields: &[FieldSpec]) -> TokenStream2 {
    let patch_fields = fields.iter().map(|f| {
        let id = &f.ident;
        let ty = &f.ty;
        quote! { pub #id: ::std::option::Option<#ty> }
    });
    let set_statements = fields.iter().map(|f| {
        let id = &f.ident;
        let name = &f.name;
        quote! {
            if let ::std::option::Option::Some(v) = ::std::clone::Clone::clone(&self.#id) {
                let __field: ::convex_native_core::__private::FieldName = #name.parse()
                    .map_err(::anyhow::Error::from)?;
                __map.insert(
                    __field,
                    ::convex_native_core::ToConvex::to_convex(v)?,
                );
            }
        }
    });
    quote! {
        #[derive(Clone, Debug, Default)]
        #[allow(dead_code)]
        pub struct #patch_ident {
            #(#patch_fields,)*
        }

        impl ::convex_native_core::ConvexPatch for #patch_ident {
            type Document = #struct_ident;

            fn to_convex_object(&self)
                -> ::anyhow::Result<::convex_native_core::__private::ConvexObject>
            {
                let mut __map: ::std::collections::BTreeMap<
                    ::convex_native_core::__private::FieldName,
                    ::convex_native_core::__private::ConvexValue,
                > = ::std::collections::BTreeMap::new();
                #(#set_statements)*
                ::std::convert::TryFrom::try_from(__map).map_err(::std::convert::Into::into)
            }
        }
    }
}

fn build_convert_impls(struct_ident: &Ident) -> TokenStream2 {
    quote! {
        impl ::convex_native_core::ToConvex for #struct_ident {
            fn to_convex(self)
                -> ::anyhow::Result<::convex_native_core::__private::ConvexValue>
            {
                <Self as ::convex_native_core::ConvexDocument>::to_convex_object(&self)
                    .map(::convex_native_core::__private::ConvexValue::Object)
            }
        }

        impl ::convex_native_core::FromConvex for #struct_ident {
            fn from_convex(
                value: ::convex_native_core::__private::ConvexValue,
            ) -> ::anyhow::Result<Self> {
                let obj = ::convex_native_core::__private::ConvexObject::try_from(value)?;
                <Self as ::convex_native_core::ConvexDocument>::from_convex_object(obj)
            }
        }
    }
}

fn build_with_id(with_id_ident: &Ident, struct_ident: &Ident) -> TokenStream2 {
    quote! {
        #[derive(Clone, Debug)]
        #[allow(dead_code)]
        pub struct #with_id_ident {
            pub id: ::convex_native_core::Id<#struct_ident>,
            pub doc: #struct_ident,
        }

        impl ::std::ops::Deref for #with_id_ident {
            type Target = #struct_ident;
            fn deref(&self) -> &Self::Target {
                &self.doc
            }
        }
    }
}

fn build_trait_impl(
    struct_ident: &Ident,
    field_enum_ident: &Ident,
    index_enum_ident: &Ident,
    patch_ident: &Ident,
    table_name: &str,
    fields: &[FieldSpec],
    indexes: &[IndexSpec],
    text_indexes: &[TextIndexSpec],
    vector_indexes: &[VectorIndexSpec],
) -> TokenStream2 {
    let field_to_object = fields.iter().map(|f| {
        let id = &f.ident;
        let name = &f.name;
        quote! {
            {
                let __field: ::convex_native_core::__private::FieldName = #name.parse()
                    .map_err(::anyhow::Error::from)?;
                __map.insert(
                    __field,
                    ::convex_native_core::ToConvex::to_convex(::std::clone::Clone::clone(&self.#id))?,
                );
            }
        }
    });

    let field_from_object = fields.iter().map(|f| {
        let id = &f.ident;
        let name = &f.name;
        let ty = &f.ty;
        quote! {
            let #id: #ty = {
                let __field: ::convex_native_core::__private::FieldName = #name.parse()
                    .map_err(::anyhow::Error::from)?;
                let __v = __map
                    .remove(&__field)
                    .unwrap_or(::convex_native_core::__private::ConvexValue::Null);
                <#ty as ::convex_native_core::FromConvex>::from_convex(__v)?
            };
        }
    });
    let field_names = fields.iter().map(|f| &f.ident);

    // Index definitions -> IndexSchema entries.
    let index_entries = indexes.iter().map(|i| {
        let name = &i.name;
        let field_paths = i.fields.iter().map(|f| {
            quote! {
                #f.parse::<::convex_native_core::__private::FieldPath>()?
            }
        });
        quote! {
            {
                let descriptor = ::convex_native_core::__private::IndexDescriptor::new(#name)?;
                let field_paths: ::std::vec::Vec<
                    ::convex_native_core::__private::FieldPath,
                > = vec![#(#field_paths),*];
                let indexed_fields: ::convex_native_core::__private::IndexedFields =
                    ::std::convert::TryFrom::try_from(field_paths)?;
                __indexes.insert(
                    descriptor.clone(),
                    ::convex_native_core::__private::IndexSchema {
                        index_descriptor: descriptor,
                        fields: indexed_fields,
                    },
                );
            }
        }
    });

    // Text index definitions -> TextIndexSchema entries.
    let text_index_entries = text_indexes.iter().map(|t| {
        let name = &t.name;
        let search_field = &t.search_field;
        let filter_fields = t.filter_fields.iter().map(|f| {
            quote! {
                __filter.insert(#f.parse::<::convex_native_core::__private::FieldPath>()?);
            }
        });
        quote! {
            {
                let descriptor = ::convex_native_core::__private::IndexDescriptor::new(#name)?;
                let search_path: ::convex_native_core::__private::FieldPath =
                    #search_field.parse()?;
                let mut __filter: ::std::collections::BTreeSet<
                    ::convex_native_core::__private::FieldPath,
                > = ::std::collections::BTreeSet::new();
                #(#filter_fields)*
                let schema = ::convex_native_core::__private::TextIndexSchema::new(
                    descriptor.clone(),
                    search_path,
                    __filter,
                )?;
                __text_indexes.insert(descriptor, schema);
            }
        }
    });

    // Document shape — (field_name, FieldValidator) tuples emitted
    // once, shared between `table_definition()`'s `document_type` and
    // the `ConvexSchema` impl.
    let document_fields = field_entries_tokens(fields);

    // Vector index definitions -> VectorIndexSchema entries.
    let vector_index_entries = vector_indexes.iter().map(|v| {
        let name = &v.name;
        let vector_field = &v.vector_field;
        let dimensions = v.dimensions;
        let filter_fields = v.filter_fields.iter().map(|f| {
            quote! {
                __filter.insert(#f.parse::<::convex_native_core::__private::FieldPath>()?);
            }
        });
        quote! {
            {
                let descriptor = ::convex_native_core::__private::IndexDescriptor::new(#name)?;
                let vector_path: ::convex_native_core::__private::FieldPath =
                    #vector_field.parse()?;
                let dims: ::convex_native_core::__private::VectorDimensions =
                    ::std::convert::TryFrom::try_from(#dimensions as u32)?;
                let mut __filter: ::std::collections::BTreeSet<
                    ::convex_native_core::__private::FieldPath,
                > = ::std::collections::BTreeSet::new();
                #(#filter_fields)*
                let schema = ::convex_native_core::__private::VectorIndexSchema::new(
                    descriptor.clone(),
                    vector_path,
                    dims,
                    __filter,
                )?;
                __vector_indexes.insert(descriptor, schema);
            }
        }
    });

    quote! {
        impl ::convex_native_core::ConvexDocument for #struct_ident {
            type Field = #field_enum_ident;
            type Index = #index_enum_ident;
            type Patch = #patch_ident;

            fn table_name() -> ::convex_native_core::__private::TableName {
                #table_name
                    .parse()
                    .expect(concat!("invalid table name: ", #table_name))
            }

            fn table_definition() -> ::convex_native_core::__private::TableDefinition {
                let __fn = || -> ::anyhow::Result<::convex_native_core::__private::TableDefinition> {
                    #[allow(unused_mut)]
                    let mut __indexes: ::std::collections::BTreeMap<
                        ::convex_native_core::__private::IndexDescriptor,
                        ::convex_native_core::__private::IndexSchema,
                    > = ::std::collections::BTreeMap::new();
                    #(#index_entries)*
                    #[allow(unused_mut)]
                    let mut __text_indexes: ::std::collections::BTreeMap<
                        ::convex_native_core::__private::IndexDescriptor,
                        ::convex_native_core::__private::TextIndexSchema,
                    > = ::std::collections::BTreeMap::new();
                    #(#text_index_entries)*
                    #[allow(unused_mut)]
                    let mut __vector_indexes: ::std::collections::BTreeMap<
                        ::convex_native_core::__private::IndexDescriptor,
                        ::convex_native_core::__private::VectorIndexSchema,
                    > = ::std::collections::BTreeMap::new();
                    #(#vector_index_entries)*
                    let __field_entries: ::std::vec::Vec<(
                        ::std::string::String,
                        ::convex_native_core::__private::FieldValidator,
                    )> = #document_fields;
                    let __obj_validator =
                        ::convex_native_core::__private::build_object_validator(__field_entries)?;
                    let __doc_schema = ::convex_native_core::__private::DocumentSchema::Union(
                        ::std::vec![__obj_validator],
                    );
                    ::std::result::Result::Ok(::convex_native_core::__private::TableDefinition {
                        table_name: <Self as ::convex_native_core::ConvexDocument>::table_name(),
                        indexes: __indexes,
                        staged_db_indexes: ::std::default::Default::default(),
                        text_indexes: __text_indexes,
                        staged_text_indexes: ::std::default::Default::default(),
                        vector_indexes: __vector_indexes,
                        staged_vector_indexes: ::std::default::Default::default(),
                        document_type: ::std::option::Option::Some(__doc_schema),
                    })
                };
                __fn().expect("building table_definition")
            }

            fn to_convex_object(&self)
                -> ::anyhow::Result<::convex_native_core::__private::ConvexObject>
            {
                let mut __map: ::std::collections::BTreeMap<
                    ::convex_native_core::__private::FieldName,
                    ::convex_native_core::__private::ConvexValue,
                > = ::std::collections::BTreeMap::new();
                #(#field_to_object)*
                ::std::convert::TryFrom::try_from(__map).map_err(::std::convert::Into::into)
            }

            fn from_convex_object(obj: ::convex_native_core::__private::ConvexObject)
                -> ::anyhow::Result<Self>
            {
                let mut __map: ::std::collections::BTreeMap<
                    ::convex_native_core::__private::FieldName,
                    ::convex_native_core::__private::ConvexValue,
                > = obj.into();
                #(#field_from_object)*
                ::std::result::Result::Ok(Self {
                    #(#field_names,)*
                })
            }
        }
    }
}

/// Emit a `Vec<(String, FieldValidator)>` literal listing every
/// struct field. The generated code hands it to
/// `build_object_validator` to produce the `ObjectValidator`.
///
/// Shared between `#[derive(ConvexDocument)]` (for `document_type`) and
/// the `ConvexSchema` impl on the struct itself.
fn field_entries_tokens(fields: &[FieldSpec]) -> TokenStream2 {
    let entries = fields.iter().map(|f| {
        let name = &f.name;
        let validator_expr = field_validator_expr(&f.ty);
        quote! { (::std::string::String::from(#name), #validator_expr) }
    });
    quote! {
        ::std::vec![#(#entries,)*]
    }
}

/// Build the expression that yields a `FieldValidator` for a single
/// struct field at codegen time.
///
/// Special-case: `Vec<u8>` — it round-trips as `Validator::Bytes` (not
/// `Array<Int64>`), and there is no `ConvexSchema` impl for `u8`, so
/// we can't route it through `field_validator_for`. Every other type
/// delegates to the trait.
fn field_validator_expr(ty: &syn::Type) -> TokenStream2 {
    if is_vec_u8(ty) {
        return quote! {
            ::convex_native_core::__private::FieldValidator::required_field_type(
                ::convex_native_core::__private::Validator::Bytes,
            )
        };
    }
    if let Some(inner) = option_of_vec_u8(ty) {
        let _ = inner;
        return quote! {
            ::convex_native_core::__private::FieldValidator::optional_field_type(
                ::convex_native_core::__private::Validator::Union(::std::vec![
                    ::convex_native_core::__private::Validator::Null,
                    ::convex_native_core::__private::Validator::Bytes,
                ]),
            )
        };
    }
    quote! {
        ::convex_native_core::__private::field_validator_for::<#ty>()
    }
}

/// True when `ty` is syntactically `Vec<u8>` (any path prefix).
fn is_vec_u8(ty: &syn::Type) -> bool {
    vec_inner(ty).is_some_and(|inner| is_primitive_path(inner, "u8"))
}

fn option_of_vec_u8(ty: &syn::Type) -> Option<&syn::Type> {
    option_inner(ty).filter(|inner| is_vec_u8(inner))
}

fn vec_inner(ty: &syn::Type) -> Option<&syn::Type> {
    let path = match ty {
        syn::Type::Path(p) => &p.path,
        _ => return None,
    };
    let last = path.segments.last()?;
    if last.ident != "Vec" {
        return None;
    }
    let args = match &last.arguments {
        syn::PathArguments::AngleBracketed(a) => a,
        _ => return None,
    };
    for arg in &args.args {
        if let syn::GenericArgument::Type(t) = arg {
            return Some(t);
        }
    }
    None
}

fn option_inner(ty: &syn::Type) -> Option<&syn::Type> {
    let path = match ty {
        syn::Type::Path(p) => &p.path,
        _ => return None,
    };
    let last = path.segments.last()?;
    if last.ident != "Option" {
        return None;
    }
    let args = match &last.arguments {
        syn::PathArguments::AngleBracketed(a) => a,
        _ => return None,
    };
    for arg in &args.args {
        if let syn::GenericArgument::Type(t) = arg {
            return Some(t);
        }
    }
    None
}

fn is_primitive_path(ty: &syn::Type, name: &str) -> bool {
    match ty {
        syn::Type::Path(p) => p
            .path
            .segments
            .last()
            .is_some_and(|seg| seg.ident == name && seg.arguments.is_empty()),
        _ => false,
    }
}

/// Emit `impl ConvexSchema` for the derived document struct. The
/// schema is `Validator::Object(fields_as_object_validator)` so the
/// struct can itself appear as a nested field.
fn build_schema_impl(struct_ident: &Ident, fields: &[FieldSpec]) -> TokenStream2 {
    let entries = field_entries_tokens(fields);
    quote! {
        impl ::convex_native_core::ConvexSchema for #struct_ident {
            fn validator() -> ::convex_native_core::__private::Validator {
                let __fields: ::std::vec::Vec<(
                    ::std::string::String,
                    ::convex_native_core::__private::FieldValidator,
                )> = #entries;
                let __obj = ::convex_native_core::__private::build_object_validator(__fields)
                    .expect("build_object_validator");
                ::convex_native_core::__private::Validator::Object(__obj)
            }
        }
    }
}

fn build_registration(struct_ident: &Ident, table_name: &str) -> TokenStream2 {
    // Build a static so the `inventory::submit!` emission stays at item
    // scope. Use an identifier derived from the struct name to keep
    // multiple documents in the same module from colliding.
    let hidden_ident = format_ident!(
        "__CONVEX_NATIVE_TABLE_{}",
        struct_ident.to_string().to_snake_case().to_uppercase()
    );
    quote! {
        #[doc(hidden)]
        #[allow(non_upper_case_globals)]
        fn #hidden_ident() -> ::convex_native_core::__private::TableDefinition {
            <#struct_ident as ::convex_native_core::ConvexDocument>::table_definition()
        }

        ::convex_native_core::inventory::submit! {
            ::convex_native_core::TableRegistration {
                table_name: #table_name,
                build: #hidden_ident,
            }
        }
    }
}
