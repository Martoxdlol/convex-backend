//! `#[derive(ConvexUnion)]` — tagged unions.
//!
//! Per `convex-native/IMPLEMENTATION_PLAN.md` step 1.6.3.
//!
//! Given an enum where each variant is a struct-variant with named
//! fields:
//!
//! ```ignore
//! #[derive(ConvexUnion)]
//! #[convex(tag = "type")]
//! pub enum NotificationChannel {
//!     Email { address: String },
//!     Sms { phone: String },
//! }
//! ```
//!
//! the macro emits `ToConvex` / `FromConvex` impls that serialize each
//! variant as a `ConvexObject` with an extra discriminant field whose
//! value is the variant's snake_case name. For example:
//!
//! ```text
//! Email { address: "a@b" }  →  { "type": "email", "address": "a@b" }
//! ```
//!
//! The tag field name is configurable via `#[convex(tag = "...")]` at
//! the enum level. Variants may override their wire name with
//! `#[convex(rename = "...")]`.

use heck::ToSnakeCase;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    parse_macro_input,
    punctuated::Punctuated,
    spanned::Spanned,
    Attribute,
    Data,
    DeriveInput,
    Expr,
    ExprLit,
    Fields,
    Ident,
    Lit,
    Meta,
    Token,
    Variant,
};

pub fn derive_convex_union(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

struct VariantSpec {
    ident: Ident,
    wire: String,
    fields: Vec<(Ident, syn::Type)>,
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
            "#[derive(ConvexUnion)] does not support generic parameters",
        ));
    }

    let Data::Enum(data) = data else {
        return Err(syn::Error::new(
            ident.span(),
            "#[derive(ConvexUnion)] only supports enums",
        ));
    };

    let tag_field = parse_tag(attrs)?.unwrap_or_else(|| "type".to_string());
    let variants: Vec<VariantSpec> = data
        .variants
        .iter()
        .map(parse_variant)
        .collect::<syn::Result<_>>()?;

    // Fail fast on tag collisions — duplicate wire names would make the
    // decoder ambiguous.
    let mut seen: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    for v in &variants {
        if !seen.insert(&v.wire) {
            return Err(syn::Error::new(
                ident.span(),
                format!(
                    "duplicate ConvexUnion tag {:?} — use #[convex(rename = \"...\")] on one of \
                     the variants",
                    v.wire
                ),
            ));
        }
    }

    let to_arms = variants.iter().map(|v| {
        let vi = &v.ident;
        let wire = &v.wire;
        let field_names: Vec<&Ident> = v.fields.iter().map(|(i, _)| i).collect();
        let field_strs: Vec<String> = v.fields.iter().map(|(i, _)| i.to_string()).collect();
        let field_inserts = v.fields.iter().enumerate().map(|(idx, (fid, _))| {
            let fname = &field_strs[idx];
            quote! {
                {
                    let __field: ::convex_native::__private::FieldName = #fname
                        .parse()
                        .map_err(::anyhow::Error::from)?;
                    __map.insert(
                        __field,
                        ::convex_native::ToConvex::to_convex(#fid)?,
                    );
                }
            }
        });
        quote! {
            Self::#vi { #(#field_names),* } => {
                let mut __map: ::std::collections::BTreeMap<
                    ::convex_native::__private::FieldName,
                    ::convex_native::__private::ConvexValue,
                > = ::std::collections::BTreeMap::new();
                {
                    let __field: ::convex_native::__private::FieldName = #tag_field
                        .parse()
                        .map_err(::anyhow::Error::from)?;
                    __map.insert(
                        __field,
                        ::convex_native::__private::ConvexValue::try_from(
                            ::std::string::String::from(#wire),
                        )?,
                    );
                }
                #(#field_inserts)*
                ::convex_native::__private::ConvexValue::Object(
                    ::std::convert::TryFrom::try_from(__map)?,
                )
            }
        }
    });

    let from_arms = variants.iter().map(|v| {
        let vi = &v.ident;
        let wire = &v.wire;
        let field_binds = v.fields.iter().map(|(fid, ty)| {
            let fname = fid.to_string();
            quote! {
                let #fid: #ty = {
                    let __field: ::convex_native::__private::FieldName = #fname
                        .parse()
                        .map_err(::anyhow::Error::from)?;
                    let __v = __map
                        .remove(&__field)
                        .unwrap_or(::convex_native::__private::ConvexValue::Null);
                    <#ty as ::convex_native::FromConvex>::from_convex(__v)?
                };
            }
        });
        let field_names: Vec<&Ident> = v.fields.iter().map(|(i, _)| i).collect();
        quote! {
            #wire => {
                #(#field_binds)*
                ::std::result::Result::Ok(Self::#vi { #(#field_names),* })
            }
        }
    });

    let known_list: Vec<&str> = variants.iter().map(|v| v.wire.as_str()).collect();

    Ok(quote! {
        impl ::convex_native::ToConvex for #ident {
            fn to_convex(self)
                -> ::anyhow::Result<::convex_native::__private::ConvexValue>
            {
                ::std::result::Result::Ok(match self {
                    #(#to_arms,)*
                })
            }
        }

        impl ::convex_native::FromConvex for #ident {
            fn from_convex(
                value: ::convex_native::__private::ConvexValue,
            ) -> ::anyhow::Result<Self> {
                let obj = ::convex_native::__private::ConvexObject::try_from(value)?;
                let mut __map: ::std::collections::BTreeMap<
                    ::convex_native::__private::FieldName,
                    ::convex_native::__private::ConvexValue,
                > = obj.into();
                let __tag_field: ::convex_native::__private::FieldName = #tag_field
                    .parse()
                    .map_err(::anyhow::Error::from)?;
                let __tag_value = __map
                    .remove(&__tag_field)
                    .ok_or_else(|| ::anyhow::anyhow!(
                        "missing discriminant field {:?} on {}",
                        #tag_field,
                        stringify!(#ident),
                    ))?;
                let __tag: ::std::string::String = <::std::string::String
                    as ::convex_native::FromConvex>::from_convex(__tag_value)?;
                match __tag.as_str() {
                    #(#from_arms,)*
                    other => ::std::result::Result::Err(::anyhow::anyhow!(
                        "unknown {} variant {:?}; expected one of {:?}",
                        stringify!(#ident),
                        other,
                        &[ #(#known_list),* ],
                    )),
                }
            }
        }
    })
}

fn parse_tag(attrs: &[Attribute]) -> syn::Result<Option<String>> {
    for attr in attrs {
        if !attr.path().is_ident("convex") {
            continue;
        }
        let nested = attr
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .map_err(|e| syn::Error::new(attr.span(), format!("malformed #[convex] attr: {e}")))?;
        for meta in nested {
            if let Meta::NameValue(nv) = &meta
                && nv.path.is_ident("tag")
            {
                let Expr::Lit(ExprLit {
                    lit: Lit::Str(s), ..
                }) = &nv.value
                else {
                    return Err(syn::Error::new(nv.span(), "tag = must be a string"));
                };
                return Ok(Some(s.value()));
            }
        }
    }
    Ok(None)
}

fn parse_variant(v: &Variant) -> syn::Result<VariantSpec> {
    let Fields::Named(named) = &v.fields else {
        return Err(syn::Error::new(
            v.span(),
            "#[derive(ConvexUnion)] requires struct-style variants with named fields — use \
             #[derive(ConvexEnum)] for unit-only enums",
        ));
    };
    let wire = parse_rename(&v.attrs)?.unwrap_or_else(|| v.ident.to_string().to_snake_case());
    let fields = named
        .named
        .iter()
        .map(|f| {
            let id = f.ident.clone().expect("named field");
            let ty = f.ty.clone();
            (id, ty)
        })
        .collect();
    Ok(VariantSpec {
        ident: v.ident.clone(),
        wire,
        fields,
    })
}

fn parse_rename(attrs: &[Attribute]) -> syn::Result<Option<String>> {
    for attr in attrs {
        if !attr.path().is_ident("convex") {
            continue;
        }
        let nested = attr
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .map_err(|e| syn::Error::new(attr.span(), format!("malformed #[convex] attr: {e}")))?;
        for meta in nested {
            if let Meta::NameValue(nv) = &meta
                && nv.path.is_ident("rename")
            {
                let Expr::Lit(ExprLit {
                    lit: Lit::Str(s), ..
                }) = &nv.value
                else {
                    return Err(syn::Error::new(nv.span(), "rename = must be a string"));
                };
                return Ok(Some(s.value()));
            }
        }
    }
    Ok(None)
}
