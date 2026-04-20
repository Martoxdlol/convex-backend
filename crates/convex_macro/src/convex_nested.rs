//! `#[derive(ConvexNested)]` — embedded objects.
//!
//! For types that live **inside** a document but aren't tables of their own
//! (nested `address: Address` on a `User` doc, say). The derive emits
//! `ToConvex` / `FromConvex` impls that round-trip the struct through a
//! `ConvexObject`, with the same field-name mapping semantics as
//! `#[derive(ConvexDocument)]` — but without the `ConvexDocument` trait
//! impl, `inventory::submit!`, or index/patch machinery.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    parse_macro_input,
    spanned::Spanned,
    Data,
    DataStruct,
    DeriveInput,
    Fields,
    Ident,
};

pub fn derive_convex_nested(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let DeriveInput {
        ident,
        data,
        generics,
        ..
    } = input;

    if !generics.params.is_empty() {
        return Err(syn::Error::new(
            generics.span(),
            "#[derive(ConvexNested)] does not support generic parameters",
        ));
    }

    let data_struct = match data {
        Data::Struct(s) => s,
        _ => {
            return Err(syn::Error::new(
                ident.span(),
                "#[derive(ConvexNested)] only supports structs",
            ));
        },
    };

    let fields = collect_fields(data_struct, ident)?;
    let to_inserts = fields.iter().map(|(f_ident, name, _ty)| {
        quote! {
            {
                let __field: ::convex_native_core::__private::FieldName = #name
                    .parse()
                    .map_err(::anyhow::Error::from)?;
                __map.insert(
                    __field,
                    ::convex_native_core::ToConvex::to_convex(
                        ::std::clone::Clone::clone(&self.#f_ident),
                    )?,
                );
            }
        }
    });
    let from_bindings = fields.iter().map(|(f_ident, name, ty)| {
        quote! {
            let #f_ident: #ty = {
                let __field: ::convex_native_core::__private::FieldName = #name
                    .parse()
                    .map_err(::anyhow::Error::from)?;
                let __v = __map
                    .remove(&__field)
                    .unwrap_or(::convex_native_core::__private::ConvexValue::Null);
                <#ty as ::convex_native_core::FromConvex>::from_convex(__v)?
            };
        }
    });
    let from_idents = fields.iter().map(|(f, ..)| f);

    let schema_entries = fields.iter().map(|(_, name, ty)| {
        let validator_expr = nested_field_validator_expr(ty);
        quote! {
            (::std::string::String::from(#name), #validator_expr)
        }
    });

    Ok(quote! {
        impl ::convex_native_core::ToConvex for #ident {
            fn to_convex(self)
                -> ::anyhow::Result<::convex_native_core::__private::ConvexValue>
            {
                let mut __map: ::std::collections::BTreeMap<
                    ::convex_native_core::__private::FieldName,
                    ::convex_native_core::__private::ConvexValue,
                > = ::std::collections::BTreeMap::new();
                #(#to_inserts)*
                ::std::result::Result::Ok(
                    ::convex_native_core::__private::ConvexValue::Object(
                        ::std::convert::TryFrom::try_from(__map)?,
                    ),
                )
            }
        }

        impl ::convex_native_core::FromConvex for #ident {
            fn from_convex(
                value: ::convex_native_core::__private::ConvexValue,
            ) -> ::anyhow::Result<Self> {
                let obj = ::convex_native_core::__private::ConvexObject::try_from(value)?;
                let mut __map: ::std::collections::BTreeMap<
                    ::convex_native_core::__private::FieldName,
                    ::convex_native_core::__private::ConvexValue,
                > = obj.into();
                #(#from_bindings)*
                ::std::result::Result::Ok(Self {
                    #(#from_idents,)*
                })
            }
        }

        impl ::convex_native_core::ConvexSchema for #ident {
            fn validator() -> ::convex_native_core::__private::Validator {
                let __entries: ::std::vec::Vec<(
                    ::std::string::String,
                    ::convex_native_core::__private::FieldValidator,
                )> = ::std::vec![#(#schema_entries,)*];
                let __obj = ::convex_native_core::__private::build_object_validator(__entries)
                    .expect("build_object_validator");
                ::convex_native_core::__private::Validator::Object(__obj)
            }
        }
    })
}

/// Emit the per-field `FieldValidator` expression used inside
/// `ConvexSchema::validator()` for `#[derive(ConvexNested)]`.
///
/// Mirrors the logic in `convex_document::field_validator_expr` — kept
/// separate to avoid crate-internal dependency graphs between the two
/// modules; the duplication is tiny.
fn nested_field_validator_expr(ty: &syn::Type) -> TokenStream2 {
    if is_vec_u8(ty) {
        return quote! {
            ::convex_native_core::__private::FieldValidator::required_field_type(
                ::convex_native_core::__private::Validator::Bytes,
            )
        };
    }
    if let Some(inner) = option_inner(ty)
        && is_vec_u8(inner)
    {
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

fn is_vec_u8(ty: &syn::Type) -> bool {
    let Some(inner) = vec_inner(ty) else {
        return false;
    };
    matches!(inner, syn::Type::Path(p)
        if p.path.segments.last().is_some_and(|s| s.ident == "u8" && s.arguments.is_empty()))
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
    args.args.iter().find_map(|arg| match arg {
        syn::GenericArgument::Type(t) => Some(t),
        _ => None,
    })
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
    args.args.iter().find_map(|arg| match arg {
        syn::GenericArgument::Type(t) => Some(t),
        _ => None,
    })
}

fn collect_fields(
    ds: &DataStruct,
    struct_ident: &Ident,
) -> syn::Result<Vec<(Ident, String, syn::Type)>> {
    let Fields::Named(named) = &ds.fields else {
        return Err(syn::Error::new(
            struct_ident.span(),
            "#[derive(ConvexNested)] requires named fields",
        ));
    };
    Ok(named
        .named
        .iter()
        .map(|f| {
            let id = f.ident.clone().expect("named field");
            let name = id.to_string();
            (id, name, f.ty.clone())
        })
        .collect())
}
