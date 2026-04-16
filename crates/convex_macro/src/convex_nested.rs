//! `#[derive(ConvexNested)]` — embedded objects.
//!
//! Per `convex-native/IMPLEMENTATION_PLAN.md` step 1.6.2.
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
                let __field: ::convex_native::__private::FieldName = #name
                    .parse()
                    .map_err(::anyhow::Error::from)?;
                __map.insert(
                    __field,
                    ::convex_native::ToConvex::to_convex(
                        ::std::clone::Clone::clone(&self.#f_ident),
                    )?,
                );
            }
        }
    });
    let from_bindings = fields.iter().map(|(f_ident, name, ty)| {
        quote! {
            let #f_ident: #ty = {
                let __field: ::convex_native::__private::FieldName = #name
                    .parse()
                    .map_err(::anyhow::Error::from)?;
                let __v = __map
                    .remove(&__field)
                    .unwrap_or(::convex_native::__private::ConvexValue::Null);
                <#ty as ::convex_native::FromConvex>::from_convex(__v)?
            };
        }
    });
    let from_idents = fields.iter().map(|(f, ..)| f);

    Ok(quote! {
        impl ::convex_native::ToConvex for #ident {
            fn to_convex(self)
                -> ::anyhow::Result<::convex_native::__private::ConvexValue>
            {
                let mut __map: ::std::collections::BTreeMap<
                    ::convex_native::__private::FieldName,
                    ::convex_native::__private::ConvexValue,
                > = ::std::collections::BTreeMap::new();
                #(#to_inserts)*
                ::std::result::Result::Ok(
                    ::convex_native::__private::ConvexValue::Object(
                        ::std::convert::TryFrom::try_from(__map)?,
                    ),
                )
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
                #(#from_bindings)*
                ::std::result::Result::Ok(Self {
                    #(#from_idents,)*
                })
            }
        }
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
