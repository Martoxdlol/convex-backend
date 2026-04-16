//! `#[derive(ConvexEnum)]` — string-valued Rust enums.
//!
//! Per `convex-native/IMPLEMENTATION_PLAN.md` step 1.6.1.
//!
//! Given an enum like
//!
//! ```ignore
//! #[derive(ConvexEnum)]
//! pub enum Role { Admin, Member, Guest }
//! ```
//!
//! this macro generates `ToConvex` / `FromConvex` impls that map each
//! variant to and from its snake_case string form
//! (`Admin` ↔ `"admin"`, `Member` ↔ `"member"`, …).
//!
//! Variants can override the wire string via
//! `#[convex(rename = "display_name")]`.

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
};

pub fn derive_convex_enum(input: TokenStream) -> TokenStream {
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
            "#[derive(ConvexEnum)] does not support generic parameters",
        ));
    }

    let Data::Enum(data) = data else {
        return Err(syn::Error::new(
            ident.span(),
            "#[derive(ConvexEnum)] only supports enums",
        ));
    };

    // Each variant must be unit-like (no fields). Tuple/struct variants
    // go through `#[derive(ConvexUnion)]` instead.
    let variants = data
        .variants
        .iter()
        .map(|v| {
            if !matches!(v.fields, Fields::Unit) {
                return Err(syn::Error::new(
                    v.span(),
                    "#[derive(ConvexEnum)] requires unit variants — use #[derive(ConvexUnion)] \
                     for variants with data",
                ));
            }
            let wire =
                parse_rename(&v.attrs)?.unwrap_or_else(|| v.ident.to_string().to_snake_case());
            Ok((v.ident.clone(), wire))
        })
        .collect::<syn::Result<Vec<_>>>()?;

    let to_arms = variants.iter().map(|(id, wire)| {
        quote! { #ident::#id => #wire }
    });
    let from_arms = variants.iter().map(|(id, wire)| {
        quote! { #wire => ::std::result::Result::Ok(#ident::#id) }
    });
    let variant_literals: Vec<&str> = variants.iter().map(|(_, w)| w.as_str()).collect();
    let known_list = quote! { &[ #(#variant_literals),* ] };

    Ok(quote! {
        impl ::convex_native::ToConvex for #ident {
            fn to_convex(self)
                -> ::anyhow::Result<::convex_native::__private::ConvexValue>
            {
                let s: &'static str = match self {
                    #(#to_arms,)*
                };
                ::std::result::Result::Ok(
                    ::convex_native::__private::ConvexValue::try_from(
                        ::std::string::String::from(s),
                    )?,
                )
            }
        }

        impl ::convex_native::FromConvex for #ident {
            fn from_convex(
                value: ::convex_native::__private::ConvexValue,
            ) -> ::anyhow::Result<Self> {
                let s: ::std::string::String =
                    <::std::string::String as ::convex_native::FromConvex>::from_convex(value)?;
                match s.as_str() {
                    #(#from_arms,)*
                    other => ::std::result::Result::Err(::anyhow::anyhow!(
                        "unknown {} variant {:?}; expected one of {:?}",
                        stringify!(#ident),
                        other,
                        #known_list,
                    )),
                }
            }
        }
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

// Suppress unused-import warnings when the file is compiled in
// isolation (e.g. `cargo clippy --lib -p convex_macro --no-deps`).
#[allow(dead_code)]
fn _marker(_: &Ident) {}
