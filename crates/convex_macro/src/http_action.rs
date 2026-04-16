//! `#[convex::http_action(method = "...", path = "...")]`.
//!
//! Per `IMPLEMENTATION_PLAN.md` step 2.7.
//!
//! Expands to:
//!
//! - The original async fn, unchanged.
//! - An `inventory::submit!` of a `HttpRouteRegistration` carrying method,
//!   path, and a synthetic name (`__http::{METHOD}:{path}`).
//!
//! The generated function is expected to take
//! `(ctx: &mut HttpActionCtx<'_, Rt>, req: HttpRequest) ->
//! Result<HttpResponse>`.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    parse_macro_input,
    punctuated::Punctuated,
    spanned::Spanned,
    Expr,
    ExprLit,
    ItemFn,
    Lit,
    Meta,
    Token,
};

pub fn attr(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr_ts: TokenStream2 = attr.into();
    let attr_meta = match syn::parse2::<AttrArgs>(attr_ts) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error().into(),
    };
    let input = parse_macro_input!(item as ItemFn);
    match expand(attr_meta, input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

struct AttrArgs {
    method: String,
    path: String,
}

impl syn::parse::Parse for AttrArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let items = Punctuated::<Meta, Token![,]>::parse_terminated(input)?;
        let mut method = None;
        let mut path = None;
        for meta in items {
            let Meta::NameValue(nv) = meta else {
                continue;
            };
            let Expr::Lit(ExprLit {
                lit: Lit::Str(s), ..
            }) = &nv.value
            else {
                return Err(syn::Error::new(nv.span(), "value must be a string literal"));
            };
            if nv.path.is_ident("method") {
                method = Some(s.value());
            } else if nv.path.is_ident("path") {
                path = Some(s.value());
            }
        }
        let method = method.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "#[convex::http_action] requires method = \"...\"",
            )
        })?;
        let path = path.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "#[convex::http_action] requires path = \"...\"",
            )
        })?;
        Ok(Self { method, path })
    }
}

fn expand(args: AttrArgs, input: ItemFn) -> syn::Result<TokenStream2> {
    let ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = input;

    if sig.asyncness.is_none() {
        return Err(syn::Error::new(
            sig.span(),
            "#[convex::http_action] requires an async fn",
        ));
    }

    let method = args.method.to_uppercase();
    let path = args.path;
    let name = format!("__http::{method}:{path}");

    let original = quote! {
        #(#attrs)*
        #vis #sig #block
    };

    let registration = quote! {
        ::convex_native::inventory::submit! {
            ::convex_native::HttpRouteRegistration {
                method: #method,
                path: #path,
                name: #name,
            }
        }
    };

    Ok(quote! {
        #original
        #registration
    })
}
