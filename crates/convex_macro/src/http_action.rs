//! `#[convex::http_action(method = "...", path = "...")]`.
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
use quote::{
    format_ident,
    quote,
};
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

    let user_fn_ident = sig.ident.clone();
    let handler_fn_ident = format_ident!("__convex_http_handler_{}", user_fn_ident);

    let method = args.method.to_uppercase();
    let path = args.path;
    let name = format!("__http::{method}:{path}");

    let original = quote! {
        #(#attrs)*
        #vis #sig #block
    };

    // Synthesize an erased-signature handler that matches
    // `convex_native_core::registry::HttpHandlerFn`. The user fn
    // already takes `(ctx, request)` returning `Result<HttpResponse>`,
    // so we just box the future and return it.
    let handler_fn = quote! {
        #[allow(non_snake_case)]
        fn #handler_fn_ident<'a>(
            ctx: &'a mut ::convex_native_core::__private::HttpActionCtx<
                'a,
                ::convex_native_core::__private::Rt,
            >,
            request: ::convex_native_core::__private::HttpRequest,
        ) -> ::convex_native_core::__private::HttpHandlerFuture<'a> {
            ::std::boxed::Box::pin(async move {
                #user_fn_ident(ctx, request).await
            })
        }
    };

    // Register the handler under the synthetic dotted name via the
    // existing NativeFunctionRegistration inventory so the
    // distributed `FunctionExecutionService` can dispatch HTTP
    // actions through `NativeFunctionRunner::run_http_action(name, request)`.
    let function_registration = quote! {
        ::convex_native_core::inventory::submit! {
            ::convex_native_core::__private::NativeFunctionRegistration {
                name: #name,
                arg_names: &[],
                handler: ::convex_native_core::__private::HandlerFn::Http(
                    #handler_fn_ident,
                ),
                is_internal: false,
                timeout_ms: 0,
            }
        }
    };

    let registration = quote! {
        ::convex_native_core::inventory::submit! {
            ::convex_native_core::HttpRouteRegistration {
                method: #method,
                path: #path,
                name: #name,
            }
        }
    };

    Ok(quote! {
        #original
        #handler_fn
        #function_registration
        #registration
    })
}
