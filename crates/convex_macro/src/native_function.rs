//! `#[convex::query]` and `#[convex::mutation]` attribute macros.
//!
//! Per `convex-native/IMPLEMENTATION_PLAN.md` steps 1.2.4 / 1.2.5.
//!
//! Given an async fn shaped like
//!
//! ```ignore
//! #[convex::query]
//! async fn list_users(ctx: &mut QueryCtx, limit: i64) -> anyhow::Result<Vec<User>> { .. }
//! ```
//!
//! the macro emits:
//!
//! - The original `list_users` fn (unchanged, still callable from Rust).
//! - A hidden `__list_users_handler` fn matching the erased `QueryHandlerFn` /
//!   `MutationHandlerFn` signature. It deserializes the `ConvexObject` args
//!   into the declared typed parameters, calls the original fn, then serializes
//!   the return value back into a `ConvexValue` and returns it as a boxed
//!   future.
//! - An `inventory::submit!` of a `NativeFunctionRegistration` carrying the
//!   name, argument names, and handler fn pointer.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{
    format_ident,
    quote,
};
use syn::{
    parse_macro_input,
    spanned::Spanned,
    FnArg,
    ItemFn,
    Pat,
    PatType,
    ReturnType,
    Signature,
    Type,
};

#[derive(Copy, Clone)]
pub enum FnKind {
    Query,
    Mutation,
}

impl FnKind {
    fn ctx_type_name(self) -> &'static str {
        match self {
            FnKind::Query => "QueryCtx",
            FnKind::Mutation => "MutationCtx",
        }
    }

    fn handler_variant(self) -> proc_macro2::Ident {
        match self {
            FnKind::Query => format_ident!("Query"),
            FnKind::Mutation => format_ident!("Mutation"),
        }
    }
}

pub fn attr(kind: FnKind, _attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    match expand(kind, input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand(kind: FnKind, input: ItemFn) -> syn::Result<TokenStream2> {
    let ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = input;

    if sig.asyncness.is_none() {
        return Err(syn::Error::new(
            sig.span(),
            format!(
                "#[convex::{}] requires an async fn",
                match kind {
                    FnKind::Query => "query",
                    FnKind::Mutation => "mutation",
                }
            ),
        ));
    }
    if !sig.generics.params.is_empty() {
        return Err(syn::Error::new(
            sig.generics.span(),
            "#[convex::query] / #[convex::mutation] do not support generic parameters",
        ));
    }

    let Signature {
        ident,
        inputs,
        output,
        ..
    } = &sig;

    // First argument must be `ctx: &mut QueryCtx` (or MutationCtx).
    // Subsequent arguments form the typed args.
    let mut iter = inputs.iter();
    let first = iter.next().ok_or_else(|| {
        syn::Error::new(
            sig.span(),
            format!(
                "#[convex::{}] functions must take `ctx: &mut {}` as the first parameter",
                match kind {
                    FnKind::Query => "query",
                    FnKind::Mutation => "mutation",
                },
                kind.ctx_type_name(),
            ),
        )
    })?;
    validate_ctx_param(first, kind)?;

    let args: Vec<&PatType> = iter
        .map(|a| match a {
            FnArg::Typed(pt) => Ok(pt),
            FnArg::Receiver(_) => Err(syn::Error::new(a.span(), "unexpected `self` parameter")),
        })
        .collect::<syn::Result<Vec<_>>>()?;

    // Parameter names (string form) for the registration metadata.
    let arg_name_strs: Vec<String> = args
        .iter()
        .map(|a| {
            if let Pat::Ident(pi) = &*a.pat {
                Ok(pi.ident.to_string())
            } else {
                Err(syn::Error::new(
                    a.span(),
                    "argument pattern must be a plain identifier",
                ))
            }
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let arg_idents: Vec<_> = args
        .iter()
        .filter_map(|a| {
            if let Pat::Ident(pi) = &*a.pat {
                Some(pi.ident.clone())
            } else {
                None
            }
        })
        .collect();
    let _arg_types: Vec<&Type> = args.iter().map(|a| &*a.ty).collect();

    // Return type: must be `anyhow::Result<T>` for some T. We don't
    // verify the error type strictly — any `Result<T, E>` with
    // compatible E will compile when the generated code calls `?`.
    let return_is_ok = matches!(output, ReturnType::Type(_, _));
    if !return_is_ok {
        return Err(syn::Error::new(
            output.span(),
            "function must return `anyhow::Result<T>`",
        ));
    }

    let handler_ident = format_ident!("__convex_native_{ident}_handler");
    let fn_name_str = ident.to_string();

    let arg_deser = args.iter().enumerate().map(|(i, pt)| {
        let ident = if let Pat::Ident(pi) = &*pt.pat {
            &pi.ident
        } else {
            unreachable!()
        };
        let ty = &pt.ty;
        let name = &arg_name_strs[i];
        quote! {
            let #ident: #ty = {
                let __field: ::convex_native::__private::FieldName = #name
                    .parse()
                    .map_err(::anyhow::Error::from)?;
                let __v = __args_map
                    .remove(&__field)
                    .unwrap_or(::convex_native::__private::ConvexValue::Null);
                <#ty as ::convex_native::FromConvex>::from_convex(__v)?
            };
        }
    });

    let handler_variant = kind.handler_variant();
    let ctx_type_name = kind.ctx_type_name();
    let ctx_ty: TokenStream2 = match kind {
        FnKind::Query => quote! {
            &mut ::convex_native::QueryCtx<'_, ::convex_native::Rt>
        },
        FnKind::Mutation => quote! {
            &mut ::convex_native::MutationCtx<'_, ::convex_native::Rt>
        },
    };
    let ctx_param_ty: TokenStream2 = match kind {
        FnKind::Query => quote! {
            &'a mut ::convex_native::QueryCtx<'a, ::convex_native::Rt>
        },
        FnKind::Mutation => quote! {
            &'a mut ::convex_native::MutationCtx<'a, ::convex_native::Rt>
        },
    };
    let _ = ctx_type_name;

    let fn_call = quote! { #ident(ctx, #(#arg_idents),*) };

    // Build the handler fn. It matches the for<'a> fn(...) signature the
    // registry expects.
    let handler_fn = quote! {
        #[doc(hidden)]
        fn #handler_ident<'a>(
            ctx: #ctx_param_ty,
            __args: ::convex_native::__private::ConvexObject,
        ) -> ::std::pin::Pin<
            ::std::boxed::Box<
                dyn ::std::future::Future<
                        Output = ::anyhow::Result<
                            ::convex_native::__private::ConvexValue,
                        >,
                    > + ::std::marker::Send + 'a,
            >,
        > {
            ::std::boxed::Box::pin(async move {
                let mut __args_map: ::std::collections::BTreeMap<
                    ::convex_native::__private::FieldName,
                    ::convex_native::__private::ConvexValue,
                > = __args.into();
                #(#arg_deser)*
                let __ret = #fn_call.await?;
                ::convex_native::ToConvex::to_convex(__ret)
            })
        }
    };

    // Registration submit. The unique static avoids collision between
    // multiple functions in the same module.
    let registration = quote! {
        ::convex_native::inventory::submit! {
            ::convex_native::NativeFunctionRegistration {
                name: #fn_name_str,
                arg_names: &[ #(#arg_name_strs),* ],
                handler: ::convex_native::HandlerFn::#handler_variant(#handler_ident),
            }
        }
    };

    // We emit the original fn unchanged. Developers can still call it
    // directly (e.g. from other native functions).
    let original = quote! {
        #(#attrs)*
        #vis #sig #block
    };
    let _ = ctx_ty;

    Ok(quote! {
        #original
        #handler_fn
        #registration
    })
}

fn validate_ctx_param(first: &FnArg, kind: FnKind) -> syn::Result<()> {
    let FnArg::Typed(pt) = first else {
        return Err(syn::Error::new(
            first.span(),
            "first parameter must be `ctx: &mut QueryCtx` / `&mut MutationCtx`",
        ));
    };
    // Light structural check: argument must be `&mut
    // <something-named-like-QueryCtx-or-MutationCtx>`
    let Type::Reference(r) = &*pt.ty else {
        return Err(syn::Error::new(
            pt.ty.span(),
            format!("first parameter must be &mut {}", kind.ctx_type_name()),
        ));
    };
    if r.mutability.is_none() {
        return Err(syn::Error::new(
            pt.ty.span(),
            format!(
                "first parameter must be &mut {} (missing `mut`)",
                kind.ctx_type_name()
            ),
        ));
    }
    // We deliberately don't enforce the exact path — developers may import
    // `QueryCtx` under an alias. The real type check happens downstream
    // when the handler casts to the registry-expected signature.
    Ok(())
}
