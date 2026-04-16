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

use heck::ToPascalCase;
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
    Action,
}

impl FnKind {
    fn ctx_type_name(self) -> &'static str {
        match self {
            FnKind::Query => "QueryCtx",
            FnKind::Mutation => "MutationCtx",
            FnKind::Action => "ActionCtx",
        }
    }

    fn handler_variant(self) -> proc_macro2::Ident {
        match self {
            FnKind::Query => format_ident!("Query"),
            FnKind::Mutation => format_ident!("Mutation"),
            FnKind::Action => format_ident!("Action"),
        }
    }

    fn label(self) -> &'static str {
        match self {
            FnKind::Query => "query",
            FnKind::Mutation => "mutation",
            FnKind::Action => "action",
        }
    }

    fn marker_trait_path(self) -> TokenStream2 {
        match self {
            FnKind::Query => quote! { ::convex_native::ConvexQueryFunction },
            FnKind::Mutation => quote! { ::convex_native::ConvexMutationFunction },
            FnKind::Action => quote! { ::convex_native::ConvexActionFunction },
        }
    }
}

pub fn attr(kind: FnKind, attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr_ts: proc_macro2::TokenStream = attr.into();
    let flags = match parse_attr_flags(attr_ts) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };
    let input = parse_macro_input!(item as ItemFn);
    match expand(kind, flags, input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

struct AttrFlags {
    is_internal: bool,
    timeout_ms: u64,
}

fn parse_attr_flags(attr: proc_macro2::TokenStream) -> syn::Result<AttrFlags> {
    let mut flags = AttrFlags {
        is_internal: false,
        timeout_ms: 0,
    };
    if attr.is_empty() {
        return Ok(flags);
    }
    use syn::{
        parse::Parser as _,
        punctuated::Punctuated as Punct,
    };
    let parsed: Punct<syn::Meta, syn::Token![,]> =
        Punct::<syn::Meta, syn::Token![,]>::parse_terminated.parse2(attr)?;
    for meta in parsed {
        match &meta {
            syn::Meta::Path(p) if p.is_ident("internal") => {
                flags.is_internal = true;
            },
            syn::Meta::NameValue(nv) if nv.path.is_ident("timeout_ms") => {
                let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Int(n),
                    ..
                }) = &nv.value
                else {
                    return Err(syn::Error::new(
                        nv.value.span(),
                        "timeout_ms = must be an integer literal",
                    ));
                };
                flags.timeout_ms = n.base10_parse::<u64>()?;
            },
            _ => {
                return Err(syn::Error::new(
                    meta.span(),
                    "unsupported modifier — expected `internal` or `timeout_ms = N`",
                ));
            },
        }
    }
    Ok(flags)
}

fn expand(kind: FnKind, flags: AttrFlags, input: ItemFn) -> syn::Result<TokenStream2> {
    let AttrFlags {
        is_internal,
        timeout_ms,
    } = flags;
    let ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = input;

    if sig.asyncness.is_none() {
        return Err(syn::Error::new(
            sig.span(),
            format!("#[convex::{}] requires an async fn", kind.label()),
        ));
    }
    if !sig.generics.params.is_empty() {
        return Err(syn::Error::new(
            sig.generics.span(),
            "#[convex::query] / #[convex::mutation] / #[convex::action] do not support generic \
             parameters",
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
                kind.label(),
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

    // Return type: must be `anyhow::Result<T>` for some T. We extract T
    // from the Result<T, E> wrapper to build the marker trait impl —
    // developers see a compile error if T isn't `ToConvex + FromConvex`.
    let output_inner = extract_result_ok_type(output)?;

    let handler_ident = format_ident!("__convex_native_{ident}_handler");
    let fn_name_str = ident.to_string();
    let marker_ident = format_ident!("{}", ident.to_string().to_pascal_case());
    let args_ident = format_ident!("{}Args", ident.to_string().to_pascal_case());

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
                // Wrap the FromConvex failure with the function + field
                // context so callers see `args::<name>` in the error
                // chain instead of an anonymous type mismatch.
                match <#ty as ::convex_native::FromConvex>::from_convex(__v) {
                    ::std::result::Result::Ok(v) => v,
                    ::std::result::Result::Err(e) => {
                        return ::std::result::Result::Err(e.context(
                            ::std::format!(
                                "decoding arg {:?} of function {:?}",
                                #name,
                                #fn_name_str,
                            ),
                        ));
                    },
                }
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
        FnKind::Action => quote! {
            &mut ::convex_native::ActionCtx<'_, ::convex_native::Rt>
        },
    };
    let ctx_param_ty: TokenStream2 = match kind {
        FnKind::Query => quote! {
            &'a mut ::convex_native::QueryCtx<'a, ::convex_native::Rt>
        },
        FnKind::Mutation => quote! {
            &'a mut ::convex_native::MutationCtx<'a, ::convex_native::Rt>
        },
        FnKind::Action => quote! {
            &'a mut ::convex_native::ActionCtx<'a, ::convex_native::Rt>
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
                is_internal: #is_internal,
                timeout_ms: #timeout_ms,
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

    let marker_trait = kind.marker_trait_path();
    // Build the args struct — one field per parameter (other than ctx).
    // ToConvex / FromConvex impls mirror what ConvexNested would emit.
    let args_field_decls = args.iter().map(|pt| {
        let id = if let Pat::Ident(pi) = &*pt.pat {
            &pi.ident
        } else {
            unreachable!()
        };
        let ty = &pt.ty;
        quote! { pub #id: #ty }
    });
    let args_field_to_inserts = args.iter().enumerate().map(|(i, pt)| {
        let id = if let Pat::Ident(pi) = &*pt.pat {
            &pi.ident
        } else {
            unreachable!()
        };
        let name = &arg_name_strs[i];
        quote! {
            {
                let __field: ::convex_native::__private::FieldName = #name
                    .parse()
                    .map_err(::anyhow::Error::from)?;
                __map.insert(
                    __field,
                    ::convex_native::ToConvex::to_convex(self.#id)?,
                );
            }
        }
    });
    let args_field_from_binds = args.iter().enumerate().map(|(i, pt)| {
        let id = if let Pat::Ident(pi) = &*pt.pat {
            &pi.ident
        } else {
            unreachable!()
        };
        let ty = &pt.ty;
        let name = &arg_name_strs[i];
        quote! {
            let #id: #ty = {
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
    let args_field_from_idents = args.iter().map(|pt| {
        if let Pat::Ident(pi) = &*pt.pat {
            &pi.ident
        } else {
            unreachable!()
        }
    });
    let marker_types = quote! {
        /// Typed args struct — one field per non-ctx parameter.
        #[derive(Debug, Clone)]
        #[allow(non_camel_case_types, dead_code)]
        pub struct #args_ident {
            #(#args_field_decls,)*
        }

        impl ::convex_native::ToConvex for #args_ident {
            fn to_convex(self)
                -> ::anyhow::Result<::convex_native::__private::ConvexValue>
            {
                let mut __map: ::std::collections::BTreeMap<
                    ::convex_native::__private::FieldName,
                    ::convex_native::__private::ConvexValue,
                > = ::std::collections::BTreeMap::new();
                #(#args_field_to_inserts)*
                ::std::result::Result::Ok(
                    ::convex_native::__private::ConvexValue::Object(
                        ::std::convert::TryFrom::try_from(__map)?,
                    ),
                )
            }
        }

        impl ::convex_native::FromConvex for #args_ident {
            fn from_convex(
                value: ::convex_native::__private::ConvexValue,
            ) -> ::anyhow::Result<Self> {
                let obj = ::convex_native::__private::ConvexObject::try_from(value)?;
                let mut __map: ::std::collections::BTreeMap<
                    ::convex_native::__private::FieldName,
                    ::convex_native::__private::ConvexValue,
                > = obj.into();
                #(#args_field_from_binds)*
                ::std::result::Result::Ok(Self { #(#args_field_from_idents,)* })
            }
        }

        /// ZST marker implementing the function-reference trait for
        /// `fn #ident`. Pass to `ctx.run_query` / `run_mutation` /
        /// `run_action` as the first argument.
        #[derive(Copy, Clone, Debug)]
        #[allow(non_camel_case_types, dead_code)]
        pub struct #marker_ident;

        impl #marker_trait for #marker_ident {
            type Args = #args_ident;
            type Output = #output_inner;
            fn name() -> &'static str { #fn_name_str }
        }
    };

    Ok(quote! {
        #original
        #handler_fn
        #marker_types
        #registration
    })
}

/// Pull the `T` out of a `-> Result<T, E>` return type.
///
/// If the user writes `-> anyhow::Result<Foo>`, we recover `Foo`.
/// Unparseable types (raw generic unsugared, ambiguous) fall back to
/// `()` — the resulting marker impl will fail to compile with a
/// reasonable error because `ToConvex for ()` exists but won't match
/// the expected shape.
fn extract_result_ok_type(ret: &ReturnType) -> syn::Result<TokenStream2> {
    let ReturnType::Type(_, ty) = ret else {
        return Err(syn::Error::new(
            ret.span(),
            "function must return `anyhow::Result<T>`",
        ));
    };
    if let Type::Path(p) = ty.as_ref()
        && let Some(seg) = p.path.segments.last()
        && seg.ident == "Result"
        && let syn::PathArguments::AngleBracketed(args) = &seg.arguments
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return Ok(quote! { #inner });
    }
    // Fall back: use the whole type — generated code will fail with a
    // proper error if it's not Result<T, _>.
    Ok(quote! { #ty })
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
