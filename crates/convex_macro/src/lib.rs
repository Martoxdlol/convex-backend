use proc_macro::TokenStream;
use quote::quote;
use syn::{
    FnArg,
    GenericArgument,
    ItemFn,
    Pat,
    PathArguments,
    PathSegment,
    ReturnType,
    Signature,
    Type,
};

mod convex_document;
mod convex_enum;
mod convex_nested;
mod convex_union;
mod http_action;
mod native_function;

/// `#[derive(ConvexDocument)]`
///
/// See `convex-native/native-rust-functions.md` for the full design.
/// Generates `ConvexDocument` impl, `XxxField` enum, `XxxIndex` enum,
/// `XxxPatch` struct, `XxxWithId` struct, and registers the table with
/// `inventory` for schema collection.
#[proc_macro_derive(ConvexDocument, attributes(convex))]
pub fn derive_convex_document(input: TokenStream) -> TokenStream {
    convex_document::derive_convex_document(input)
}

/// `#[derive(ConvexEnum)]` — string-valued Rust enums.
///
/// Variants must be unit-like; each maps to its snake_case string form
/// (`Admin` ↔ `"admin"`), optionally overridden with
/// `#[convex(rename = "...")]` on a variant.
#[proc_macro_derive(ConvexEnum, attributes(convex))]
pub fn derive_convex_enum(input: TokenStream) -> TokenStream {
    convex_enum::derive_convex_enum(input)
}

/// `#[derive(ConvexNested)]` — embedded object types.
///
/// Generates `ToConvex` / `FromConvex` impls without the
/// `ConvexDocument` trait or the inventory table registration. Use for
/// types that appear as fields inside a `ConvexDocument` but aren't
/// tables of their own.
#[proc_macro_derive(ConvexNested, attributes(convex))]
pub fn derive_convex_nested(input: TokenStream) -> TokenStream {
    convex_nested::derive_convex_nested(input)
}

/// `#[derive(ConvexUnion)]` — tagged unions.
///
/// Generates `ToConvex` / `FromConvex` impls that serialize enum
/// variants as objects with a discriminant field. The enum attribute
/// `#[convex(tag = "...")]` picks the discriminant field name (default
/// `"type"`). Variants may override their wire name with
/// `#[convex(rename = "...")]`.
#[proc_macro_derive(ConvexUnion, attributes(convex))]
pub fn derive_convex_union(input: TokenStream) -> TokenStream {
    convex_union::derive_convex_union(input)
}

/// `#[convex::query]` — declare a native Convex query.
///
/// Requires an async fn whose first parameter is `ctx: &mut QueryCtx`.
/// Subsequent parameters must be `ToConvex + FromConvex` types.
#[proc_macro_attribute]
pub fn query(attr: TokenStream, item: TokenStream) -> TokenStream {
    native_function::attr(native_function::FnKind::Query, attr, item)
}

/// `#[convex::mutation]` — declare a native Convex mutation.
///
/// Requires an async fn whose first parameter is `ctx: &mut MutationCtx`.
/// Subsequent parameters must be `ToConvex + FromConvex` types.
#[proc_macro_attribute]
pub fn mutation(attr: TokenStream, item: TokenStream) -> TokenStream {
    native_function::attr(native_function::FnKind::Mutation, attr, item)
}

/// `#[convex::action]` — declare a native Convex action.
///
/// Requires an async fn whose first parameter is `ctx: &mut ActionCtx`.
/// Actions run outside the database transaction and can perform
/// external I/O. Subsequent parameters must be `ToConvex + FromConvex`
/// types.
#[proc_macro_attribute]
pub fn action(attr: TokenStream, item: TokenStream) -> TokenStream {
    native_function::attr(native_function::FnKind::Action, attr, item)
}

/// `#[convex::http_action(method = "GET", path = "/api/...")]` —
/// declare an HTTP action handler.
///
/// Expects an async fn with signature
/// `(ctx: &mut HttpActionCtx<'_, Rt>, req: HttpRequest) ->
/// Result<HttpResponse>`. Registers the route in the inventory; lookup happens
/// at runtime via `convex_native::HttpRouter`.
#[proc_macro_attribute]
pub fn http_action(attr: TokenStream, item: TokenStream) -> TokenStream {
    http_action::attr(attr, item)
}

#[proc_macro_attribute]
pub fn instrument_future(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let ItemFn {
        ref attrs,
        ref vis,
        ref sig,
        ref block,
    } = syn::parse(item).unwrap();

    assert!(sig.constness.is_none(), "Can't instrument const fn");
    assert!(sig.asyncness.is_some(), "Can only instrument async fn");
    assert!(sig.unsafety.is_none(), "Can't instrument unsafe fn");
    assert!(sig.abi.is_none(), "Can't instrument fn with explicit ABI");
    assert!(
        sig.variadic.is_none(),
        "Can't instrument fn with variadic arguments"
    );

    let Signature {
        ident,
        generics,
        inputs,
        output,
        ..
    } = sig;

    let r#gen = quote! {
        #(#attrs)*
        #vis async fn #ident #generics (#inputs) #output {
            ::common::run_instrumented!(
                #ident,
                #block
            )
        }
    };
    r#gen.into()
}

/// Use as #[convex_macro::v8_op] to annotate "ops" (Rust code callable from
/// JavaScript that is shipped with backend).
/// Must be used within the `isolate` crate.
///
/// Types:
/// Arguments and return value can be anything that implements
/// `serde::Serialize`. TODO: support &str and &mut [u8].
///
/// Note: Option::None in return values is encoded as `null` (not
/// undefined), while both `null` and `undefined` (and missing positional)
/// arguments become None.
///
/// The function should be called as `op_name(provider, args, rt)?`.
#[proc_macro_attribute]
pub fn v8_op(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let ItemFn {
        ref attrs,
        ref vis,
        ref sig,
        ref block,
    } = syn::parse(item).unwrap();

    assert!(sig.constness.is_none(), "const fn cannot be op");
    assert!(sig.asyncness.is_none(), "async fn cannot be op");
    assert!(sig.unsafety.is_none(), "unsafe fn cannot be op");
    assert!(sig.abi.is_none(), "fn with explicit ABI cannot be op");
    assert!(
        sig.variadic.is_none(),
        "fn with variadic arguments cannot be op"
    );

    let Signature {
        ident,
        generics,
        inputs,
        output,
        ..
    } = sig;

    let Some(FnArg::Typed(first_pat_type)) = inputs.first() else {
        panic!("op should take a first argument for its op provider");
    };
    let Pat::Ident(first_pat_ident) = &*first_pat_type.pat else {
        panic!("op's first argument should be a plain identifier");
    };
    let provider_ident = &first_pat_ident.ident;

    let arg_pats: Vec<_> = inputs
        .iter()
        .skip(1)
        .map(|input| {
            let FnArg::Typed(pat) = input else {
                panic!("input must be typed")
            };
            &pat.pat
        })
        .collect();
    let arg_parsing: Vec<_> = inputs
        .iter()
        .enumerate()
        .skip(1)
        .map(|(idx, input)| {
            let idx = idx as i32;
            let arg_info = format!("{ident} arg{idx}");
            let FnArg::Typed(pat) = input else {
                panic!("input must be typed")
            };
            let ty = &pat.ty;
            // NOTE: deno has special case when pat.ty is &mut [u8].
            // While that would make some ops more efficient, it also makes them
            // unsafe because it's hard to prove that the same buffer isn't
            // being mutated from multiple ops in parallel or multiple arguments
            // on the same op.
            //
            // Forego all special casing and just use serde_v8.
            quote! {
                {
                    let __raw_arg = __args.get(#idx);
                    use ::anyhow::Context as _;
                    <#ty as crate::convert_v8::FromV8>::from_v8(__scope, __raw_arg)
                        .context(#arg_info)?
                }
            }
        })
        .collect();

    let ReturnType::Type(_, return_type) = output else {
        panic!("op needs return type");
    };
    let Type::Path(rtype_path) = &**return_type else {
        panic!("op must return anyhow::Result<...>")
    };
    let PathSegment {
        ident: retval_type,
        arguments: retval_arguments,
    } = rtype_path.path.segments.last().unwrap();
    assert_eq!(&retval_type.to_string(), "Result");
    let PathArguments::AngleBracketed(retval_arguments) = retval_arguments else {
        panic!("op must return anyhow::Result<...>")
    };
    let GenericArgument::Type(_retval_type) = retval_arguments
        .args
        .last()
        .expect("op must return anyhow::Result<...>")
    else {
        panic!("op must return anyhow::Result<...>");
    };

    let r#gen = quote! {
        #(#attrs)*
        #vis fn #ident #generics (
            #first_pat_type,
            __args: ::deno_core::v8::FunctionCallbackArguments,
            mut __rv: ::deno_core::v8::ReturnValue,
        ) -> ::anyhow::Result<()> {
            #[allow(clippy::unused_unit)]
            let ( #(#arg_pats,)*) = {
                let mut __scope = OpProvider::scope(#provider_ident);
                ::deno_core::v8::scope!(let __scope, &mut __scope);
                (#(#arg_parsing,)*)
            };
            let __result_v = (|| #output { #block })()?;
            {
                let mut __scope = OpProvider::scope(#provider_ident);
                ::deno_core::v8::scope!(let __scope, &mut __scope);
                let __value_v8 = crate::convert_v8::ToV8::to_v8(__result_v, __scope)?;
                __rv.set(__value_v8);
            }
            Ok(())
        }
    };
    r#gen.into()
}
