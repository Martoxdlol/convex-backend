//! `#[convex::cron(name = "...", schedule = "...", target =
//! "query|mutation|action")]`.
//!
//! The macro is applied to a module-level item but doesn't transform
//! it — it only emits an `inventory::submit!(CronRegistration)`
//! adjacent to the item. The target string must match a function
//! already (or later) registered via `#[convex::query]` /
//! `#[convex::mutation]` / `#[convex::action]`.
//!
//! Usage:
//!
//! ```ignore
//! #[convex::cron(name = "nightly_cleanup", schedule = "0 3 * * *", target = "cleanup")]
//! fn nightly_cleanup_cron() {}
//! ```
//!
//! The placeholder fn body exists only to give `#[convex::cron]`
//! something to attach to; the name of that fn doesn't matter.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    parse_macro_input,
    punctuated::Punctuated,
    spanned::Spanned,
    Expr,
    ExprLit,
    Item,
    Lit,
    Meta,
    Token,
};

struct AttrArgs {
    name: String,
    schedule: String,
    target: String,
    target_kind: String,
}

impl syn::parse::Parse for AttrArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let items = Punctuated::<Meta, Token![,]>::parse_terminated(input)?;
        let mut name = None;
        let mut schedule = None;
        let mut target = None;
        let mut target_kind = None;
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
            if nv.path.is_ident("name") {
                name = Some(s.value());
            } else if nv.path.is_ident("schedule") {
                schedule = Some(s.value());
            } else if nv.path.is_ident("target") {
                target = Some(s.value());
            } else if nv.path.is_ident("target_kind") {
                target_kind = Some(s.value());
            }
        }
        let name = name.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "#[convex::cron] requires name = \"...\"",
            )
        })?;
        let schedule = schedule.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "#[convex::cron] requires schedule = \"<cron-expr>\"",
            )
        })?;
        // Compile-time validation: parse the schedule with saffron.
        // Typos like `0 3 * *` (missing a field) fail the build
        // instead of hiding until the cron registry is collected.
        if let Err(e) = schedule.parse::<saffron::Cron>() {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                format!("invalid cron schedule {:?}: {e}", schedule),
            ));
        }
        let target = target.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "#[convex::cron] requires target = \"<fn name>\"",
            )
        })?;
        let target_kind = target_kind.unwrap_or_else(|| "mutation".to_string());
        match target_kind.as_str() {
            "mutation" | "action" => {},
            other => {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    format!(
                        "target_kind must be \"mutation\" or \"action\", got {:?}",
                        other,
                    ),
                ));
            },
        }
        Ok(Self {
            name,
            schedule,
            target,
            target_kind,
        })
    }
}

pub fn attr(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr_ts: TokenStream2 = attr.into();
    let args = match syn::parse2::<AttrArgs>(attr_ts) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error().into(),
    };
    let input: Item = parse_macro_input!(item as Item);

    let AttrArgs {
        name,
        schedule,
        target,
        target_kind,
    } = args;

    let registration = quote! {
        ::convex_native::inventory::submit! {
            ::convex_native::CronRegistration {
                name: #name,
                schedule: #schedule,
                target: #target,
                target_kind: #target_kind,
            }
        }
    };

    let expanded = quote! {
        #input
        #registration
    };
    expanded.into()
}
