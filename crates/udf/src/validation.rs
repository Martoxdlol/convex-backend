use std::{
    sync::{
        LazyLock,
        OnceLock,
    },
    time::Duration,
};

use anyhow::Context;
use common::{
    components::{
        CanonicalizedComponentFunctionPath,
        ComponentId,
        ComponentPath,
        PublicFunctionPath,
        ResolvedComponentFunctionPath,
    },
    errors::{
        report_error_sync,
        JsError,
    },
    identity::InertIdentity,
    log_lines::LogLines,
    query_journal::QueryJournal,
    runtime::{
        Runtime,
        UnixTimestamp,
    },
    types::{
        AllowedVisibility,
        OldBackendState,
        UdfType,
    },
    version::{
        Version,
        DEPRECATION_THRESHOLD,
    },
};
use database::{
    BootstrapComponentsModel,
    Transaction,
};
use errors::ErrorMetadata;
use keybroker::{
    DeploymentOp,
    Identity,
};
use model::{
    backend_info::BackendInfoModel,
    backend_state::BackendStateModel,
    components::ComponentsModel,
    modules::{
        function_validators::ReturnsValidator,
        module_versions::{
            AnalyzedFunction,
            Visibility,
        },
        ModuleModel,
    },
    udf_config::UdfConfigModel,
    virtual_system_mapping,
};
use rand::Rng;
use serde_json::Value as JsonValue;
use sync_types::types::SerializedArgs;
use value::{
    heap_size::HeapSize,
    serialized_args_ext::SerializedArgsExt,
    ConvexArray,
    ConvexValue,
    JsonPackedValue,
    NamespacedTableMapping,
};

use crate::{
    helpers::{
        parse_udf_args,
        validate_udf_args_size,
    },
    ActionOutcome,
    SyscallTrace,
    UdfOutcome,
};

pub const DISABLED_ERROR_MESSAGE_FREE_PLAN: &str =
    "You have exceeded the free plan limits, so your deployments have been disabled. Please \
     upgrade to a Pro plan or reach out to us at support@convex.dev for help.";
pub const DISABLED_ERROR_MESSAGE_PAID_PLAN: &str =
    "You have exceeded your spending limits, so your deployments have been disabled. Please \
     increase your spending limit on the Convex dashboard or wait until limits reset.";
pub const PAUSED_ERROR_MESSAGE: &str = "Cannot run functions while this deployment is paused. \
                                        Resume the deployment in the dashboard settings to allow \
                                        functions to run.";
pub const SUSPENDED_ERROR_MESSAGE: &str = "Cannot run functions while this deployment is \
                                           suspended. Please contact Convex if you believe this \
                                           is a mistake.";

/// Convex CLI versions before 1.28.2 double-deployed betterAuth/ paths.
static MIN_NPM_VERSION_FOR_BETTER_AUTH: LazyLock<Version> =
    LazyLock::new(|| Version::new(1, 28, 2));

/// Metadata a validation short-circuit needs to synthesize an
/// `AnalyzedFunction` for a name that's registered as a native
/// Rust handler (see `convex-native/ISSUE_NATIVE_HTTP_VALIDATION.md`).
///
/// Native handlers are registered via `inventory` at link time and
/// never flow through `ModuleModel::apply` / `_modules` rows. Without
/// this short-circuit the HTTP validation path refuses every
/// pure-native deployment's request because no analyzed row exists
/// for the name.
#[derive(Debug, Clone)]
pub struct NativeFunctionDescriptor {
    /// Kind of handler (query / mutation / action). HTTP actions
    /// don't flow through `ValidatedPathAndArgs` so they're not
    /// represented here.
    pub udf_type: UdfType,
    /// `true` when the function was declared with the
    /// `#[convex::*(internal)]` modifier. Maps to
    /// `Visibility::Internal`; otherwise `Visibility::Public`.
    pub is_internal: bool,
}

/// A trait the native-runtime layer implements to expose its
/// `inventory::collect!`ed registry to `udf::validation` without
/// forcing the `udf` crate to depend on `convex_native_core`.
///
/// Installed once at backend boot via
/// [`install_native_function_resolver`].
pub trait NativeFunctionResolver: Send + Sync + 'static {
    /// Return metadata for a native function by bare function name
    /// (e.g. `"create"` for a `#[convex::mutation] fn create(...)`).
    ///
    /// The module path component of the user-facing path
    /// (`mutations:create`, `queries:create`, ...) is ignored
    /// because native registrations are keyed by bare identifier.
    /// Two native handlers can't share an identifier — the
    /// registry enforces that at collection time, so a match is
    /// unambiguous.
    fn lookup(&self, function_name: &str) -> Option<NativeFunctionDescriptor>;
}

static NATIVE_FUNCTION_RESOLVER: OnceLock<std::sync::Arc<dyn NativeFunctionResolver>> =
    OnceLock::new();

/// Install the global native-function resolver. Idempotent: a
/// second call is a no-op. Called from `local_backend::make_app`
/// after the native registry is collected from inventory.
pub fn install_native_function_resolver(
    resolver: std::sync::Arc<dyn NativeFunctionResolver>,
) -> bool {
    NATIVE_FUNCTION_RESOLVER.set(resolver).is_ok()
}

/// Best-effort lookup against the installed resolver. Returns
/// `None` when no resolver was installed (either because the
/// backend is JS-only or because `make_app` hasn't run yet — tests
/// that bypass `make_app` fall into the latter).
fn resolve_native_function(function_name: &str) -> Option<NativeFunctionDescriptor> {
    NATIVE_FUNCTION_RESOLVER
        .get()
        .and_then(|resolver| resolver.lookup(function_name))
}

/// Build a synthetic `AnalyzedFunction` from a native-registry
/// descriptor. Args + returns validators are left `None`
/// (deserialised by `AnalyzedFunction::args` / `returns` as
/// `Unvalidated`) because the `#[convex::*]` macros do not emit
/// full validator metadata today — extending them is tracked in
/// `ISSUE_NATIVE_HTTP_VALIDATION.md` question 2.
fn synthesize_native_analyzed_function(
    path: &ResolvedComponentFunctionPath,
    descriptor: &NativeFunctionDescriptor,
) -> AnalyzedFunction {
    AnalyzedFunction {
        name: path.udf_path.function_name().clone(),
        pos: None,
        udf_type: descriptor.udf_type,
        visibility: Some(if descriptor.is_internal {
            Visibility::Internal
        } else {
            Visibility::Public
        }),
        args_str: None,
        returns_str: None,
    }
}

/// Fails with an error if the backend is not running. We have to return a
/// result of a result of () and a JSError because we use them to
/// differentiate between system and user errors.
#[fastrace::trace]
pub async fn fail_while_not_running<RT: Runtime>(
    tx: &mut Transaction<RT>,
) -> anyhow::Result<Result<(), JsError>> {
    // Use backend info entitlements as a proxy for whether the backend belongs to a
    // free or paid team.
    let backend_info = BackendInfoModel::new(tx).get().await?;
    let is_paid = backend_info
        .map(|info| info.streaming_export_enabled)
        .unwrap_or(false);

    let backend_state = BackendStateModel::new(tx)
        .get_backend_state()
        .await?
        .into_value();
    match backend_state {
        OldBackendState::Running => {},
        OldBackendState::Paused => {
            return Ok(Err(JsError::from_message(PAUSED_ERROR_MESSAGE.to_string())));
        },
        OldBackendState::Disabled => {
            if is_paid {
                return Ok(Err(JsError::from_message(
                    DISABLED_ERROR_MESSAGE_PAID_PLAN.to_string(),
                )));
            } else {
                return Ok(Err(JsError::from_message(
                    DISABLED_ERROR_MESSAGE_FREE_PLAN.to_string(),
                )));
            }
        },
        OldBackendState::Suspended => {
            return Ok(Err(JsError::from_message(
                SUSPENDED_ERROR_MESSAGE.to_string(),
            )));
        },
    }

    Ok(Ok(()))
}

pub async fn validate_schedule_args<RT: Runtime>(
    path: CanonicalizedComponentFunctionPath,
    udf_args: Vec<JsonValue>,
    scheduled_ts: UnixTimestamp,
    udf_ts: UnixTimestamp,
    tx: &mut Transaction<RT>,
) -> anyhow::Result<(CanonicalizedComponentFunctionPath, ConvexArray)> {
    // We validate the following mostly so the developer don't get the timestamp
    // wrong with more than order of magnitude.
    let delta = scheduled_ts.as_secs_f64() - udf_ts.as_secs_f64();
    if delta > 5.0 * 366.0 * 24.0 * 3600.0 {
        anyhow::bail!(ErrorMetadata::bad_request(
            "InvalidScheduledFunctionDelay",
            format!("{scheduled_ts:?} is more than 5 years in the future")
        ));
    }
    if delta < -5.0 * 366.0 * 24.0 * 3600.0 {
        anyhow::bail!(ErrorMetadata::bad_request(
            "InvalidScheduledFunctionDelay",
            format!("{scheduled_ts:?} is more than 5 years in the past")
        ));
    }
    let udf_args = parse_udf_args(&path.udf_path, udf_args)?;

    // Native-handler short-circuit: a scheduled `#[convex::mutation]`
    // has no `_modules` row, so `get_metadata_for_function` would
    // flag it as "nonexistent path". Ask the native resolver first;
    // on a hit the UDF is known-registered and no further checks are
    // needed here (arg validators live in the registry, but the
    // macros don't emit them yet — see ISSUE_NATIVE_HTTP_VALIDATION).
    if resolve_native_function(path.udf_path.function_name()).is_some() {
        return Ok((path, udf_args));
    }

    // Even though we might use different version of modules when executing,
    // we do validate that the scheduled function exists at time of scheduling.
    // We do it here instead of within transaction in order to leverage the module
    // cache.
    let canonicalized = path.clone();
    let module = ModuleModel::new(tx)
        .get_metadata_for_function(canonicalized.clone())
        .await?
        .with_context(|| {
            let p = String::from(path.udf_path.module().clone());
            let component = if path.component.is_root() {
                "".to_string()
            } else {
                format!("{} ", String::from(path.clone().component))
            };
            ErrorMetadata::bad_request(
                "InvalidScheduledFunction",
                format!("Attempted to schedule function at nonexistent path: {component}{p}",),
            )
        })?;

    // We validate the function name if analyzed modules are available. Note
    // that scheduling was added after we started persisting the result
    // of analyze, we should always validate in practice. We will tighten
    // the interface and make AnalyzedResult non-optional in the future.
    let function_name = canonicalized.udf_path.function_name();
    if let Some(analyze_result) = &module.analyze_result {
        let found = analyze_result
            .functions
            .iter()
            .any(|f| &f.name == function_name);
        if !found {
            anyhow::bail!(ErrorMetadata::bad_request(
                "InvalidScheduledFunction",
                format!(
                    "Attempted to schedule function, but no exported function {} found in the \
                     file: {}{}. Did you forget to export it?",
                    function_name,
                    String::from(path.udf_path.module().clone()),
                    path.component.in_component_str(),
                ),
            ));
        }
    }

    Ok((path, udf_args))
}

/// Check whether the caller's allowed visibility permits running a function
/// with the given visibility, identity, component, and UDF type.
///
/// When `is_system_module` is true the function lives in a system module
/// (`_system/`).  System modules are not analyzed, so they have no
/// declared visibility.  We treat them like privileged endpoints: only
/// admin/system identities with the appropriate View/WriteData operation
/// may call them, regardless of component.
///
/// Returns:
/// - `Ok(Ok(()))` if access is allowed
/// - `Ok(Err(JsError))` if the function should appear as missing (e.g.
///   non-admin calling an internal function)
/// - `Err(anyhow)` if the caller lacks a required deployment operation
fn check_visibility_access(
    allowed_visibility: AllowedVisibility,
    visibility: &Option<Visibility>,
    identity: &Identity,
    component: ComponentId,
    expected_udf_type: UdfType,
    path: PublicFunctionPath,
    is_system_module: bool,
) -> anyhow::Result<Result<(), JsError>> {
    if identity.is_acting_as_user() {
        identity.require_operation(DeploymentOp::ActAsUser)?;
    }
    // System modules always require admin/system with View/WriteData.
    if is_system_module {
        return require_admin_data_op(identity, expected_udf_type, path);
    }
    match allowed_visibility {
        AllowedVisibility::All => Ok(Ok(())),
        AllowedVisibility::PublicOnly => match visibility {
            Some(Visibility::Public) => {
                // In a component, public functions still require an
                // admin/system identity with the appropriate operation.
                // User and Unknown identities cannot reach into components.
                if component != ComponentId::Root {
                    return require_admin_data_op(identity, expected_udf_type, path);
                }
                Ok(Ok(()))
            },
            Some(Visibility::Internal) => {
                // Admins may have the ability to run the internal function.
                if identity.is_admin() || identity.is_system() || identity.is_acting_as_user() {
                    let op = match expected_udf_type {
                        UdfType::Query => DeploymentOp::RunInternalQueries,
                        UdfType::Mutation => DeploymentOp::RunInternalMutations,
                        UdfType::Action | UdfType::HttpAction => DeploymentOp::RunInternalActions,
                    };
                    identity.require_operation(op)?;
                    Ok(Ok(()))
                } else {
                    Ok(Err(JsError::from_message(missing_or_internal_error(path)?)))
                }
            },
            None => {
                anyhow::bail!("No visibility found for analyzed function");
            },
        },
    }
}

/// Require that the identity is admin/system with the appropriate
/// View/WriteData operation. Returns `Ok(Err(JsError))` for
/// User/Unknown identities so callers can produce a clean error response.
fn require_admin_data_op(
    identity: &Identity,
    expected_udf_type: UdfType,
    path: PublicFunctionPath,
) -> anyhow::Result<Result<(), JsError>> {
    if identity.is_admin() || identity.is_system() || identity.is_acting_as_user() {
        let op = match expected_udf_type {
            UdfType::Query => DeploymentOp::ViewData,
            UdfType::Mutation | UdfType::Action | UdfType::HttpAction => DeploymentOp::WriteData,
        };
        identity.require_operation(op)?;
        Ok(Ok(()))
    } else {
        Ok(Err(JsError::from_message(missing_or_internal_error(path)?)))
    }
}

fn missing_or_internal_error(path: PublicFunctionPath) -> anyhow::Result<String> {
    let path = path.debug_into_component_path();
    // The hint at the end of the error message used to be
    // unconditional ("Did you forget to run `npx convex dev`?"), which
    // misled pure-native deployments whose functions live in Rust via
    // `#[convex::*]` macros (no `npx convex dev` involved). When a
    // native resolver is installed we adjust the hint so the
    // troubleshooting trail points at the native-handler plumbing
    // instead. See `convex-native/ISSUE_NATIVE_HTTP_VALIDATION.md`
    // question 5.
    let hint = if NATIVE_FUNCTION_RESOLVER.get().is_some() {
        "Double-check the `#[convex::query/mutation/action]` spelling and that the handler module \
         is linked into the backend binary. If this deployment also uses JavaScript, run `npx \
         convex dev` to push the JS side."
    } else {
        "Did you forget to run `npx convex dev`?"
    };
    Ok(format!(
        "Could not find public function for '{}'{}. {}",
        String::from(path.udf_path.clone().strip()),
        path.component.in_component_str(),
        hint,
    ))
}

fn should_block_path(path: &ResolvedComponentFunctionPath) -> bool {
    if path.component != ComponentId::Root {
        return false;
    }

    let path_str = path.udf_path.to_string();
    path_str.starts_with("betterAuth/")
}

#[fastrace::trace]
async fn udf_version<RT: Runtime>(
    path: &ResolvedComponentFunctionPath,
    tx: &mut Transaction<RT>,
) -> anyhow::Result<Result<Version, JsError>> {
    let udf_config = UdfConfigModel::new(tx, path.component.into()).get().await?;

    let udf_version = match udf_config {
        Some(udf_config) if udf_config.server_version > DEPRECATION_THRESHOLD.npm.unsupported => {
            udf_config.server_version.clone()
        },
        _ => {
            if udf_config.is_none()
                && ModuleModel::new(tx)
                    .get_analyzed_function_by_id(path)
                    .await?
                    .is_err()
            {
                // We don't have a UDF config and we can't find the analyzed function.
                // Likely this developer has never pushed before, so give them
                // the missing error message.
                return Ok(Err(JsError::from_message(missing_or_internal_error(
                    PublicFunctionPath::ResolvedComponent(path.clone()),
                )?)));
            }

            let unsupported = format!(
                "Convex functions at or below version {} are no longer supported. Update your \
                 Convex npm package and then push your functions again with `npx convex deploy` \
                 or `npx convex dev`.",
                DEPRECATION_THRESHOLD.npm.unsupported
            );

            return Ok(Err(JsError::from_message(unsupported)));
        },
    };
    Ok(Ok(udf_version))
}

/// The path and args to a UDF that have already undergone validation.
///
/// This validation includes:
/// - Checking the visibility of the UDF.
/// - Checking that the UDF is the correct type.
/// - Checking the args size.
/// - Checking that the args pass validation.
///
/// This should only be constructed via `ValidatedPathAndArgs::new` to use the
/// type system to enforce that validation is never skipped.
#[derive(Clone, Eq, PartialEq)]
pub struct ValidatedPathAndArgs {
    path: ResolvedComponentFunctionPath,
    args: SerializedArgs,
    // Not set for system modules.
    npm_version: Option<Version>,
}

impl ValidatedPathAndArgs {
    /// Check if the function being called matches the allowed visibility and
    /// return a ValidatedPathAndArgs or an appropriate JsError.
    ///
    /// We want to use the same error message for "this function exists, but
    /// with the wrong visibility" and "this function does not exist" so we
    /// don't leak which non-public functions exist.
    pub async fn new<RT: Runtime>(
        allowed_visibility: AllowedVisibility,
        tx: &mut Transaction<RT>,
        path: PublicFunctionPath,
        args: SerializedArgs,
        expected_udf_type: UdfType,
    ) -> anyhow::Result<Result<ValidatedPathAndArgs, JsError>> {
        Self::new_with_returns_validator(allowed_visibility, tx, path, args, expected_udf_type)
            .await
            .map(|r| r.map(|(path_and_args, _)| path_and_args))
    }

    /// Do argument validation and get returns validator without retrieving
    /// the analyze result twice.
    #[fastrace::trace]
    pub async fn new_with_returns_validator<RT: Runtime>(
        allowed_visibility: AllowedVisibility,
        tx: &mut Transaction<RT>,
        public_path: PublicFunctionPath,
        args: SerializedArgs,
        expected_udf_type: UdfType,
    ) -> anyhow::Result<Result<(ValidatedPathAndArgs, ReturnsValidator), JsError>> {
        if public_path.is_system() {
            let path = match public_path {
                PublicFunctionPath::RootExport(path) => ResolvedComponentFunctionPath {
                    component: ComponentId::Root,
                    udf_path: path.into(),
                    component_path: ComponentPath::root(),
                },
                PublicFunctionPath::Component(path) => {
                    let (_, component) = BootstrapComponentsModel::new(tx)
                        .component_path_to_ids(&path.component)?
                        .context(ErrorMetadata::bad_request(
                            "ComponentPathNotFound",
                            format!("Component path '{}' not found", path.component),
                        ))?;
                    ResolvedComponentFunctionPath {
                        component,
                        udf_path: path.udf_path,
                        component_path: path.component,
                    }
                },
                PublicFunctionPath::ResolvedComponent(path) => path,
            };
            // We don't analyze system modules, so we don't validate anything
            // except the identity for them.
            if let Err(js_error) = check_visibility_access(
                allowed_visibility,
                &None,
                tx.identity(),
                path.component,
                expected_udf_type,
                PublicFunctionPath::ResolvedComponent(path.clone()),
                true,
            )? {
                return Ok(Err(js_error));
            }
            return Ok(Ok((
                ValidatedPathAndArgs {
                    path,
                    args,
                    npm_version: None,
                },
                ReturnsValidator::Unvalidated,
            )));
        }

        match fail_while_not_running(tx).await {
            Ok(Ok(())) => {},
            Ok(Err(e)) => {
                return Ok(Err(e));
            },
            Err(e) => return Err(e),
        }

        let path = match public_path.clone() {
            PublicFunctionPath::RootExport(path) => {
                let path = ComponentsModel::new(tx)
                    .resolve_public_export_path(path)
                    .await?;
                let (_, component) = BootstrapComponentsModel::new(tx)
                    .must_component_path_to_ids(&path.component)?;
                ResolvedComponentFunctionPath {
                    component,
                    udf_path: path.udf_path,
                    component_path: path.component,
                }
            },
            PublicFunctionPath::Component(path) => {
                let (_, component) = BootstrapComponentsModel::new(tx)
                    .component_path_to_ids(&path.component)?
                    .context(ErrorMetadata::bad_request(
                        "ComponentPathNotFound",
                        format!("Component path '{}' not found", path.component),
                    ))?;
                ResolvedComponentFunctionPath {
                    component,
                    udf_path: path.udf_path,
                    component_path: path.component,
                }
            },
            PublicFunctionPath::ResolvedComponent(path) => path,
        };

        // Native-handler short-circuit. When a native resolver is
        // installed (see `install_native_function_resolver`) and the
        // bare function name matches a `#[convex::*]` registration,
        // synthesize an `AnalyzedFunction` from the registry instead
        // of reading `_modules`. A pure-native deployment never
        // writes those rows — without this short-circuit every HTTP
        // request would hit the `missing_or_internal_error` branch
        // below. See `convex-native/ISSUE_NATIVE_HTTP_VALIDATION.md`.
        if let Some(native) = resolve_native_function(path.udf_path.function_name()) {
            let analyzed_function = synthesize_native_analyzed_function(&path, &native);
            let returns_validator = ReturnsValidator::Unvalidated;
            // Pass `None` npm_version through `new_inner_with_version`
            // — native handlers are not published via the npm SDK so
            // `udf_server_version` has no meaningful value for them.
            return match ValidatedPathAndArgs::new_inner(
                allowed_visibility,
                tx,
                path,
                args,
                expected_udf_type,
                analyzed_function,
                None,
            )? {
                Ok(validated) => Ok(Ok((validated, returns_validator))),
                Err(js_err) => Ok(Err(js_err)),
            };
        }

        let udf_version = match udf_version(&path, tx).await? {
            Ok(udf_version) => udf_version,
            Err(e) => return Ok(Err(e)),
        };

        // AnalyzeResult result should be populated for all supported versions.
        //
        //
        let Ok(analyzed_function) = ModuleModel::new(tx)
            .get_analyzed_function_by_id(&path)
            .await?
        else {
            return Ok(Err(JsError::from_message(missing_or_internal_error(
                public_path,
            )?)));
        };

        if udf_version < *MIN_NPM_VERSION_FOR_BETTER_AUTH && should_block_path(&path) {
            tracing::warn!(
                "Blocking betterAuth/ path '{}' for SDK version {} (< 1.28.2)",
                path.udf_path,
                udf_version
            );
            return Ok(Err(JsError::from_message(missing_or_internal_error(
                public_path,
            )?)));
        }

        let returns_validator = if path.udf_path.is_system() {
            ReturnsValidator::Unvalidated
        } else {
            analyzed_function.returns()?
        };

        match ValidatedPathAndArgs::new_inner(
            allowed_visibility,
            tx,
            path,
            args,
            expected_udf_type,
            analyzed_function,
            Some(udf_version),
        )? {
            Ok(validated_udf_path_and_args) => {
                Ok(Ok((validated_udf_path_and_args, returns_validator)))
            },
            Err(js_err) => Ok(Err(js_err)),
        }
    }

    fn new_inner<RT: Runtime>(
        allowed_visibility: AllowedVisibility,
        tx: &mut Transaction<RT>,
        path: ResolvedComponentFunctionPath,
        args: SerializedArgs,
        expected_udf_type: UdfType,
        analyzed_function: AnalyzedFunction,
        version: Option<Version>,
    ) -> anyhow::Result<Result<ValidatedPathAndArgs, JsError>> {
        if let Err(js_error) = check_visibility_access(
            allowed_visibility,
            &analyzed_function.visibility,
            tx.identity(),
            path.component,
            expected_udf_type,
            PublicFunctionPath::ResolvedComponent(path.clone()),
            false,
        )? {
            return Ok(Err(js_error));
        }
        if expected_udf_type != analyzed_function.udf_type {
            return Ok(Err(JsError::from_message(format!(
                "Trying to execute {}{} as {}, but it is defined as {}.",
                path.udf_path,
                path.clone().for_logging().component.in_component_str(),
                expected_udf_type,
                analyzed_function.udf_type
            ))));
        }

        let udf_args = match parse_udf_args(&path.udf_path, args.clone().into_args()?) {
            Ok(udf_args) => udf_args,
            Err(err) => return Ok(Err(err)),
        };
        match validate_udf_args_size(&path.udf_path, &udf_args) {
            Ok(()) => (),
            Err(err) => return Ok(Err(err)),
        }

        let table_mapping = &tx.table_mapping().namespace(path.component.into());

        // If the UDF has an args validator, check that these args match.
        let args_validation_error = analyzed_function.args()?.check_args(
            &udf_args,
            table_mapping,
            virtual_system_mapping(),
        )?;

        if let Some(error) = args_validation_error {
            return Ok(Err(JsError::from_message(format!(
                "ArgumentValidationError: {error}",
            ))));
        }

        Ok(Ok(ValidatedPathAndArgs {
            path,
            args,
            npm_version: version,
        }))
    }

    pub fn args_size(&self) -> usize {
        self.args.heap_size()
    }

    pub fn path(&self) -> &ResolvedComponentFunctionPath {
        &self.path
    }

    pub fn consume(
        self,
    ) -> (
        ResolvedComponentFunctionPath,
        SerializedArgs,
        Option<Version>,
    ) {
        (self.path, self.args, self.npm_version)
    }

    pub fn npm_version(&self) -> &Option<Version> {
        &self.npm_version
    }

    pub fn from_proto(
        pb::common::ValidatedPathAndArgs {
            path,
            args,
            npm_version,
            component_path,
            component_id,
        }: pb::common::ValidatedPathAndArgs,
    ) -> anyhow::Result<Self> {
        let args =
            SerializedArgs::from_slice(&args.ok_or_else(|| anyhow::anyhow!("Missing args"))?)?;
        let component = ComponentId::deserialize_from_string(component_id.as_deref())?;
        let component_path = component_path
            .context("Missing component_path in proto")?
            .try_into()
            .context("Invalid component path in proto")?;
        Ok(Self {
            path: ResolvedComponentFunctionPath {
                component,
                udf_path: path.context("Missing udf_path")?.parse()?,
                component_path,
            },
            args,
            npm_version: npm_version.map(|v| Version::parse(&v)).transpose()?,
        })
    }
}

impl TryFrom<ValidatedPathAndArgs> for pb::common::ValidatedPathAndArgs {
    type Error = anyhow::Error;

    fn try_from(
        ValidatedPathAndArgs {
            path,
            args,
            npm_version,
        }: ValidatedPathAndArgs,
    ) -> anyhow::Result<Self> {
        let args = args.get().as_bytes().to_vec();
        let component_path = Some(path.component_path.into());
        Ok(Self {
            path: Some(path.udf_path.to_string()),
            args: Some(args),
            npm_version: npm_version.map(|v| v.to_string()),
            component_path,
            component_id: path.component.serialize_to_string(),
        })
    }
}

/// A UDF path that has been validated to be an HTTP route.
///
/// This should only be constructed via `ValidatedHttpRoute::try_from` to use
/// the type system to enforce that validation is never skipped.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ValidatedHttpPath {
    path: ResolvedComponentFunctionPath,
    npm_version: Option<Version>,
}

impl ValidatedHttpPath {
    pub async fn new<RT: Runtime>(
        tx: &mut Transaction<RT>,
        path: CanonicalizedComponentFunctionPath,
    ) -> anyhow::Result<Result<Self, JsError>> {
        // This is not a developer error on purpose.
        anyhow::ensure!(
            path.udf_path.module().as_str() == "http.js",
            "Unexpected http udf path: {:?}",
            path.udf_path,
        );
        if !path.udf_path.is_system() {
            match fail_while_not_running(tx).await {
                Ok(Ok(())) => {},
                Ok(Err(e)) => {
                    return Ok(Err(e));
                },
                Err(e) => return Err(e),
            }
        }
        let (_, component) =
            BootstrapComponentsModel::new(tx).must_component_path_to_ids(&path.component)?;
        let path = ResolvedComponentFunctionPath {
            component,
            udf_path: path.udf_path,
            component_path: path.component,
        };
        let udf_version = match udf_version(&path, tx).await? {
            Ok(udf_version) => udf_version,
            Err(e) => return Ok(Err(e)),
        };
        Ok(Ok(ValidatedHttpPath {
            path,
            npm_version: Some(udf_version),
        }))
    }

    pub fn npm_version(&self) -> &Option<Version> {
        &self.npm_version
    }

    pub fn path(&self) -> &ResolvedComponentFunctionPath {
        &self.path
    }

    pub fn from_proto(
        pb::common::ValidatedHttpPath {
            path,
            component_path,
            component_id,
            npm_version,
        }: pb::common::ValidatedHttpPath,
    ) -> anyhow::Result<Self> {
        let component = ComponentId::deserialize_from_string(component_id.as_deref())?;
        let component_path = component_path
            .context("Missing component_path in proto")?
            .try_into()
            .context("Invalid component path in proto")?;
        Ok(Self {
            path: ResolvedComponentFunctionPath {
                component,
                udf_path: path.context("Missing udf_path")?.parse()?,
                component_path,
            },
            npm_version: npm_version.map(|v| Version::parse(&v)).transpose()?,
        })
    }
}

impl TryFrom<ValidatedHttpPath> for pb::common::ValidatedHttpPath {
    type Error = anyhow::Error;

    fn try_from(
        ValidatedHttpPath { path, npm_version }: ValidatedHttpPath,
    ) -> anyhow::Result<Self> {
        let component_path = Some(path.component_path.into());
        Ok(Self {
            path: Some(path.udf_path.to_string()),
            npm_version: npm_version.map(|v| v.to_string()),
            component_path,
            component_id: path.component.serialize_to_string(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ValidatedUdfOutcome {
    pub path: CanonicalizedComponentFunctionPath,
    pub arguments: SerializedArgs,
    pub identity: InertIdentity,

    pub rng_seed: [u8; 32],
    pub observed_rng: bool,

    pub unix_timestamp: UnixTimestamp,
    pub observed_time: bool,

    pub log_lines: LogLines,
    pub journal: QueryJournal,

    // QueryUdfOutcomes are stored in the Udf level cache, which is why we would like
    // them to have more compact representation.
    pub result: Result<JsonPackedValue, JsError>,

    pub syscall_trace: SyscallTrace,

    pub udf_server_version: Option<semver::Version>,
    pub mutation_queue_length: Option<usize>,
    pub memory_in_mb: u64,
    // TODO(ENG-10204) Make required
    pub user_execution_time: Option<Duration>,
}

impl HeapSize for ValidatedUdfOutcome {
    fn heap_size(&self) -> usize {
        self.path.udf_path.heap_size()
            + self.arguments.heap_size()
            + self.identity.heap_size()
            + self.log_lines.heap_size()
            + self.journal.heap_size()
            + self.result.heap_size()
            + self.syscall_trace.heap_size()
    }
}

impl ValidatedUdfOutcome {
    /// Used for synthesizing an outcome when we encounter an error before
    /// reaching the isolate.
    pub fn from_error(
        js_error: JsError,
        path: CanonicalizedComponentFunctionPath,
        arguments: SerializedArgs,
        identity: InertIdentity,
        rt: impl Runtime,
        udf_server_version: Option<semver::Version>,
    ) -> anyhow::Result<Self> {
        Ok(ValidatedUdfOutcome {
            path,
            arguments,
            identity,
            rng_seed: rt.rng().random(),
            observed_rng: false,
            unix_timestamp: rt.unix_timestamp(),
            observed_time: false,
            log_lines: vec![].into(),
            journal: QueryJournal::new(),
            result: Err(js_error),
            syscall_trace: SyscallTrace::new(),
            udf_server_version,
            mutation_queue_length: None,
            memory_in_mb: 0,
            user_execution_time: Some(Duration::ZERO),
        })
    }

    pub fn new(
        outcome: UdfOutcome,
        returns_validator: ReturnsValidator,
        table_mapping: &NamespacedTableMapping,
        mutation_queue_length: Option<usize>,
    ) -> Self {
        let mut validated = ValidatedUdfOutcome {
            path: outcome.path,
            arguments: outcome.arguments,
            identity: outcome.identity,
            rng_seed: outcome.rng_seed,
            observed_rng: outcome.observed_rng,
            unix_timestamp: outcome.unix_timestamp,
            observed_time: outcome.observed_time,
            log_lines: outcome.log_lines,
            journal: outcome.journal,
            result: outcome.result,
            syscall_trace: outcome.syscall_trace,
            udf_server_version: outcome.udf_server_version,
            mutation_queue_length,
            memory_in_mb: outcome.memory_in_mb,
            user_execution_time: outcome.user_execution_time,
        };

        // TODO(CX-6318) Don't pack json value until it's been validated.
        if returns_validator.needs_validation() {
            let returns: ConvexValue = match &validated.result {
                Ok(json_packed_value) => match json_packed_value.unpack() {
                    Ok(v) => v,
                    Err(mut e) => {
                        report_error_sync(&mut e);
                        return validated;
                    },
                },
                Err(_) => return validated,
            };

            if let Some(js_err) =
                returns_validator.check_output(&returns, table_mapping, virtual_system_mapping())
            {
                validated.result = Err(js_err);
            };
        }
        validated
    }
}

#[derive(Debug, Clone)]
pub struct ValidatedActionOutcome {
    pub path: CanonicalizedComponentFunctionPath,
    pub arguments: SerializedArgs,
    pub identity: InertIdentity,

    pub unix_timestamp: UnixTimestamp,

    pub result: Result<JsonPackedValue, JsError>,
    pub syscall_trace: SyscallTrace,

    pub udf_server_version: Option<semver::Version>,
    pub mutation_queue_length: Option<usize>,
    // TODO(ENG-10204) Make required
    pub user_execution_time: Option<Duration>,
}

impl ValidatedActionOutcome {
    pub fn new(
        outcome: ActionOutcome,
        returns_validator: ReturnsValidator,
        table_mapping: &NamespacedTableMapping,
    ) -> Self {
        let mut validated = ValidatedActionOutcome {
            path: outcome.path,
            arguments: outcome.arguments,
            identity: outcome.identity,
            unix_timestamp: outcome.unix_timestamp,
            result: outcome.result,
            syscall_trace: outcome.syscall_trace,
            udf_server_version: outcome.udf_server_version,
            mutation_queue_length: None,
            user_execution_time: outcome.user_execution_time,
        };

        if returns_validator.needs_validation()
            && let Ok(json_packed_value) = &validated.result
        {
            match json_packed_value.unpack() {
                Ok(output) => {
                    if let Some(js_err) = returns_validator.check_output(
                        &output,
                        table_mapping,
                        virtual_system_mapping(),
                    ) {
                        validated.result = Err(js_err);
                    }
                },
                Err(mut e) => {
                    report_error_sync(&mut e);
                },
            }
        }

        validated
    }

    pub fn from_error(
        js_error: JsError,
        path: CanonicalizedComponentFunctionPath,
        arguments: SerializedArgs,
        identity: InertIdentity,
        rt: impl Runtime,
        udf_server_version: Option<semver::Version>,
    ) -> Self {
        ValidatedActionOutcome {
            path,
            arguments,
            identity,
            unix_timestamp: rt.unix_timestamp(),
            result: Err(js_error),
            syscall_trace: SyscallTrace::new(),
            udf_server_version,
            mutation_queue_length: None,
            // FIXME: We should count user execution time even for failed functions
            user_execution_time: None,
        }
    }

    pub fn from_system_error(
        path: CanonicalizedComponentFunctionPath,
        arguments: SerializedArgs,
        identity: InertIdentity,
        unix_timestamp: UnixTimestamp,
        e: &anyhow::Error,
    ) -> ValidatedActionOutcome {
        ValidatedActionOutcome {
            path,
            arguments,
            identity,
            unix_timestamp,
            result: Err(JsError::from_error_ref(e)),
            syscall_trace: SyscallTrace::new(),
            udf_server_version: None,
            mutation_queue_length: None,
            user_execution_time: None,
        }
    }
}

#[cfg(test)]
mod native_resolver_tests {
    //! Unit tests for the native-function short-circuit plumbing.
    //!
    //! These tests exercise the pure helpers
    //! (`synthesize_native_analyzed_function`) and pin the `AnalyzedFunction`
    //! shape we hand to `ValidatedPathAndArgs::new_inner`. The full wire
    //! round-trip (HTTP → Application → ValidatedPathAndArgs) lives as an
    //! end-to-end test in the integration suite because it needs
    //! `Database<RT>` + a running backend.
    use common::{
        components::{
            ComponentId,
            ComponentPath,
            ResolvedComponentFunctionPath,
        },
        types::UdfType,
    };
    use model::modules::module_versions::Visibility;

    use super::{
        synthesize_native_analyzed_function,
        NativeFunctionDescriptor,
        NativeFunctionResolver,
    };

    fn path_for(fn_name: &str) -> ResolvedComponentFunctionPath {
        ResolvedComponentFunctionPath {
            component: ComponentId::Root,
            udf_path: format!("mutations:{fn_name}").parse().unwrap(),
            component_path: ComponentPath::root(),
        }
    }

    #[test]
    fn synthesize_preserves_name_and_udf_type() {
        let path = path_for("create");
        let desc = NativeFunctionDescriptor {
            udf_type: UdfType::Mutation,
            is_internal: false,
        };
        let synthesised = synthesize_native_analyzed_function(&path, &desc);
        assert_eq!(synthesised.name.to_string(), "create");
        assert_eq!(synthesised.udf_type, UdfType::Mutation);
    }

    #[test]
    fn synthesize_maps_internal_flag_to_visibility_internal() {
        // Reason we pin this: visibility drives the
        // `check_visibility_access` check inside `new_inner`. If
        // `is_internal` silently stopped mapping to
        // `Visibility::Internal`, every `#[convex::*(internal)]`
        // handler would become callable from non-admin clients.
        let desc = NativeFunctionDescriptor {
            udf_type: UdfType::Query,
            is_internal: true,
        };
        let synthesised = synthesize_native_analyzed_function(&path_for("secret"), &desc);
        assert_eq!(synthesised.visibility, Some(Visibility::Internal));
    }

    #[test]
    fn synthesize_maps_public_flag_to_visibility_public() {
        let desc = NativeFunctionDescriptor {
            udf_type: UdfType::Query,
            is_internal: false,
        };
        let synthesised = synthesize_native_analyzed_function(&path_for("list"), &desc);
        assert_eq!(synthesised.visibility, Some(Visibility::Public));
    }

    #[test]
    fn synthesize_leaves_args_and_returns_unvalidated() {
        // The `#[convex::*]` macros do not yet emit full validator
        // metadata, so the short-circuit maps both to the
        // `Unvalidated` variant (`AnalyzedFunction::args()` /
        // `returns()` return `Unvalidated` when the JSON string is
        // `None`). This test pins the contract — if the macros
        // start emitting full validators, tighten this test + the
        // synthesizer accordingly.
        let desc = NativeFunctionDescriptor {
            udf_type: UdfType::Action,
            is_internal: false,
        };
        let synthesised = synthesize_native_analyzed_function(&path_for("summarise"), &desc);
        assert!(synthesised.args_str.is_none());
        assert!(synthesised.returns_str.is_none());
    }

    #[test]
    fn trait_impl_returns_none_for_unknown_name() {
        struct NoHandlers;
        impl NativeFunctionResolver for NoHandlers {
            fn lookup(&self, _: &str) -> Option<NativeFunctionDescriptor> {
                None
            }
        }
        let resolver: Box<dyn NativeFunctionResolver> = Box::new(NoHandlers);
        assert!(resolver.lookup("create").is_none());
    }

    #[test]
    fn trait_impl_returns_descriptor_for_known_name() {
        struct StubResolver;
        impl NativeFunctionResolver for StubResolver {
            fn lookup(&self, name: &str) -> Option<NativeFunctionDescriptor> {
                (name == "create").then_some(NativeFunctionDescriptor {
                    udf_type: UdfType::Mutation,
                    is_internal: false,
                })
            }
        }
        let resolver: Box<dyn NativeFunctionResolver> = Box::new(StubResolver);
        let desc = resolver.lookup("create").expect("known name");
        assert_eq!(desc.udf_type, UdfType::Mutation);
        assert!(!desc.is_internal);
        assert!(resolver.lookup("unknown").is_none());
    }
}
