//! HTTP actions — external-facing handlers routed by method + path.

use convex_native::{
    convex,
    HttpActionCtx,
    HttpRequest,
    HttpResponse,
    Rt,
};

use crate::mutations::{
    CreateUser,
    CreateUserArgs,
};

/// Webhook endpoint — accepts `{ email, display_name }` and
/// triggers the `create_user` mutation. The mutation runs in its
/// own transaction (actions don't have one), so the webhook returns
/// the new id only after the insert commits.
#[convex::http_action(method = "POST", path = "/api/signup")]
#[allow(dead_code)]
async fn signup(
    ctx: &mut HttpActionCtx<'_, Rt>,
    req: HttpRequest,
) -> anyhow::Result<HttpResponse> {
    #[derive(serde::Deserialize)]
    struct Body {
        email: String,
        display_name: String,
    }

    let Body {
        email,
        display_name,
    } = match req.body_json() {
        Ok(b) => b,
        Err(e) => {
            return Ok(HttpResponse::json(
                400,
                serde_json::json!({ "error": format!("bad body: {e}") }),
            ));
        },
    };

    let id = ctx
        .run_mutation(
            CreateUser,
            CreateUserArgs {
                email,
                display_name,
            },
        )
        .await?;

    let request_id = ctx
        .execution_context()
        .map(|c| format!("{:?}", c))
        .unwrap_or_else(|| "<none>".to_string());

    HttpResponse::json(
        200,
        serde_json::json!({
            "id": id.to_string(),
            "request_context": request_id,
        }),
    )
    .with_header("Cache-Control", "no-store")
}
