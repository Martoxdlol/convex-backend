# Migrating from JS to Rust

A side-by-side reference for porting a JS-based Convex app to
`convex_native`. Covers the 80% pattern bank. Read `QUICKSTART.md`
first for the shipped API.

## Schema

### JS

```ts
// convex/schema.ts
import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  users: defineTable({
    email: v.string(),
    profile: v.object({
      tier: v.union(v.literal("free"), v.literal("pro")),
      displayName: v.string(),
    }),
  }).index("by_email", ["email"]),

  messages: defineTable({
    author: v.id("users"),
    body: v.string(),
    channel: v.string(),
  }).index("by_channel", ["channel", "_creationTime"]),
});
```

### Rust

```rust
// src/schema.rs
use convex_native::prelude::*;

#[derive(ConvexEnum, Debug, Clone)]
pub enum Tier { Free, Pro }

#[derive(ConvexNested, Debug, Clone)]
pub struct Profile {
    pub tier: Tier,
    pub display_name: String,
}

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "users")]
#[convex(index(name = "by_email", fields = ["email"]))]
pub struct User {
    pub email: String,
    pub profile: Profile,
}

#[derive(ConvexDocument, Debug, Clone)]
#[convex(table = "messages")]
#[convex(index(name = "by_channel", fields = ["channel", "_creation_time"]))]
pub struct Message {
    pub author: Id<User>,
    pub body: String,
    pub channel: String,
}
```

Mapping cheatsheet:

| JS | Rust |
|----|------|
| `v.string()` | `String` |
| `v.number()` | `f64` |
| `v.int64()` | `i64` |
| `v.boolean()` | `bool` |
| `v.bytes()` | `Vec<u8>` |
| `v.null()` + `Option<T>` | `Option<T>` |
| `v.array(T)` | `Vec<T>` |
| `v.object({...})` | `#[derive(ConvexNested)]` struct |
| `v.id("users")` | `Id<User>` |
| `v.union(v.literal("a"), v.literal("b"))` | `#[derive(ConvexEnum)]` enum |
| Discriminated object union | `#[derive(ConvexUnion)]` enum |

## Query

### JS

```ts
// convex/users.ts
import { query } from "./_generated/server";
import { v } from "convex/values";

export const getByEmail = query({
  args: { email: v.string() },
  handler: async (ctx, { email }) => {
    return await ctx.db
      .query("users")
      .withIndex("by_email", q => q.eq("email", email))
      .unique();
  },
});
```

### Rust

```rust
// src/users.rs
use convex_native::{convex, prelude::*, QueryCtx};
use crate::schema::{User, UserField, UserIndex};

#[convex::query]
pub async fn get_by_email(
    ctx: &mut QueryCtx<'_, Rt>,
    email: String,
) -> anyhow::Result<Option<User>> {
    ctx.db()
        .query::<User>()
        .with_index(UserIndex::ByEmail)
        .eq(UserField::Email, email)?
        .unique()
        .await
}
```

Query chain cheatsheet:

| JS | Rust |
|----|------|
| `q.eq("field", v)` | `.eq(TField::Field, v)?` |
| `q.gt / .gte / .lt / .lte` | `.gt / .gte / .lt / .lte` |
| `.order("asc" / "desc")` | `.order(Order::Asc / Desc)` |
| `.take(n)` | `.take(n)` |
| `.first()` / `.unique()` / `.collect()` | same |

## Mutation

### JS

```ts
import { mutation } from "./_generated/server";
import { v } from "convex/values";

export const create = mutation({
  args: { email: v.string() },
  handler: async (ctx, { email }) => {
    const id = await ctx.db.insert("users", {
      email,
      profile: { tier: "free", displayName: "anon" },
    });
    await ctx.scheduler.runAfter(60_000, internal.users.send_welcome, { id });
    return id;
  },
});
```

### Rust

```rust
use convex_native::{convex, prelude::*, MutationCtx};

#[convex::mutation]
pub async fn create(
    ctx: &mut MutationCtx<'_, Rt>,
    email: String,
) -> anyhow::Result<Id<User>> {
    let id = ctx.db().insert(User {
        email,
        profile: Profile {
            tier: Tier::Free,
            display_name: "anon".into(),
        },
    }).await?;

    ctx.scheduler()
        .run_after(
            Duration::from_secs(60),
            SendWelcome,
            SendWelcomeArgs { id },
        )
        .await?;
    Ok(id)
}
```

Patch:

| JS | Rust |
|----|------|
| `ctx.db.patch(id, { name: "..." })` | `ctx.db().patch(id, UserPatch { name: Some("...".into()), ..Default::default() })` |

Full replace:

| JS | Rust |
|----|------|
| `ctx.db.replace(id, full)` | `ctx.db().replace(id, full)` |

## Action

### JS

```ts
import { action } from "./_generated/server";
import { v } from "convex/values";

export const sendWelcome = action({
  args: { id: v.id("users") },
  handler: async (ctx, { id }) => {
    const user = await ctx.runQuery(internal.users.getById, { id });
    // fetch external API...
    await ctx.runMutation(internal.users.markSent, { id });
    return null;
  },
});
```

### Rust

```rust
use convex_native::{convex, prelude::*, ActionCtx};

#[convex::action]
pub async fn send_welcome(
    ctx: &mut ActionCtx<'_, Rt>,
    id: Id<User>,
) -> anyhow::Result<()> {
    let user: Option<User> = ctx
        .run_query(GetById, GetByIdArgs { id })
        .await?;

    // reqwest::Client::new().post(...).send().await?;

    ctx.run_mutation(MarkSent, MarkSentArgs { id }).await?;
    Ok(())
}
```

## HTTP action

### JS

```ts
import { httpAction } from "./_generated/server";
import { httpRouter } from "convex/server";

const http = httpRouter();
http.route({
  path: "/api/webhooks/stripe",
  method: "POST",
  handler: httpAction(async (ctx, request) => {
    const body = await request.text();
    // ...
    return new Response(null, { status: 204 });
  }),
});
export default http;
```

### Rust

```rust
use convex_native::{convex, prelude::*, HttpActionCtx, HttpRequest, HttpResponse};

#[convex::http_action(method = "POST", path = "/api/webhooks/stripe")]
pub async fn stripe_webhook(
    ctx: &mut HttpActionCtx<'_, Rt>,
    req: HttpRequest,
) -> anyhow::Result<HttpResponse> {
    let _body = req.body_text()?;
    Ok(HttpResponse::new(204))
}
```

## Modifier translation

| JS pattern | Rust |
|------------|------|
| `internalQuery({...})` | `#[convex::query(internal)]` |
| `internalMutation({...})` | `#[convex::mutation(internal)]` |
| `internalAction({...})` | `#[convex::action(internal)]` |
| (no direct equivalent) | `#[convex::action(timeout_ms = 5000)]` |

## Error handling

### JS

```ts
throw new ConvexError({ code: "Unauthenticated", message: "..." });
```

### Rust

```rust
return Err(convex_native::errors::unauthenticated(
    "Unauthenticated",
    "..."
).into());
```

## Testing

JS-side `convex-test` has no direct Rust analog yet, but:

```rust
use convex_native::testing::{TestCallbacks, CallRecord, args};

let (cb, history) = TestCallbacks::new()
    .on_query("get_by_email", |_| Ok(ConvexValue::Null))
    .build();

runner.run_action_with_callbacks("send_welcome", ns, args! {
    "id" => "jd72...".to_string(),
}, cb).await?;

assert!(history.count(|r| matches!(r, CallRecord::Query { name, .. } if name == "get_by_email")) >= 1);
```

## Things that aren't covered here yet

- **Components.** JS supports `defineComponent` for reusable modules;
  native has no analog yet.
- **Crons.** JS `cron.ts`:

  ```ts
  import { cronJobs } from "convex/server";
  import { internal } from "./_generated/api";
  const crons = cronJobs();
  crons.cron("nightly-cleanup", "0 3 * * *", internal.tasks.nightlyCleanup);
  export default crons;
  ```

  Native:

  ```rust
  #[convex::mutation(internal)]
  async fn nightly_cleanup(_ctx: &mut MutationCtx<'_, Rt>) -> Result<()> { Ok(()) }

  #[convex::cron(
      name = "nightly-cleanup",
      schedule = "0 3 * * *",
      target = "nightly_cleanup",
  )]
  fn _nightly_cleanup_cron() {}
  ```
- **Deploy / dev workflow.** `npx convex dev` doesn't talk to the
  native runtime yet. You build and run the Rust binary locally and
  deploy it as a normal Rust service.
- **Client SDK codegen.** JS generates `_generated/api.d.ts`; native
  exposes marker types (`GetByEmail`, `SendWelcomeArgs`) as the typed
  reference surface. There's no codegen step.
