# langchain-store

Shared machinery for `langchain.db`-backed actor stores — the seam the
cloud-itonami actors otherwise hand-roll identically.

**316 repos depend on this library** (measured 2026-08-05 across
`orgs/cloud-itonami/*/deps.edn`), which makes it one of the highest-leverage
repos in the fleet: ADR-2608052000's dependency analysis put it in the top
three by fleet-wide effect per unit of work, because it gates 322 repos
transitively.

```clojure
(require '[langchain-store.core :as ls])

;; codec — compound values stored as EDN strings so langchain.db
;; doesn't expand them into sub-entities
(ls/enc {:a [1 2]})            ; => "{:a [1 2]}"
(ls/dec* "{:a [1 2]}")         ; => {:a [1 2]}

;; :db.unique/identity schema
(ls/identity-schema [:ev/seq]) ; => {:ev/seq {:db/unique :db.unique/identity}}

;; keyed EDN-blob entity (the single most duplicated shape in the fleet)
(def conn (d/create-conn (ls/identity-schema [:enlisted/id])))
(ls/put-blob!   conn :enlisted/id :enlisted/payload "e-1" {:rank :sgt})
(ls/blob-lookup conn :enlisted/id :enlisted/payload "e-1")   ; => {:rank :sgt}
(ls/blob-lookup conn :enlisted/id :enlisted/payload "gone")  ; => nil

;; seq-keyed EDN-blob event log (append-only; duplicate seq upserts)
(ls/append-blob! conn :ev/seq :ev/edn 1 {:kind :a})
(ls/read-stream  conn :ev/seq :ev/edn)   ; => [{:kind :a}]

;; append-only audit ledger: stamp + next seq in one call
(ls/append-record! conn :rec/seq :rec/payload :commit {:who "a"})
;; => appended as {:who "a" :type :commit :timestamp <now-ms>}

;; field-spec-driven entity mapping (non-blob attrs)
(def spec {:id     {:attr :app/id}
           :status {:attr :app/status}
           :beneficiaries {:attr :app/beneficiaries :blob? true :default []}})
(ls/map->tx   spec {:id "a-1" :status :intake})
(ls/pull->map spec :id (d/pull (d/db conn) (ls/pull-pattern spec) [:app/id "a-1"]))
```

The domain keeps its own `Store` protocol and its domain shaping — this is
the reusable substrate underneath, **not a framework**. Portable `.cljc`;
the pure parts (`enc`/`dec*`/`identity-schema`/`stamp`/`now-ms`) are
zero-dep, the conn-taking helpers use `langchain.db` (the Datomic-API seam,
swappable to a kotoba-server pod).

## What is here, and why (each backed by a count)

Everything in this library earned its place by being measured as duplicated
across the consumers first. The counts below are from 2026-08-05 over the
316 repos that depend on it; re-measure before adding anything else.

| Helper | Duplication it replaces | Consumers hand-rolling it |
|---|---|---|
| `enc` / `dec*` | the two-line EDN-blob codec | 190 (the original extraction, ADR-2607141600) |
| `blob-lookup` / `put-blob!` | read/write one EDN-blob payload by a unique id attr | **222** |
| `read-stream` / `append-blob!` | seq-keyed EDN-blob event log | 177 now call it |
| `now-ms` | the `#?(:clj System/currentTimeMillis :cljs …)` branch | **85** |
| `stamp` / `append-record!` | `:type` + `:timestamp` stamping at the next seq | 177 re-implement the surround |
| `pull->map` / `map->tx` / `pull-pattern` | field-spec entity mapping | 28 now call it |

`blob-lookup` is the notable one: only 9 of those 222 stores gave the shape
a name, so a name-based grep finds almost none of it. The duplication is
real, it is just inlined.

## Things this library deliberately does not do

- **`append-record!` is not atomic.** It reads the stream to find the next
  sequence number, then appends — the same read-then-append the hand-rolled
  `add-record!` bodies perform. A store with concurrent writers needs a
  single writer or an externally supplied seq; use `append-blob!` directly
  for that. Centralizing the code did not centralize a guarantee that was
  never there.
- **`now-ms` is a real clock, not injectable time.** `stamp` takes the
  timestamp as an argument so tests can pin it; `append-record!`'s 5-arity
  calls the clock and its 6-arity lets you supply one.
- **No `Store` protocol.** Each actor's protocol is its domain surface. A
  shared protocol would make the domain's vocabulary this library's problem.

## Provenance & scope

Extracted per **ADR-2607141600** (store-seam commonalization). First
adopter: `cloud-itonami-isic-6611-cryptoexchange`'s `cryptoexchange.store`.
Increment 2 (keyed-blob entity pair, portable clock, ledger record) landed
2026-08-05 per `docs/adr/0001-increment-2-keyed-blob-clock-ledger.md`, as
the maturity work ADR-2608052000 identified for this repo.

The field-spec `pull->map` / `map->tx` helper that earlier revisions of this
README called "the documented next increment" has been implemented since
increment 1 — the README was stale, not the code.

## Test

Both runtimes are gates; ClojureScript is the primary one.

```sh
clojure -M:test                                     # JVM compat
clojure -Sdeps '{:paths ["src" "test"]}' -M:cljs \  # CLJS primary
  -m cljs.main --target node -m langchain-store.cljs-runner
```

Last run 2026-08-05: 13 tests / 40 assertions, 0 failures on both.

AGPL-3.0-or-later.
