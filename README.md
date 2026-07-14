# langchain-store

Shared machinery for `langchain.db`-backed actor stores — the seam
~190 cloud-itonami actors currently hand-roll identically.

The exact two-liner EDN-blob codec

```clojure
(defn- enc [v] (pr-str v))
(defn- dec* [s] (when s (edn/read-string s)))
```

appears **verbatim in 190 `store.cljc` files**. This lib centralizes it
plus the other genuinely-common store machinery, so a store shrinks to
its domain-specific field/pull shaping.

```clojure
(require '[langchain-store.core :as ls])

;; codec — compound values stored as EDN strings so langchain.db
;; doesn't expand them into sub-entities
(ls/enc {:a [1 2]})            ; => "{:a [1 2]}"
(ls/dec* "{:a [1 2]}")         ; => {:a [1 2]}

;; :db.unique/identity schema
(ls/identity-schema [:ev/seq]) ; => {:ev/seq {:db/unique :db.unique/identity}}

;; seq-keyed EDN-blob event log (append-only; duplicate seq upserts)
(def conn (d/create-conn (ls/identity-schema [:ev/seq])))
(ls/append-blob! conn :ev/seq :ev/edn 1 {:kind :a})
(ls/read-stream conn :ev/seq :ev/edn)   ; => [{:kind :a}]
```

The domain keeps its own `Store` protocol and pull/tx shaping — this is
the reusable substrate underneath, **not a framework**. Portable
`.cljc`; the pure parts (`enc`/`dec*`/`identity-schema`) are zero-dep,
the event helpers use `langchain.db` (the Datomic-API seam, swappable
to a kotoba-server pod).

## Provenance & scope

Extracted per **ADR-2607141600** (store-seam commonalization). First
adopter: `cloud-itonami-isic-6611-cryptoexchange`'s
`cryptoexchange.store`. A field-spec-driven `pull->map` / `map->tx`
helper (to also dedupe the *entity*-store boilerplate the 190 share in
shape but not in field lists) is the documented next increment.

## Test

```sh
clojure -M:test                                     # JVM compat
clojure -Sdeps '{:paths ["src" "test"]}' -M:cljs \  # CLJS primary
  -m cljs.main --target node -m langchain-store.cljs-runner
```

AGPL-3.0-or-later.
