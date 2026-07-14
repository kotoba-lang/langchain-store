(ns langchain-store.core
  "Shared machinery for `langchain.db`-backed actor stores — the seam
  ~190 cloud-itonami actors currently hand-roll identically (the exact
  two-liner `enc`/`dec*` codec appears verbatim in 190 store.cljc
  files, ADR-2607141600).

  It centralizes the genuinely-common parts:
    - the EDN-blob codec (`enc`/`dec*`): compound values are stored as
      EDN strings so `langchain.db` doesn't expand them into
      sub-entities — the convention every DatomicStore uses;
    - `identity-schema`: build the `:db.unique/identity` schema map;
    - `read-stream` / `append-blob!`: the seq-keyed EDN-blob event-log
      read/append pattern (event-sourced stores).

  The DOMAIN keeps its own field/pull shaping and its `Store`
  protocol — this lib is the reusable substrate underneath, not a
  framework. Portable `.cljc` (JVM/CLJS/kotoba-WASM); the pure parts
  (`enc`/`dec*`/`identity-schema`) have zero deps, the event helpers
  use `langchain.db` (itself the Datomic-API seam, swappable to a
  kotoba-server pod)."
  (:require #?(:clj [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])
            [langchain.db :as d]))

;; ------------------------------- codec -------------------------------

(defn enc
  "Encode a compound value as an EDN string blob (stored so langchain.db
  doesn't expand it into sub-entities). The identical codec 190 stores
  reimplement."
  [v]
  (pr-str v))

(defn dec*
  "Decode an EDN-string blob; nil-safe."
  [s]
  (when s (edn/read-string s)))

;; ------------------------------ schema -------------------------------

(defn identity-schema
  "Build a langchain.db/DataScript schema marking each attr in `attrs`
  as `:db.unique/identity` (an accidental re-put of the same key
  upserts instead of forking history)."
  [attrs]
  (reduce (fn [m a] (assoc m a {:db/unique :db.unique/identity})) {} attrs))

;; --------------------------- event streams ---------------------------

(defn read-stream
  "Read a seq-keyed EDN-blob event stream: every entity carrying
  `seq-attr` + `edn-attr`, sorted by seq, blobs decoded → a vector.
  The event-log read the event-sourced stores share."
  [conn seq-attr edn-attr]
  (->> (d/q [:find '?s '?v
             :where ['?e seq-attr '?s] ['?e edn-attr '?v]]
            (d/db conn))
       (sort-by first)
       (mapv (comp dec* second))))

(defn append-blob!
  "Append one seq-keyed EDN-blob entity (`{seq-attr seq, edn-attr
  (enc value)}`). Because `seq-attr` is `:db.unique/identity`, a
  duplicate seq upserts rather than forking the log."
  [conn seq-attr edn-attr seq value]
  (d/transact! conn [{seq-attr seq edn-attr (enc value)}]))
