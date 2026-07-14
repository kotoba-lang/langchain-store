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

;; --------------------------- entity field-spec -----------------------
;; The entity stores (application/party/... in ~190 actors) map a
;; logical map to/from a langchain.db entity through hand-written
;; `x->tx` / `pull->x` / `x-pull` triples. A field-spec drives all
;; three from data (ADR-2607141600 increment 2):
;;
;;   {logical-key {:attr :ns/attr        ; the datom attribute
;;                 :blob? bool           ; store as an EDN string blob
;;                 :default any          ; blob decode fallback (nil -> default)
;;                 :coerce fn}}          ; non-blob read transform (e.g. boolean)
;;
;; Semantics preserved from the hand-written stores: `map->tx` includes
;; only present (some?) keys (cond-> semantics; a false boolean IS
;; present); `pull->map` returns nil when the identity attr is absent,
;; decodes blobs with the default, and applies :coerce on read.

(defn pull-pattern
  "The langchain.db pull pattern (attr vector) for a field spec."
  [spec]
  (mapv :attr (vals spec)))

(defn map->tx
  "Logical map -> langchain.db tx-map via `spec`. Only present (some?)
  keys are included; :blob? fields are `enc`'d."
  [spec m]
  (reduce-kv (fn [tx lk {:keys [attr blob?]}]
               (let [v (get m lk)]
                 (if (some? v) (assoc tx attr (if blob? (enc v) v)) tx)))
             {} spec))

(defn pull->map
  "langchain.db pulled entity -> logical map via `spec`. Returns nil
  when the identity field's attr is absent. :blob? fields are `dec*`'d
  (nil -> :default); other fields pass through, with :coerce applied on
  read if given. `id-key` is the spec's identity logical key."
  [spec id-key pulled]
  (when (some? (get pulled (get-in spec [id-key :attr])))
    (reduce-kv (fn [m lk {:keys [attr blob? default coerce]}]
                 (assoc m lk (let [v (get pulled attr)]
                               (cond blob? (or (dec* v) default)
                                     coerce (coerce v)
                                     :else v))))
               {} spec)))
