(ns langchain-store.core-test
  "Spec for the shared langchain.db store machinery: the EDN-blob codec
  round-trips (incl. nested/compound values), the identity schema is
  shaped right, and the seq-keyed event stream reads back in order
  (upsert on duplicate seq). Portable — JVM + CLJS."
  (:require [clojure.test :refer [deftest is testing]]
            [langchain.db :as d]
            [langchain-store.core :as ls]))

(deftest codec-round-trips
  (testing "compound values survive as EDN string blobs (nil-safe)"
    (doseq [v [{:a 1 :b [:x "y" 3]} [1 2 3] {:nested {:deep #{:a :b}}} "str" 42 nil]]
      (is (= v (ls/dec* (ls/enc v))) (str "round-trip " (pr-str v)))))
  (is (nil? (ls/dec* nil)) "decoding nil yields nil, not a read error")
  (is (string? (ls/enc {:a 1})) "enc is a string blob"))

(deftest identity-schema-shape
  (is (= {:ev/seq {:db/unique :db.unique/identity}
          :ev/edn {:db/unique :db.unique/identity}}
         (ls/identity-schema [:ev/seq :ev/edn])))
  (is (= {} (ls/identity-schema []))))

(deftest event-stream-reads-in-order
  (let [conn (d/create-conn (ls/identity-schema [:ev/seq]))]
    (ls/append-blob! conn :ev/seq :ev/edn 1 {:kind :a :n 10})
    (ls/append-blob! conn :ev/seq :ev/edn 2 {:kind :b :n 20})
    (ls/append-blob! conn :ev/seq :ev/edn 3 {:kind :c :n 30})
    (testing "read-stream returns the decoded events sorted by seq"
      (is (= [{:kind :a :n 10} {:kind :b :n 20} {:kind :c :n 30}]
             (ls/read-stream conn :ev/seq :ev/edn))))
    (testing "a duplicate seq upserts (identity), not forks the log"
      (ls/append-blob! conn :ev/seq :ev/edn 2 {:kind :b :n 999})
      (is (= [{:kind :a :n 10} {:kind :b :n 999} {:kind :c :n 30}]
             (ls/read-stream conn :ev/seq :ev/edn))))))

(deftest empty-stream
  (let [conn (d/create-conn (ls/identity-schema [:ev/seq]))]
    (is (= [] (ls/read-stream conn :ev/seq :ev/edn)))))

;; ------------------- entity field-spec (increment 2) -----------------
;; Specs mirroring cloud-itonami-isic-6511's application + party
;; entities, so this pins that the generic helpers reproduce the
;; hand-written x->tx / pull->x behavior exactly.

(def ^:private app-spec
  {:id {:attr :app/id}
   :insured {:attr :app/insured}
   :beneficiaries {:attr :app/beneficiaries :blob? true :default []}
   :coverage-amount {:attr :app/coverage-amount}
   :currency {:attr :app/currency}
   :jurisdiction {:attr :app/jurisdiction}
   :status {:attr :app/status}
   :policy-number {:attr :app/policy-number}})

(def ^:private party-spec
  {:id {:attr :party/id}
   :name {:attr :party/name}
   :role {:attr :party/role}
   :sanctions-hit? {:attr :party/sanctions-hit? :coerce boolean}
   :id-doc {:attr :party/id-doc}})

(deftest pull-pattern-is-the-attr-vector
  (is (= #{:app/id :app/insured :app/beneficiaries :app/coverage-amount
           :app/currency :app/jurisdiction :app/status :app/policy-number}
         (set (ls/pull-pattern app-spec)))))

(deftest entity-round-trips-through-a-real-conn
  (let [conn (d/create-conn (ls/identity-schema [:app/id]))
        app {:id "app-1" :insured "party-1" :beneficiaries ["party-2" "party-3"]
             :coverage-amount 50000000 :currency "JPY" :jurisdiction "JPN"
             :status :intake :policy-number nil}]
    (d/transact! conn [(ls/map->tx app-spec app)])
    (let [pulled (d/pull (d/db conn) (ls/pull-pattern app-spec) [:app/id "app-1"])
          back (ls/pull->map app-spec :id pulled)]
      (testing "blob (beneficiaries) survives; a nil field round-trips to nil"
        (is (= ["party-2" "party-3"] (:beneficiaries back)))
        (is (= "party-1" (:insured back)))
        (is (= :intake (:status back)))
        (is (nil? (:policy-number back)))))))

(deftest blob-default-and-boolean-coerce
  (testing "an absent blob decodes to its :default"
    (let [conn (d/create-conn (ls/identity-schema [:app/id]))]
      (d/transact! conn [(ls/map->tx app-spec {:id "x" :insured "p"})])
      (let [back (ls/pull->map app-spec :id
                               (d/pull (d/db conn) (ls/pull-pattern app-spec) [:app/id "x"]))]
        (is (= [] (:beneficiaries back)) "missing blob -> default []"))))
  (testing "a false boolean is stored (some? gate) and coerced back to false"
    (let [conn (d/create-conn (ls/identity-schema [:party/id]))]
      (d/transact! conn [(ls/map->tx party-spec {:id "p1" :name "A" :role :insured
                                                 :sanctions-hit? false :id-doc "doc"})])
      (let [back (ls/pull->map party-spec :id
                               (d/pull (d/db conn) (ls/pull-pattern party-spec) [:party/id "p1"]))]
        (is (false? (:sanctions-hit? back)) "false is preserved, not dropped"))))
  (testing "an absent boolean coerces to false (mirrors (boolean nil))"
    (let [conn (d/create-conn (ls/identity-schema [:party/id]))]
      (d/transact! conn [(ls/map->tx party-spec {:id "p2" :name "B" :role :insured})])
      (let [back (ls/pull->map party-spec :id
                               (d/pull (d/db conn) (ls/pull-pattern party-spec) [:party/id "p2"]))]
        (is (false? (:sanctions-hit? back)))))))

(deftest pull-of-absent-identity-is-nil
  (let [conn (d/create-conn (ls/identity-schema [:app/id]))]
    (is (nil? (ls/pull->map app-spec :id
                            (d/pull (d/db conn) (ls/pull-pattern app-spec) [:app/id "nope"]))))))
