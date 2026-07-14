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
