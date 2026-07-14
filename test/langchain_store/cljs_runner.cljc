(ns langchain-store.cljs-runner
  "Run the portable suite under ClojureScript (cljs.main --target node):
    clojure -Sdeps '{:paths [\"src\" \"test\"]}' -M:dev:cljs \\
      -m cljs.main --target node -m langchain-store.cljs-runner"
  (:require [clojure.test :as t :refer [run-tests]]
            [langchain-store.core-test]))

#?(:cljs
   (defmethod t/report [:cljs.test/default :end-run-tests] [m]
     (when-not (t/successful? m)
       (set! (.-exitCode js/process) 1))))

(defn -main [] (run-tests 'langchain-store.core-test))
