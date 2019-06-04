(defproject ryuuseijin/chronologer "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "https://github.com/ryuuseijin/chronologer"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0-RC5"]
                 [com.cognitect.aws/api "0.8.301"]
                 [com.cognitect.aws/dynamodb "697.2.391.0"]
                 [com.cognitect.aws/endpoints "1.1.11.537"]
                 [com.cognitect/transit-clj "0.8.313"]
                 [ryuuseijin/chan-utils "0.1.2-SNAPSHOT"]]
  :repl-options {:init-ns ryuuseijin.chronologer.core}
  :repositories {"dynamodb-local" "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"})
