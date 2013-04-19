(ns flowctrl.core
  (:require [criterium.core :as crit]
            [clojure.tools.logging :as log]
            [flowctrl.parse-edi :as edi])
  (:gen-class))

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defn get-file-from-dir
  [dir]
  (first (rest (file-seq (clojure.java.io/file dir)))))

(defn load-edi-file
  [file]
  (let [data (slurp (.getAbsolutePath file))]
    (clojure.java.io/delete-file file)
    data))

(defn write-xml-file
  [dir]
  (fn [data]
    (spit (str dir (uuid) ".xml") data)))

(def #^{:dynamic true} *flows* (ref {}))

(declare flow flow-fork)

(comment
  [a b c [[d e h i j [[k u]]]
          [f g h i j]] y u i]
  )

(defn flow
  [& steps]
  (let [intro (take-while #(not (vector? %)) steps)]
    (vec (if (= (count intro) (count steps))
           intro
           (let [remaining (drop-while #(not (vector? %)) steps)]
             (merge (vec intro) (flow-fork (first remaining) (rest remaining))))))))

(defn flow-fork
  [paths steps]
  (map #(apply flow (vec (concat % steps))) paths))

(def test-flow (flow load-edi-file
                     edi/parse-edi
                     (edi/get-parser-by-format edi/utilmd-format)
                     edi/to-xml
                     (write-xml-file "/home/cp/test-dir/out/")))

(defn set-last-run
  [flow-name]
  (let [flow-key (keyword flow-name)]
    (dosync (ref-set *flows* (assoc @*flows* flow-key
                                    (assoc (flow-key @*flows*) :last-run
                                           (System/currentTimeMillis)))))))

(defn mod-thread-count
  [flow-name fn]
  (let [flow-key (keyword flow-name)]
    (dosync (ref-set *flows* (assoc @*flows* flow-key
                                    (assoc (flow-key @*flows*) :thread-count
                                           (fn (-> @*flows* flow-key :thread-count))))))))

(defn register-flow
  [name interval initializer flow]
  (dosync (ref-set *flows* (assoc @*flows* (keyword name)
                                  (hash-map :name name
                                            :interval interval
                                            :last-run 0
                                            :thread-count 0
                                            :initializer initializer
                                            :flow flow)))))

(defn process-flow
  [a flow-name flow data]
  (log/info "Processing flow")
  (if (coll? (first flow))
    (doseq [path (first flow)]
      (do
        (mod-thread-count flow-name inc)
        (send-off (agent 0) process-flow flow-name path data)))
    (if (= 1 (count flow))
      (do
        ((first flow) data)
        (mod-thread-count flow-name dec))
      (send-off *agent* process-flow flow-name (rest flow) ((first flow) data)))))

(defn initialize-flow
  [a flow]
  (log/info "Checking flow for initializing")
  (let [data ((:initializer flow))]
    (if (not (nil? data))
      (do
        (log/info "Send off thread")
        (mod-thread-count (:name flow) inc)
        (set-last-run (:name flow))
        (send-off *agent* process-flow (:name flow) (:flow flow) data)))))

(defn monitor
  [a]
  (Thread/sleep 10000)
  (log/info "Monitoring")
  (doseq [flow (vals @*flows*)]
    (log/info (str "Checking flow: " (:name flow)))
    (if (and (> (- (System/currentTimeMillis) (:last-run flow))
                (:interval flow))
             (= 0 (:thread-count flow)))
      (send-off (agent 0) initialize-flow flow))
    (log/info "Status for flow: " (:name flow) " - Last run: " (:last-run flow)
              " - Running: " (:thread-count flow)))
  (send-off *agent* monitor))
      
(defn -main
  []
  (log/info "Starting service")
  (register-flow :test-flow 10000 #(get-file-from-dir "/home/cp/test-dir/in/") test-flow)
  (send-off (agent 0) monitor))