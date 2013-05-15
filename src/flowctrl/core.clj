(ns flowctrl.core
  (:require [criterium.core :as crit]
            [clojure.tools.logging :as log]
            [flowctrl.parse-edi :as edi]
            [clojure.data.xml :as xml]
            [clojure.string :as string])
  (:gen-class))

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defn get-files-from-dir
  [dir]
  (rest (file-seq (clojure.java.io/file dir))))

(defn get-file-from-dir
  [dir]
  (first (rest (file-seq (clojure.java.io/file dir)))))

(defn load-disk-file
  [file]
  (let [data (slurp (.getAbsolutePath file))]
    (clojure.java.io/delete-file file)
    data))

(defn load-and-move-file
  [file]
  (let [file-path (.getAbsolutePath file)]
    (clojure.java.io/copy (clojure.java.io/file file-path)
                          (clojure.java.io/file (str (.getParent file) "/" (uuid) "."
                                                     (last (string/split file-path #"\.")))))
    (load-disk-file file)))

(defn write-file
  [dir extension]
  (fn [data]
    (spit (str dir (uuid) "." extension) data)))

(defn write-to-dev-null
  [dir]
  (fn [data]
    (spit dir data)))

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

(def edi-flow (flow load-and-move-file
                    edi/parse-edi
                    (edi/get-parser-by-format edi/utilmd-format)
                    edi/convert-to-utilmd-xml
                    (write-to-dev-null "/dev/null/")))

(def xml-flow (flow load-and-move-file
                    xml/parse-str
                    edi/create-utilmd-edi-struct
                    edi/edi-to-str
                    (write-to-dev-null "/dev/null/")))

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
  (if (coll? (first flow))
    (doseq [path (first flow)]
      (do
        (mod-thread-count flow-name inc)
        (send (agent 0) process-flow flow-name path data)))
    (if (= 1 (count flow))
      (do
        ((first flow) data)
        (mod-thread-count flow-name dec))
      (send *agent* process-flow flow-name (rest flow) ((first flow) data)))))

(defn process-single-flow
  [flow data]
  (reduce #(%2 %1) data flow))

(defn process-chunked-flow
  [a flow-name flow chunk]
  (try
    (doall (map #(process-single-flow flow %) chunk))
    (log/info (str flow-name " processed"))
    (catch Exception ex (log/warn (str "Exception in flow: " flow-name " - " ex)))
    (finally (mod-thread-count flow-name dec))))

(defn initialize-flow
  [a flow]
  (mod-thread-count (:name flow) inc)
  (let [data ((:initializer flow))]
    (if (not (empty? data))
      (do
        (set-last-run (:name flow))
        (send *agent* process-chunked-flow (:name flow) (:flow flow) (first (partition 5000 5000 nil data))))
      (mod-thread-count (:name flow) dec))))

(defn monitor
  [a]
  (doseq [flow (vals @*flows*)]
    (if (and (> (- (System/currentTimeMillis) (:last-run flow))
                (:interval flow))
             (= 0 (:thread-count flow)))
      (send (agent 0) initialize-flow flow)))
  (Thread/sleep 5000)
  (send *agent* monitor))
      
(defn -main
  []
  (log/info "Startup")
  (register-flow :edi-flow 10000 #(get-files-from-dir "/tmp/openedix/in/edi/") edi-flow)
  (register-flow :xml-flow 10000 #(get-files-from-dir "/tmp/openedix/in/xml/") xml-flow)
  (send (agent 0) monitor))