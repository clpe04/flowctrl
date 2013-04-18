(ns flowctrl.core
  (:require [criterium.core :as crit]
            [clojure.tools.logging :as log]))

(defn get-files-from-dir
  [dir]
  (rest (file-seq (clojure.java.io/file dir))))

(defn files-in-dir?
  [dir]
  (> (count (get-files-from-dir dir)) 0))

(defn load-file
  [file]
  (slurp (.getAbsolutePath file)))

(defn write-file
  [filepath]
  (fn [data]
    (spit filepath data)))

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
  ([a flow]
     (send-off *agent* process-flow (rest flow) ((first flow))))
  ([a flow data]
     (if (coll? (first flow))
       (doseq [path (first flow)]
         (send-off (agent 0) process-flow path data))
       (if (= 1 (count flow))
         ((first flow) data)
         (send-off *agent* process-flow (rest flow) ((first flow) data))))))

(defn monitor
  [a]
  (Thread/sleep 10000)
  (log/info "Monitoring")
  (doseq [flow *flows*]
    (if (and (> (- (System/currentTimeMillis) (:last-run flow))
                (:interval flow))
             (:initializer flow))
      (send-off (agent 0) process-flow flow))
    (log/info "Status for flow: " (:name flow) " - Last run: " (:last-run flow)
              " - Running: " (:thread-count flow)))
  (send-off *agent* monitor))
      
(defn -main
  []
  (log/info "Starting service")
  (send-off (agent 0) monitor))