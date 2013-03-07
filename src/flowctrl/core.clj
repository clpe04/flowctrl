(ns flowctrl.core
  (:require [criterium.core :as crit]))

(defstruct step :attr :fn :next)

(defn split-lines
  [text]
  (.split text "\\n+"))

(defn split-words
  [text]
  (.split text "\\s+"))

(defn uppercase
  [text]
  (.toUpperCase text))

(defn lowercase
  [text]
  (.toLowerCase text))

(defn trim
  [text]
  (.trim text))

(defn get-file-text
  [file]
  (slurp file))

(def step-4 (struct step {} #(map lowercase %) nil))

(def step-3 (struct step {} #(map uppercase %) nil))

(def step-2 (struct step {} split-lines [step-3 step-4]))

(def step-1 (struct step {} get-file-text [step-2]))
  
(declare run-structure call-next)

(defn run-structure
  [data structs]
  (map #(call-next data %) structs))

(defn call-next
  [data struct]
  (let [result ((:fn struct) data)]
    (cond
     (nil? (:next struct)) result
     :else (run-structure result (:next struct)))))

(defn run-pipeline
  [data struct]
  (trampoline run-structure data struct))

(def test-struc
  {get-file-text
   {split-lines
    {#(map split-words %) nil
     #(map trim %)
     {#(map uppercase %) nil}}
    split-words
    {#(map uppercase %) nil}
    uppercase nil}})

(defn get-files-from-dir
  [dir]
  (drop 1 (file-seq (clojure.java.io/file dir))))