(ns flowctrl.parse-edi
  (:require [clojure.string :as string]))

(defn edi-segments [edi-message]
  "Returns a vector containing all segments"
  (string/split edi-message #"(?<!\?)\'"))

(defn edi-seperator? [character]
  (some true? (map #(= % character) '(\+ \:))))

(defn edi-data-elements [edi-segment]
  "Returns a vector containing all data elements in a segment"
  (let [rs (string/split edi-segment #"(?<!\?)[\+\:]")
        line (if (edi-seperator? (last edi-segment))
               (conj rs "")
               rs)]
    (hash-map :name (keyword (first line)) :data (rest line))))

(def mscons "UNA:+.? 'UNB+UNOC:3+5790000681327:14+5790001687137:14+091029:1027+L091029102749S++DK-CUS++1+D'UNH+1+MSCONS:D:96A:ZZ:E2DK02+DK-BT-007-002'BGM+Z01::260+L091029102749S+9+AB'DTM+137:200910290925:203'DTM+163:200812312300:203'DTM+164:200910232200:203'DTM+ZZZ:0:805'NAD+FR+5790000681327::9'NAD+DO+5790001687137::9'UNS+D'NAD+XX'LOC+90+571313134802045214::9'LIN+1++9011:::DK'MEA+AAZ++KWH'QTY+136:9127'DTM+324:200812312300200910232200:Z13'CCI+++Z04'MEA+SV++ZZ:1'LIN+2++9016:::DK'MEA+AAZ++KWH'QTY+136:9127'DTM+324:200812312300200910232200:Z13'CCI+++Z04'MEA+SV++ZZ:1'LIN+3++9015:::DK'MEA+AAZ++KWH'QTY+31:12444'CCI+++Z04'MEA+SV++ZZ:1'CNT+1:30698'UNT+30+1'UNZ+1+L091029102749S'")

(def utilmd "UNA:+.? 'UNB+UNOC:3+5790000392261:14+5790000710133:14+130218:1436+L1302181436076++DK-CUS++1+DK'UNH+1+UTILMD:D:02B:UN:E5DK02+DK-BT-004-002'BGM+E07::260+L1302181436076+9+AB'DTM+137:201302181334:203'DTM+735:?+0000:406'MKS+23+E01::260'NAD+MR+5790000710133::9'NAD+MS+5790000392261::9'IDE+24+L1302181436076E571313124401206383'DTM+92:201209302200:203'DTM+157:201302172300:203'DTM+752:1231:106'STS+7++Z04::DK'LOC+172+571313124401206383::9'CCI+++E02::260'CAV+E01::260'CCI+++E15::260'CAV+E22::260'SEQ++1'QTY+31:503:KWH'NAD+DDK+5790000701278::9'NAD+IT++++Højbovej::8:630;1069;8;;+Vejle++7100+DK'NAD+UD+++Hans Mandøe:E Glæsner'NAD+DDQ+5790000710133::9'UNT+24+1'UNZ+1+L1302181436076'")

(def utilmd-end "UNA:+.? 'UNB+UNOC:3+5790001687137:14+5790000705689:14+091109:0126+26710++DK-CUS+++DK'UNH+26710+UTILMD:D:02B:UN:E5DK02+DK-BT-003-002'BGM+432+2449+9+NA'DTM+137:200911090122:203'DTM+735:?+0000:406'MKS+23+E01::260'NAD+MR+5790000705689::9'NAD+MS+5790001687137::9'IDE+24+2449'DTM+93:200911302300:203'STS+7++E20::260'LOC+172+571313174115162192::9'UNT+12+26710'UNZ+1+26710'")

(def utilmd-format '({:segment :UNA :name :UNA :keys [:na1 :na2]}
              {:segment :UNB :name :UNB :keys [:na1 :na2 :na3 :na4 :na5 :na6 :na7 :na8 :na9 :na10 :na11 :na12 :na13 :na14]}
              {:segment :UNH :name :messages :end-segment :UNZ :nesting
               ({:segment :UNH :name :message :keys [:message-reference :message-type :unknown1 :unknown2 :unknown3 :version :business-transaction]}
                {:segment :BGM :name :BGM :keys [:document-type :na :organisation-code :message-id :original :ack]}
                {:segment :DTM :name :message-date :keys [:type :timestamp :format]}
                {:segment :DTM :name :message-timezone :keys [:type :deviation :format]}
                {:segment :MKS :name :market :keys [:type :na1 :na2 :org-code]}
                {:segment :NAD :name :recipient :keys [:type :id :na :id-type]}
                {:segment :NAD :name :sender :keys [:type :id :na :id-type]}
                {:segment :IDE :name :transactions :end-segment :UNT :nesting
                 ({:segment :IDE :name :transaction :keys [:type :id]}
                  {:segment :DTM :name :contract-date :keys [:type :timestamp :format]}
                  {:segment :DTM :name :validity-date :keys [:type :timestamp :format]}
                  {:segment :DTM :name :reading-date :keys [:type :timestamp :format]}
                  {:segment :STS :name :reason :keys [:type :code :org-code :org-group]}
                  {:segment :LOC :name :installation :keys [:type :na :id :id-type]}
                  {:segment :RFF :name :reference :keys [:type :id]}
                  {:segment :CCI :name :method-code :keys [:na1 :na2 :settlement :na3 :org-code]}
                  {:segment :CAV :name :method :keys [:type :na :org-code]}
                  {:segment :CCI :name :status-code :keys [:na1 :na2 :physical :na3 :org-code]}
                  {:segment :CAV :name :status :keys [:type :na :org-code]}
                  {:segment :SEQ :name :SEQ :keys [:na :reg-no]}
                  {:segment :QTY :name :estimated-value :keys [:type :quantity :measure]}
                  {:segment :NAD :name :balance-responsible :keys [:org :id :na :type-code]}
                  {:segment :NAD :name :metering-point :keys [:type :na1 :na2 :na3 :street-name1 :street-name2 :house-number :coded-address :city :na4 :zip-code :country]}
                  {:segment :NAD :name :consumer :keys [:type :na1 :na2 :name :name2]}
                  {:segment :NAD :name :balance-supplier :keys [:type :id :na :type-code]})}
                {:segment :UNT :name :message-end :keys [:number-og-segments :ref-no]})}
              {:segment :UNZ :name :UNZ :keys [:na1 :na2]}))
                    
(defn end-parse?
  [lines end-segment]
  (or (empty? lines) (= (:name (first lines)) end-segment)))

(defn create-result
  [lines result]
  (hash-map :lines lines :result result))

(defn start-nesting?
  [line format-line]
  (and (= (:segment format-line) (:name line)) (not (nil? (:nesting format-line)))))

(defn add-element
  [collection line format-line]
  (assoc collection (:name format-line) (zipmap (:keys format-line) (:data line))))

(defn parse-by-format
  [data format end-segment]
  (loop [lines data
         format-lines format
         result '()
         temp {}]
    (let [line (first lines)
          f (first format-lines)]
      (cond
       (end-parse? lines end-segment) (create-result lines (conj result temp))
       (start-nesting? line f) (let [rs (parse-by-format lines (:nesting f)
                                                         (:end-segment f))]
                                 (recur (:lines rs) (rest format-lines) result
                                        (assoc temp (:name f) (:result rs))))
       (empty? format-lines) (recur lines format (conj result temp) {})
       (= (:segment f) (:name line)) (recur (rest lines) (rest format-lines)
                                            result
                                            (add-element temp line f))
       :else (recur lines (rest format-lines) result temp)))))