(ns flowctrl.parse-edi
  (:use [net.cgrand.enlive-html :exclude [flatten]])
  (:require [clojure.string :as string]
            [criterium.core :as crit]
            [clojure.data.xml :as xml]))

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

(defn parse-edi
  [edi-message]
  (map edi-data-elements (edi-segments edi-message)))

(defn combine-values
  [data]
  (if (coll? data)
    (string/join ":" data)
    data))

(defn edi-to-str
  [data]
  (str (string/join "'" (map #(string/join "+" (map combine-values %)) data)) "'"))

(def mscons "UNA:+.? 'UNB+UNOC:3+5790000681327:14+5790001687137:14+091029:1027+L091029102749S++DK-CUS++1+D'UNH+1+MSCONS:D:96A:ZZ:E2DK02+DK-BT-007-002'BGM+Z01::260+L091029102749S+9+AB'DTM+137:200910290925:203'DTM+163:200812312300:203'DTM+164:200910232200:203'DTM+ZZZ:0:805'NAD+FR+5790000681327::9'NAD+DO+5790001687137::9'UNS+D'NAD+XX'LOC+90+571313134802045214::9'LIN+1++9011:::DK'MEA+AAZ++KWH'QTY+136:9127'DTM+324:200812312300200910232200:Z13'CCI+++Z04'MEA+SV++ZZ:1'LIN+2++9016:::DK'MEA+AAZ++KWH'QTY+136:9127'DTM+324:200812312300200910232200:Z13'CCI+++Z04'MEA+SV++ZZ:1'LIN+3++9015:::DK'MEA+AAZ++KWH'QTY+31:12444'CCI+++Z04'MEA+SV++ZZ:1'CNT+1:30698'UNT+30+1'UNZ+1+L091029102749S'")

(def utilmd "UNA:+.? 'UNB+UNOC:3+5790000392261:14+5790000710133:14+130218:1436+L1302181436076++DK-CUS++1+DK'UNH+1+UTILMD:D:02B:UN:E5DK02+DK-BT-004-002'BGM+E07::260+L1302181436076+9+AB'DTM+137:201302181334:203'DTM+735:?+0000:406'MKS+23+E01::260'NAD+MR+5790000710133::9'NAD+MS+5790000392261::9'IDE+24+L1302181436076E571313124401206383'DTM+92:201209302200:203'DTM+157:201302172300:203'DTM+752:1231:106'STS+7++Z04::DK'LOC+172+571313124401206383::9'CCI+++E02::260'CAV+E01::260'CCI+++E15::260'CAV+E22::260'SEQ++1'QTY+31:503:KWH'NAD+DDK+5790000701278::9'NAD+IT++++Højbovej::8:630;1069;8;;+Vejle++7100+DK'NAD+UD+++Hans Mandøe:E Glæsner'NAD+DDQ+5790000710133::9'UNT+24+1'UNZ+1+L1302181436076'")

(def utilmd-end "UNA:+.? 'UNB+UNOC:3+5790001687137:14+5790000705689:14+091109:0126+26710++DK-CUS+++DK'UNH+26710+UTILMD:D:02B:UN:E5DK02+DK-BT-003-002'BGM+432+2449+9+NA'DTM+137:200911090122:203'DTM+735:?+0000:406'MKS+23+E01::260'NAD+MR+5790000705689::9'NAD+MS+5790001687137::9'IDE+24+2449'DTM+93:200911302300:203'STS+7++E20::260'LOC+172+571313174115162192::9'UNT+12+26710'UNZ+1+26710'")

(defn format-line
  [segment name keys]
  (hash-map :segment segment :name name :keys keys))

(defn format-nesting
  [segment name end-segment format-group]
  (hash-map :segment segment :name name :end-segment end-segment :nesting format-group))

(defn create-format
  [& format-lines]
  (loop [result []
         lines format-lines]
    (let [line (first lines)]
      (cond
       (empty? lines) result
       (= 3 (count line)) (recur (conj result (apply format-line line)) (rest lines))
       :else (recur (conj result (let [[segment name end-segment & flines] line]
                                   (format-nesting segment name end-segment
                                                   (apply create-format flines))))
                    (rest lines))))))

(def utilmd-format
  (create-format
   [:UNA :UNA [:na1 :na2]]
   [:UNB :UNB [:na1 :na2 :na3 :na4 :na5 :na6 :na7 :na8 :na9 :na10 :na11 :na12 :na13 :na14]]
   [:UNH :messages :UNZ 
    [:UNH :message [:message-reference :message-type :unknown1 :unknown2 :unknown3 :version :business-transaction]]
    [:BGM :BGM [:document-type :na :organisation-code :message-id :original :ack]]
    [:DTM :message-date [:type :timestamp :format]]
    [:DTM :message-timezone [:type :deviation :format]]
    [:MKS :market [:type :na1 :na2 :org-code]]
    [:NAD :recipient [:type :id :na :id-type]]
    [:NAD :sender [:type :id :na :id-type]]
    [:IDE :transactions :UNT
     [:IDE :transaction [:type :id]]
     [:DTM :validity-date [:type :timestamp :format]]
     [:DTM :contract-date [:type :timestamp :format]]
     [:DTM :reading-date [:type :timestamp :format]]
     [:STS :reason [:type :code :org-code :na :org-group]]
     [:LOC :installation [:type :na :id :id-type]]
     [:RFF :reference [:type :id]]
     [:CCI :method-code [:na1 :na2 :settlement :na3 :org-code]]
     [:CAV :method [:type :na :org-code]]
     [:CCI :status-code [:na1 :na2 :physical :na3 :org-code]]
     [:CAV :status [:type :na :org-code]]
     [:SEQ :SEQ [:na :reg-no]]
     [:QTY :estimated-value [:type :quantity :measure]]
     [:NAD :balance-responsible [:org :id :na :type-code]]
     [:NAD :metering-point [:type :na1 :na2 :na3 :street-name1 :street-name2 :house-number :city :na4 :zip-code :country]]
     [:NAD :consumer [:type :na1 :na2 :name :name2]]
     [:NAD :balance-supplier [:type :id :na :type-code]]]
    [:UNT :message-end [:number-og-segments :ref-no]]]
   [:UNZ :UNZ [:na1 :na2]]))

(def mscons-format
  (create-format
   [:UNA :UNA [:na1 :na2]]
   [:UNB :UNB [:na1 :na2 :na3 :na4 :na5 :na6 :na7 :na8 :na9 :na10 :na11 :na12 :na13 :na14]]
   [:UNH :messages :UNZ
    [:UNH :message [:message-reference :message-type :unknown1 :unknown2 :unknown3 :version :business-transaction]]
    [:BGM :BGM [:document-type :na :organisation-code :message-id :original :ack]]
    [:DTM :message-date [:type :timestamp :format]]
    [:DTM :processing-date1 [:type :timestamp :format]]
    [:DTM :processing-date2 [:type :timestamp :format]]
    [:DTM :message-timezone [:type :deviation :format]]
    [:NAD :recipient [:type :id :na :id-type]]
    [:NAD :sender [:type :id :na :id-type]]
    [:UNS :UNS [:na]]
    [:NAD :NAD [:na]]
    [:LOC :metering-point [:type :id :na :type-code]]
    [:LIN :products :CNT
     [:LIN :product [:line-no :na :code :na1 :na2 :org-code]]
     [:MEA :measure [:type :na :unit]]
     [:CUX :currency [:qualifier :type]]
     [:QTY :quantity [:type :amount]]
     [:DTM :reading-time [:type :timestamp :format]]
     [:CCI :reason-code [:na1 :na2 :type]]
     [:MEA :reason [:na1 :na2 :na3 :code]]]
    [:CNT :CNT [:type :sum]]
    [:UNT :message-end [:number-og-segments :ref-no]]]
   [:UNZ :UNZ [:na1 :na2]]))

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

(defn- parse
  [data format end-segment]
  (loop [lines data
         format-lines format
         result '()
         temp {}]
    (let [line (first lines)
          f (first format-lines)]
      (cond
       (end-parse? lines end-segment) (create-result lines (conj result temp))
       (start-nesting? line f) (let [rs (parse lines (:nesting f)
                                                         (:end-segment f))]
                                 (recur (:lines rs) (rest format-lines) result
                                        (assoc temp (:name f) (:result rs))))
       (empty? format-lines) (recur lines format (conj result temp) {})
       (= (:segment f) (:name line)) (recur (rest lines) (rest format-lines)
                                            result
                                            (add-element temp line f))
       :else (recur lines (rest format-lines) result temp)))))

(defn parse-by-format
  [data format]
  (first (:result (parse data format nil))))

(defn get-parser-by-format
  [format]
  (fn [data]
    (parse-by-format data format)))

(defn convert-to-utilmd-xml
  [data]
  (let [data (first (:messages data))]
    (xml/element :UTILMD {}
                 (xml/element :UtilmdHeader {}
                              (xml/element :IGVersion {:S009_0065 (:message-type (:message data))
                                                       :S009_0052 (:unknown1 (:message data))
                                                       :S009_0054 (:unknown2 (:message data))
                                                       :S009_0051 (:unknown3 (:message data))
                                                       :S009_0057 (:version (:message data))
                                                       :T0068 (:business-transaction (:message data))}
                                           (:message-reference (:message data)))
                              (xml/element :MessageName {:C002_1001 (:document-type (:BGM data))
                                                         :C002_3055 (:organisation-code (:BGM data))}
                                           (:document-type (:BGM data)))
                              (xml/element :MessageId {} (:message-id (:BGM data)))
                              (xml/element :MessageFunction {:T1225 (:original (:BGM data))})
                              (xml/element :RequestForAck {:T4343 (:ack (:BGM data))})
                              (xml/element :MessageDate {:C507_2005 (:type (:message-date data))
                                                         :C507_2379 (:format (:message-date data))}
                                           (:timestamp (:message-date data)))
                              (xml/element :TimeZone {:C507_2005 (:type (:message-timezone data))
                                                      :C507_2379 (:format (:message-timezone data))
                                                      :C507_2380 (:deviation (:message-timezone data))}
                                           (:deviation (:message-timezone data)))
                              (xml/element :Market {:T7293 (:type (:market data))
                                                    :C332_3496 (:na1 (:market data))
                                                    :C332_3055 (:org-code (:market data))})
                              (xml/element :MessageRecipent {:T3035 (:type (:recipient data))
                                                             :C082_3055 (:id-type (:recipient data))}
                                           (:id (:recipient data)))
                              (xml/element :MessageSender {:T3035 (:type (:sender data))
                                                           :C082_3055 (:id-type (:sender data))}
                                           (:id (:sender data))))
                 (xml/element :Transactions {}
                              (for [transaction (:transactions data)]
                                (xml/element :Transaction {}
                                             (xml/element :TransactionId {:T7495 (:type (:transaction transaction))}
                                                          (:id (:transaction transaction)))
                                             (xml/element :MeteringPointId {:T3227 (:type (:installation transaction))
                                                                            :C517_3055 (:id-type (:installation transaction))}
                                                          (:na (:installation transaction)))
                                             (xml/element :ContractStartDate {:C507_2005 (:type (:contract-date transaction))
                                                                              :C507_2379 (:format (:contract-date transaction))}
                                                          (:timestamp (:contract-date transaction)))
                                             (xml/element :ValidityStartDate {:C507_2005 (:type (:validity-date transaction))
                                                                              :C507_2379 (:format (:validity-date transaction))}
                                                          (:timestamp (:validity-date transaction)))
                                             (xml/element :NextScheduledReadings {}
                                                          (xml/element :NextScheduledReading {:C507_2005 (:type (:reading-date transaction))
                                                                                              :C507_2379 (:format (:reading-date transaction))}
                                                                       (:timestamp (:reading-date transaction))))
                                             (xml/element :ReasonForTransaction {:C601_9015 (:type (:reason transaction))
                                                                                 :C556_3055 (:org-group (:reason transaction))}
                                                          (:org-code (:reason transaction)))
                                             (xml/element :PhysicalStatus {:C240_7037 (:physical (:status-code transaction))
                                                                           :C240_3055 (:org-code (:status-code transaction))
                                                                           :C889_3055 (:org-code (:status transaction))}
                                                          (:type (:status transaction)))
                                             (xml/element :SettlementMethod {:C240_7037 (:settlement (:method-code transaction))
                                                                             :C240_3055 (:org-code (:method-code transaction))
                                                                             :C889_3055 (:org-code (:method transaction))}
                                                          (:type (:method transaction)))
                                             (xml/element :EstimatedAnnualVolume {:C286_1050 (:reg-no (:SEQ transaction))
                                                                                  :C186_6063 (:type (:estimated-value transaction))
                                                                                  :C186_6411 (:measure (:estimated-value transaction))}
                                                          (:quantity (:estimated-value transaction)))
                                             (xml/element :BalanceResponsible {:T3035 (:org (:balance-responsible transaction))
                                                                               :C082_3055 (:type-code (:balance-responsible transaction))}
                                                          (:id (:balance-responsible transaction)))
                                             (xml/element :BalanceSupplier {:T3035 (:org (:balance-supplier transaction))
                                                                            :C082_3055 (:type-code (:balance-supplier transaction))}
                                                          (:id (:balance-supplier transaction)))
                                             (xml/element :Consumer {:T3035 (:type (:consumer transaction))}
                                                          (xml/element :PartyName {} (:name (:consumer transaction)))
                                                          (xml/element :CityName {})
                                                          (xml/element :ZipCode {})
                                                          (xml/element :CountryCode {}))
                                             (xml/element :MeterLocationAddress {:T3035 (:type (:metering-point transaction))}
                                                          (xml/element :StreetName {} (:street-name1 (:metering-point transaction)))
                                                          (xml/element :StreetName2 {} (:street-name2 (:metering-point transaction)))
                                                          (xml/element :HouseNumber {} (:house-number (:metering-point transaction)))
                                                          (xml/element :RoadCode {}
                                                                       (xml/element :CountryCode {})
                                                                       (xml/element :StreetCode {})
                                                                       (xml/element :HouseNumber {})
                                                                       (xml/element :HouseFloor {})
                                                                       (xml/element :DoorNo {}))
                                                          (xml/element :CityName {} (:city (:metering-point transaction)))
                                                          (xml/element :ZipCode {} (:zip-code (:metering-point transaction)))
                                                          (xml/element :CountryCode {} (:country (:metering-point transaction))))))))))

(defn get-by-tag
  [tag data]
  (first (filter #(= tag (:tag %)) (:content data))))

(defn get-by-tags
  [& args]
  (reduce #(get-by-tag %2 %1) (last args) (drop-last args)))

(defn convert-to-utilmde07-edi
  [xml]
  (let [ig (get-by-tags :UtilmdHeader :IGVersion xml)
        ig-attr (:attrs ig)
        msg-name (get-by-tags :UtilmdHeader :MessageName xml)
        msg-id (get-by-tags :UtilmdHeader :MessageId xml)
        msg-fn (get-by-tags :UtilmdHeader :MessageFunction xml)
        ack (get-by-tags :UtilmdHeader :RequestForAck xml)
        msg-date (get-by-tags :UtilmdHeader :MessageDate xml)
        msg-timezone (get-by-tags :UtilmdHeader :TimeZone xml)
        mks  (get-by-tags :UtilmdHeader :Market xml)
        recipient (get-by-tags :UtilmdHeader :MessageRecipient xml)
        sender (get-by-tags :UtilmdHeader :MessageSender xml)]
    [["UNA:+.?"]
     ["UNB" ["UNOC" 3] [(apply str (:content sender)) 14] [(apply str (:content recipient)) 14] ["091022" "0739"] (apply str (:content ig)) "" "DK-CUS" "" "" "DK"]
     ["UNH" (apply str (:content ig)) [(:S009_0065 ig-attr) (:S009_0052 ig-attr) (:S009_0054 ig-attr) (:S009_0051 ig-attr) (:S009_0057 ig-attr)] (:T0068 ig-attr)]
     ["BGM" [(apply str (:content msg-name)) "" (:C002_3055 (:attrs msg-name))] (apply str (:content msg-id)) (:T1225 (:attrs msg-fn)) (:T4343 (:attrs ack))]
     ["DTM" [(:C507_2005 (:attrs msg-date)) (apply str (:content msg-date)) (:C507_2379 (:attrs msg-date))]]
     ["DTM" [(:C507_2005 (:attrs msg-timezone)) (apply str (:content msg-timezone)) (:C507_2379 (:attrs msg-timezone))]]
     ["MKS" (:T7293 (:attrs mks)) [(:C332_3496 (:attrs mks)) "" (:C332_3055 (:attrs mks))]]
     ["NAD" (:T3035 (:attrs recipient)) [(apply str (:content recipient)) "" (:C082_3055 (:attrs recipient))]]
     ["NAD" (:T3035 (:attrs sender)) [(apply str (:content sender)) "" (:C082_3055 (:attrs sender))]]]))
