(ns flowctrl.parse-edi
  (:use [net.cgrand.enlive-html :exclude [flatten]])
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

(defn parse-edi
  [edi-message]
  (map edi-data-elements (edi-segments edi-message)))

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

; xml-resource is nessesary because the standard parser (html) lowercases attribute names
(deftemplate utilmd-test (xml-resource "utilmd.xml") [data]
  [:UtilmdHeader] #(at %
                       [:IGVersion] (do->
                                     (set-attr :S009_0065 (:message-type (:message data)))
                                     (set-attr :S009_0052 (:unknown1 (:message data)))
                                     (set-attr :S009_0054 (:unknown2 (:message data)))
                                     (set-attr :S009_0051 (:unknown3 (:message data)))
                                     (set-attr :S009_0057 (:version (:message data)))
                                     (set-attr :T0068 (:business-transaction (:message data)))
                                     (content (:message-reference (:message data))))
                       [:MessageName] (do->
                                       (set-attr :C002_1001 (:document-type (:BGM data)))
                                       (set-attr :C002_3055 (:organisation-code (:BGM data)))
                                       (content (:document-type (:BGM data))))
                       [:MessageId] (content (:message-id (:BGM data)))
                       [:MessageFunction] (set-attr :T1225 (:original (:BGM data)))
                       [:RequestForAck](set-attr :T4343 (:ack (:BGM data))) ;TODO check
                       [:MessageDate] (do->
                                       (set-attr :C507_2005 (:type (:message-date data)))
                                       (set-attr :C507_2379 (:format (:message-date data)))
                                       (content (:timestamp (:message-date data))))
                       [:TimeZone] (do->
                                    (set-attr :C507_2005 (:type (:message-timezone data)))
                                    (set-attr :C507_2379 (:format (:message-timezone data)))
                                    (set-attr :C507_2380 (:deviation (:message-timezone data)))
                                    (content (:deviation (:message-timezone data))))
                       [:Market] (do->
                                  (set-attr :T7293 (:type (:market data)))
                                  (set-attr :C332_3496 (:na1 (:market data)))
                                  (set-attr :C332_3055 (:org-code (:market data))))
                       [:MessageRecipent](do->
                                          (set-attr :T3035 (:type (:recipient data)))
                                          (set-attr :C082_3055 (:id-type (:recipient data)))
                                          (content (:id (:recipient data))))
                       [:MessageSender] (do->
                                         (set-attr :T3035 (:type (:sender data)))
                                         (set-attr :C082_3055 (:id-type (:sender data)))
                                         (content (:id (:sender data)))))
  [:Transactions :Transaction] (clone-for [transaction (:transactions data)]
                                          [:TransactionId] (do->
                                                            (set-attr :T7495 (:type (:transaction transaction)))
                                                            (content (:id (:transaction transaction))))
                                          [:MeteringPointId] (do->
                                                              (set-attr :T3227 (:type (:installation transaction)))
                                                              (set-attr :C517_3055 (:id-type (:installation transaction)))
                                                              (content (:na (:installation transaction))))
                                          [:ContractStartDate] (do->
                                                                (set-attr :C507_2005 (:type (:contract-date transaction)))
                                                                (set-attr :C507_2379 (:format (:contract-date transaction)))
                                                                (content (:timestamp (:contract-date transaction))))
                                          [:ValidityStartDate] (do->
                                                                (set-attr :C507_2005 (:type (:validity-date transaction)))
                                                                (set-attr :C507_2379 (:format (:validity-date transaction)))
                                                                (content (:timestamp (:validity-date transaction))))
                                          [:NextScheduledReadings :NextScheduledReading] (do->
                                                                (set-attr :C507_2005 (:type (:reading-date transaction)))
                                                                (set-attr :C507_2379 (:format (:reading-date transaction)))
                                                                (content (:timestamp (:reading-date transaction))))
                                          [:ReasonForTransaction] (do->
                                                                   (set-attr :C601_9015 (:type (:reason transaction)))
                                                                   (set-attr :C556_3055 (:org-group (:reason transaction)))
                                                                   (content (:org-code (:reason transaction))))
                                          [:PhysicalStatus] (do->
                                                             (set-attr :C240_7037 (:physical (:status-code transaction)))
                                                             (set-attr :C240_3055 (:org-code (:status-code transaction)))
                                                             (set-attr :C889_3055 (:org-code (:status transaction)))
                                                             (content (:type (:status transaction))))
                                          [:SettlementMethod] (do->
                                                             (set-attr :C240_7037 (:settlement (:method-code transaction)))
                                                             (set-attr :C240_3055 (:org-code (:method-code transaction)))
                                                             (set-attr :C889_3055 (:org-code (:method transaction)))
                                                             (content (:type (:method transaction))))
                                          [:EstimatedAnnualVolume] (do->
                                                                    (set-attr :C286_1050 (:reg-no (:SEQ transaction)))
                                                                    (set-attr :C186_6063 (:type (:estimated-value transaction)))
                                                                    (set-attr :C186_6411 (:measure (:estimated-value transaction)))
                                                             (content (:quantity (:estimated-value transaction))))
                                          [:BalanceResponsible] (do->
                                                                 (set-attr :T3035 (:org (:balance-responsible transaction)))
                                                                 (set-attr :C082_3055 (:type-code (:balance-responsible transaction)))
                                                                 (content (:id (:balance-responsible transaction))))
                                          [:BalanceSupplier] (do->
                                                              (set-attr :T3035 (:org (:balance-supplier transaction)))
                                                              (set-attr :C082_3055 (:type-code (:balance-supplier transaction)))
                                                              (content (:id (:balance-supplier transaction))))
                                          [:Consumer] (do->
                                                       (set-attr :T3035 (:type (:consumer transaction))))
                                          [:Consumer :PartyName] (do->
                                                                  (content (:name (:consumer transaction))))
                                          [:MeterLocationAddress] (do->
                                                                   (set-attr :T3035 (:type (:metering-point transaction))))
                                          [:MeterLocationAddress :StreetName] (do->
                                                                               (content (:street-name1 (:metering-point transaction))))
                                          [:MeterLocationAddress :StreetName2] (do->
                                                                                (content (:street-name2 (:metering-point transaction))))
                                          [:MeterLocationAddress :HouseNumber] (do->
                                                                                (content (:house-number (:metering-point transaction))))
                                          [:MeterLocationAddress :CityName] (do->
                                                                             (content (:city (:metering-point transaction))))
                                          [:MeterLocationAddress :ZipCode] (do->
                                                                            (content (:zip-code (:metering-point transaction))))
                                          [:MeterLocationAddress :CountryCode] (do->
                                                                               (content (:country (:metering-point transaction))))
                                          ))

(defn to-xml
  [data]
  (apply str (utilmd-test (first (:messages data)))))

(comment
  (apply str (utilmd-test (first (:messages (parse-by-format (parse-edi (slurp "/home/cp/test-dir/in/stamdata.edi")) utilmd-format)))))
  )