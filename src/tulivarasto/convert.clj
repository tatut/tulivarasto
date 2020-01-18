(ns tulivarasto.convert
  "Convert between Clojure maps and Firestore documents.")

(set! *warn-on-reflection* true)

(defprotocol Conversion
  (to-firestore [this clj-value]
    "Convert Clojure data to Firestore field value.")
  (to-clojure [this firestore-value]
    "Convert Firestore field value to Clojure data."))

(defonce keyword-conversions (atom {}))

(defn register-keyword-conversion! [kw conversion]
  {:pre [(keyword? kw)
         (satisfies? Conversion conversion)]}
  (swap! keyword-conversions
         assoc kw conversion))

(def identity-conversion
  (reify Conversion
    (to-firestore [_ value] value)
    (to-clojure [_ value] value)))

(defn document-data->clj
  "Convert a documents data map into a Clojure map."
  [^java.util.Map document-data]
  (into {}
        (map (fn [[^String k v]]
               (let [kw (if (= \: (.charAt k 0))
                          (let [ns-sep (.indexOf k "/")]
                            (if (= -1 ns-sep)
                              (keyword (subs k 1))
                              (keyword (subs k 1 ns-sep)
                                       (subs k (inc ns-sep)))))
                          k)
                     conv (get @keyword-conversions kw identity-conversion)]

                 [kw (to-clojure conv v)]))
             document-data)))

(defn clj->document-data
  "Convert a clojure map into a document data map."
  [data]
  (into {}
        (map (fn [[kw v]]
               (let [conv (get @keyword-conversions kw
                               identity-conversion)]
                 [(str kw)
                  (to-firestore conv v)])))
        data))
