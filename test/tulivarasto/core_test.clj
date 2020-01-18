(ns tulivarasto.core-test
  (:require [tulivarasto.core :as sut]
            [clojure.test :as t :refer [deftest is]]
            [tulivarasto.traverse :as traverse]))


(def ^:dynamic db nil)

(t/use-fixtures :once
  (fn [tests]
    (binding [db (-> "config.edn"
                     slurp
                     read-string
                     sut/connect)]
      (tests))))

(def data {:user/first "Rolf"
           :user/last "Teflon"
           :user/active? true
           :user/friends 42
           :user/birth-date #inst "2000-01-18T09:37:37.045-00:00"})

(deftest basic-read-write-test
  (let [collection-name (keyword (gensym "testusers"))]
    (try

      (sut/write-path! db [collection-name "rolf"] data)

      (let [read-data (sut/read-path db [collection-name "rolf"])]
        ;; Check that stored and read data are the same
        ;; Dates are read back as a different class
        (is (apply = (map #(dissoc % :user/birth-date)
                          [read-data data])))
        (is (= (.toDate (:user/birth-date read-data))
               (:user/birth-date data))))

      (finally
        (.delete (traverse/traverse db [collection-name "rolf"]))))))
