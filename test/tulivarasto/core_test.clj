(ns tulivarasto.core-test
  (:require [tulivarasto.core :as sut]
            [clojure.test :as t :refer [deftest is testing]]
            [tulivarasto.traverse :as traverse]))


(def ^:dynamic db nil)

(t/use-fixtures :once
  (fn [tests]
    (binding [db (-> "config.edn"
                     slurp
                     read-string
                     sut/connect)]
      (tests))))


(deftest basic-read-write-test
  (let [collection-name (keyword (gensym "testusers"))
        data {:user/first "Rolf"
              :user/last "Teflon"
              :user/active? true
              :user/friends 42
              :user/birth-date #inst "2000-01-18T09:37:37.045-00:00"}]
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

(deftest query-test
  (let [collection-name (keyword (gensym "measurements"))
        measurements [{:measurement/time #inst "2020-01-18T09:37:37.045-00:00"
                       :measurement/value -5}
                      {:measurement/time #inst "2020-01-05T19:07:37.045-00:00"
                       :measurement/value 4}
                      {:measurement/time #inst "2019-07-07T12:12:12.045-00:00"
                       :measurement/value 22}
                      {:measurement/time #inst "2019-05-08T13:13:13.045-00:00"
                       :measurement/value 7}]
        q (partial sut/query db [collection-name])]
    (try
      (doall
       (map-indexed (fn [i measurement]
                      (sut/write-path! db [collection-name i] measurement))
                    measurements))

      (testing "All measurements are returned"
        (is (= (count measurements)
               (count (q)))))

      (testing "Ordering"
        (testing "by number field"
          (is (= -5 (-> (q {:order-by :measurement/value}) first :measurement/value)))
          (is (= 22 (-> (q {:order-by :measurement/value :order-direction :desc}) first :measurement/value))))


        (testing "by timestamp field"
          (is (= #inst "2019-05-08T13:13:13.045-00:00"
                 (-> (q {:order-by :measurement/time}) first :measurement/time .toDate)))
          (is (= #inst "2020-01-18T09:37:37.045-00:00"
                 (-> (q {:order-by :measurement/time :order-direction :desc}) first :measurement/time .toDate)))))

      (testing "Select specified fields"
        (testing "Only the specified keys are in output maps"
            (is (every? #(= #{:measurement/value}
                            (set (keys %)))
                        (q {:select [:measurement/value]}))))

        (testing "Sorting by an unselected key"
          (is (= [7 22 4 -5]
                 (mapv :measurement/value
                       (q {:order-by :measurement/time
                           :select [:measurement/value]}))))))

      (finally
        (dotimes [i (count measurements)]
          (.delete (traverse/traverse db [collection-name i])))))))
