(ns tulivarasto.core
  "Google Cloud Firestore client library."
  (:import (com.google.cloud.firestore
            Firestore FirestoreOptions
            FieldPath Query Query$Direction
            QueryDocumentSnapshot)
           (com.google.auth.oauth2 GoogleCredentials))
  (:require [tulivarasto.traverse :as traverse]
            [tulivarasto.convert :as convert]))


(defn connect [{:keys [project-id credentials]
                :or {credentials (GoogleCredentials/getApplicationDefault)}}]
  (-> (FirestoreOptions/getDefaultInstance)
      .toBuilder
      (.setCredentials credentials)
      (.setProjectId project-id)
      .build
      .getService))

(defn write-path-async!
  "Async version of `write-path!`. Returns a value that
  can be `deref`ed to get the write result."
  [db path document-data]
  (let [write-future
        (-> db
            (traverse/traverse path)
            (.set (convert/clj->document-data document-data)))]
    (delay (.get write-future))))

(defn write-path!
  "Write document to given path.

  Db and path are used to find the document reference. See `tulivarasto.traverse/traverse`.

  Document-data is a map of Clojure data to write as the document. Should have keyword keys.

  "
  [db path document-data]
  @(write-path-async! db path document-data))

(defn read-path-async
  "Async version of `read-path`. Returns value that can be
  `deref`ed to return the document data."
  [db path]
  (let [read-future
        (-> db
            (traverse/traverse path))]
    (delay
      (-> read-future
          .get .get .getData
          convert/document-data->clj))))

(defn read-path
  "Read document from path.

  Db and path are used to find the document reference. See `tulivarasto.traverse/traverse`.

  Returns the document data converted to a Clojure map."

  [db path]
  (some-> db
          (traverse/traverse path)
          .get .get .getData
          convert/document-data->clj))
