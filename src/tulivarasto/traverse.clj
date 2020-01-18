(ns tulivarasto.traverse
  "Traverse nested document/collection trees."
  (:import (com.google.cloud.firestore
            Firestore DocumentReference CollectionReference)))

(set! *warn-on-reflection* true)

(defprotocol Traversable
  (document [this name]
    "Traverse to the nested document.")
  (collection [this name]
    "Traverse to the nested collection."))

(defn traverse
  "Traverse from root to nested collection/document.

  Root must be a traversable type: the firestore db service
  or a document/collection reference.

  Path is a collection of values to traverse.
  A keyword value is traversed as a nested collection path.
  All other values are stringified and traversed as a document path.

  Returns a document or collection reference, depending on the
  type of the last value in path.

  For example:
  `(traverse-path db [:users \"foo\" :items 1])`

  would first travel to collection \"users\" from root,
  take the document \"foo\" and then traverse to nested
  collection \"items\" and take the document \"1\".


  "
  [root path]
  (loop [[p & path] path
         r root]
    (if-not p
      r
      (recur path
             (if (keyword? p)
               (collection r (name p))
               (document r (str p)))))))

(extend-protocol Traversable
  Firestore
  (document [^Firestore fs ^String name]
    (.document fs name))
  (collection [^Firestore fs ^String name]
    (.collection fs name))

  DocumentReference
  (document [dr name]
    (throw (ex-info "Document can only contain collections, not other documents"
                    {:document-reference dr
                     :requested-document-name name})))
  (collection [^DocumentReference dr ^String name]
    (.collection dr name))

  CollectionReference
  (document [^CollectionReference cr ^String name]
    (.document cr name))
  (collection [^CollectionReference cr ^String name]
    (throw (ex-info "Collection can only contain documents, not other collections"
                    {:collection-reference cr
                     :requested-collection-name name}))))
