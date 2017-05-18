(defpackage :mastodon-search
  (:use :cl)
  (:documentation "Search engine for mastodon")
  (:export #:index-loop))

(defpackage :mastodon-search.db
  (:use cl)
  (:export #:create-document
           #:get-document
           #:couchdb-error
           #:document-not-found
           #:credentials
           #:*credentials*
           #:*database*
           #:document-id-conflict))
