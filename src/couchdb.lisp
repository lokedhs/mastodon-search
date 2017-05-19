(in-package :mastodon-search.db)

(declaim (optimize (speed 0) (safety 3) (debug 3)))

(define-condition couchdb-error (error)
  ((code   :initarg :code
           :reader couchdb-error/code)
   (reason :initarg :reason
           :reader couchdb-error/reason))
  (:report (lambda (condition stream)
             (format stream "HTTP code ~a: ~a" (couchdb-error/code condition) (couchdb-error/reason condition)))))

(define-condition document-not-found (couchdb-error)
  ())

(define-condition document-id-conflict (couchdb-error)
  ())

(defclass credentials ()
  ((url      :type string
             :initarg :url
             :reader credentials/url)
   (user     :type string
             :initarg :user
             :reader credentials/user)
   (password :type string
             :initarg :password
             :reader credentials/password)))

(defvar *credentials* nil)
(defvar *database* nil)

(defun authenticated-request (suffix cred &key parameters additional-headers (method :get) content)
  (let ((url (credentials/url cred)))
    (multiple-value-bind (content code return-headers url-reply stream need-close reason-string)
        (drakma:http-request (format nil "~a~a~a"
                                     url
                                     (if (eql (aref url (1- (length url))) #\/)
                                         ""
                                         "/")
                                     suffix)
                             :method method
                             :content content
                             :content-type "application/json"
                             :basic-authorization (list (credentials/user cred) (credentials/password cred))
                             :want-stream t
                             :external-format-out :utf-8
                             :external-format-in :utf-8
                             :parameters parameters
                             :additional-headers additional-headers)
      (declare (ignore content return-headers url-reply))
      (unwind-protect
           (ecase code
             (200 (yason:parse stream))
             (201 (let ((json (yason:parse stream)))
                    (unless (gethash "ok" json)
                      (error "Couchdb result document does not contain an 'ok' value"))
                    (list (gethash "id" json) (gethash "rev" json))))
             (404 (error 'document-not-found :code code :reason reason-string))
             (409 (error 'document-id-conflict :code code :reason reason-string))
             (t   (error 'couchdb-error :code code :reason reason-string)))
        (when need-close
          (close stream))))))

(defun ensure-db (db)
  (let ((d (or db *database*)))
    (unless d
      (error "No database selected. Use the :DB keyword or set *DATABASE* to the name of the database to use."))
    d))

(defun get-document (id &key
                          (db *database*) (cred *credentials*)
                          attachments conflicts deleted-conflicts
                          revs revs-info)
  (authenticated-request (format nil "~a/~a" (ensure-db db) (do-urlencode:urlencode id)) cred
                         :parameters (append (if attachments '(("attachments" . "true")))
                                             (if conflicts '(("conflicts" . "true")))
                                             (if deleted-conflicts '(("deleted_conflicts" . "true")))
                                             (if revs '(("revs" . "true")))
                                             (if revs-info '(("revs_info" . "true"))))))

(defun create-document (doc &key
                              (db *database*) (cred *credentials*)
                              id (override-full-commit nil override-full-commit-p) batch)
  (let ((dbname (ensure-db db))
        (json-string (with-output-to-string (s)
                       (yason:encode doc s))))
    (authenticated-request (if id (format nil "~a/~a" dbname (do-urlencode:urlencode id)) dbname) cred
                           :method (if id :put :post)
                           :additional-headers `(,@(if override-full-commit-p `((:x-couch-full-commit . ,(if override-full-commit "true" "false")))))
                           :parameters (append (if batch '(("batch" . "ok"))))
                           :content json-string)))
