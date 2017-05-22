(in-package :mastodon-search.db)

(declaim (optimize (speed 0) (safety 3) (debug 3)))

(defun make-map (&rest elements)
  (let ((h (make-hash-table :test 'equal)))
    (loop
      for (key value) on elements by #'cddr
      do (setf (gethash key h) value))
    h))

(define-condition couchdb-error (error)
  ((code   :initarg :code
           :reader couchdb-error/code)
   (reason :initarg :reason
           :reader couchdb-error/reason)
   (result :initarg :result
           :reader couchdb-error/result))
  (:report (lambda (condition stream)
             (format stream "HTTP code ~a: ~a~%~a"
                     (couchdb-error/code condition)
                     (couchdb-error/reason condition)
                     (with-output-to-string (s)
                       (yason:encode (couchdb-error/result condition) s))))))

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
(defvar *debug-http-errors* nil)

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
           (let ((json (yason:parse stream)))
             (case code
               (200 json)
               (201 (progn
                      (unless (gethash "ok" json)
                        (error "Couchdb result document does not contain an 'ok' value"))
                      (list (gethash "id" json) (gethash "rev" json))))
               (404 (error 'document-not-found :code code :reason reason-string :result json))
               (409 (error 'document-id-conflict :code code :reason reason-string :result json))
               (t   (error 'couchdb-error :code code :reason reason-string :result json))))
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

(defun make-sort-arg (fields)
  (loop
    for f in fields
    collect (etypecase f
              (string (make-map f "asc"))
              (list (make-map (first f)
                              (if (second f)
                                  "asc"
                                  "desc"))))))

(defmacro selector (clauses)
  (labels ((sequence-clause (name)
             `(make-map ,name (list ,@(loop
                                        for v in (cdr clauses)
                                        collect `(selector ,v))))))
    (ecase (car clauses)
      (:and (sequence-clause "$and"))
      (:or (sequence-clause "$or"))
      (:in (let* ((key (second clauses))
                  (objs (cddr clauses))
                  (l (length objs)))
             (cond ((zerop l)
                    (error "No arguments to :IN"))
                   ((= l 1)
                    `(make-map ,key ,(car objs)))
                   (t
                    `(make-map ,key (make-map "$in" (list ,@objs)))))))
      (:not `(make-map "$not" (selector ,(second clauses)))))))

(defun find-document (query &key
                              (db *database*) (cred *credentials*)
                              fields sort limit skip)
  "Find documents in the database.

FIELDS is a list of fields to return, the default is to return all
fields.

SORT is a list of columns to sort on. Each element is either the name
of a column, or a list where the first element is a column name, and
the second is a boolean value where true indicated ascending sort,
while false indicates descending.

LIMIT specifies the maximum number of documents to return.

SKIP specifies the number of docuemnts to skip."
  (let ((json (authenticated-request (format nil "~a/_find" (ensure-db db)) cred
                                     :method :post
                                     :content (with-output-to-string (s)
                                                (yason:encode (apply #'make-map
                                                                     "selector" query
                                                                     (append (if fields
                                                                                 `("fields" ,fields))
                                                                             (if sort
                                                                                 `("sort" ,(make-sort-arg sort)))
                                                                             (if limit
                                                                                 `("limit" ,limit))
                                                                             (if skip
                                                                                 `("skip" ,skip))))
                                                              s)))))
    (gethash "docs" json)))
