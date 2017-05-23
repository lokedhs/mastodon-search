(in-package :mastodon-search)

(declaim (optimize (speed 0) (safety 3) (debug 3)))

(defun process-message (type msg origin)
  (format t "Got message. type=~a, content=~s~%" type msg)
  (when (equal type "update")
    (let ((obj (make-hash-table :test 'equal)))
      (setf (gethash "msg" obj) msg)
      (setf (gethash "type" obj) "message")
      (setf (gethash "instance" obj) origin)
      (let ((msg-id (gethash "url" msg)))
        (if msg-id
            (handler-case
                (mastodon-search.db:create-document obj :db "foo" :id msg-id)
              (mastodon-search.db:document-id-conflict (condition)
                (warn "Document conflict: ~a" condition)))
            (warn "No message id in message: ~s" msg))))))

(defun index-loop (cred)
  (mastodon:stream-public (lambda (type msg)
                            (process-message type msg (mastodon:credentials/url cred)))
                          :cred cred :raw-json t))
