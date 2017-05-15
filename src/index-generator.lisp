(in-package :mastodon-search)

(declaim (optimize (speed 0) (safety 3) (debug 3)))

(defun process-message (type msg)
  (format t "Got message. type=~a, content=~s~%" type msg)
  (let ((obj (make-hash-table :test 'equal)))
    (setf (gethash "msg" obj) msg)
    (setf (gethash "type" obj) "message")
    (let ((msg-id (gethash "url" msg)))
      (if msg-id
          (mastodon-search.db:create-document obj :db "foo" :id msg-id)
          (warn "No message id in message: ~s" msg)))))

(defun index-loop (cred)
  (mastodon:stream-user (lambda (type msg) (process-message type msg)) :cred cred :raw-json t))
