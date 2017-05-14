(in-package :mastodon-search)

(defun process-message (type msg)
  (format t "Got message. type=~a, content=~s~%" type msg))

(defun index-loop (cred)
  (mastodon:stream-user #'process-message :cred cred))
