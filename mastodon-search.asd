(asdf:defsystem #:mastodon-search
  :description "Search engine for mastodon"
  :license "Apache"
  :serial t
  :depends-on (:do-urlencode
               :mastodon)
  :components ((module "src"
                       :serial t
                       :components ((:file "package")
                                    (:file "couchdb")
                                    (:file "index-generator")))))
