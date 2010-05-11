
(in-package :lisp-db)

(defvar *default-backend* 'sqlite)
(defvar *db* nil "The current database connection")

(define-condition sql-error (error)
  ((msg :initarg :msg :reader err-msg)
   (code :initarg :code :reader code)))

(defgeneric open-db (backend db-spec)
  (:documentation "Opens a database of the specified type 
using `db-spec'"))

(defgeneric close-db (db)
  (:documentation "Closes a database previously opened by `open-db'")
  (:method :before (db)
	   (finalize db)))

(defgeneric statements (db)
  (:documentation "Returns a list of prepared statements associated
with this database"))

(defgeneric prepare (sql db)
  (:documentation "Prepares the sql statement for execution on
the currently connected database")
  (:method :around (sql db)
	   (let ((stmt (call-next-method sql db)))
	     (push stmt (statements db))
	     stmt)))

(defgeneric changes (db)
  (:documentation "Returns the number of changed rows as a result
of the previous operation on `db'"))

(defgeneric reset (stmt)
  (:documentation "Resets the specified statement so it can be
re-bound with new parameters"))

(defgeneric next-row (stmt)
  (:documentation "Returns the next row in the query associated
with stmt"))

(defgeneric bind (stmt params)
  (:documentation "Binds the specified parameters to stmt"))

(defgeneric finalize (object)
  (:documentation "Cleans up any resources used by object"))

(defgeneric most-recent-insert-id (db)
  (:documentation "Returns the rowid of the last inserted row"))
