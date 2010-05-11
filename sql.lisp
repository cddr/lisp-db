
(in-package :lisp-db)

;;; This module puts a Lisp front-end on SQL.  It's basically sql
;;; with parenthesis and prefix operators.  The one exported function
;;; `sql' simply takes a sql expression tree and returns a string
;;; that can be sent to a database for evaluation.  It's implementation
;;; is based on the "Metacircular Evaluator" described by Abelson and 
;;; Sussman [1].

;;; It works like this...
;;;  o Operators are defined in terms of functions that return
;;;    true when passed an expression that should be evaulated by
;;;    that operator
;;;
;;;  o For each operator, a corresponding evaluator parses the
;;;    expression and returns a tree of strings which when printed
;;;    in breadth-first order, seperated by spaces, results in an
;;;    expression that can be executed on a SQL database.
;;;
;;;  o The `sql' function simply provides an easy way of getting
;;;    that string.

;;; [1] H. Abelson and G. Sussman, The Structure and Interpretation
;;;     of Computer Programs. Cambridge, Massachusetts: McGraw-Hill
;;;     

;;; A `syntax' is made up of "predicates", and "evaluators"

(defvar *syntax* nil)

(defun predicate (rule)
  (car rule))

(defun evaluator (rule)
  (cdr rule))

(defun find-evaluator (expr syntax)
  "Finds an evaluator to evaluate `expr' from the list of evaluators described
in `syntax'"
  (evaluator (find expr
		   syntax
		   :test (lambda (e r)
			   (funcall (predicate r)
				    e)))))

(defmacro define-operators (operators)
  "A macro to remove some of the verbosity from defining predicates"
  `(progn
    ,@(loop for (test-name . symbol) in operators
	   collect
	   `(defun ,test-name (sql)
	     (tagged-list? sql ,symbol)))))

(defun interpose (obj list)
  "Interposes `obj' into alternate places in list"
  (loop for cons on list
	append (if (cdr cons)
		 ;;interpose in the object if there's a next
		 (list (car cons) obj)
		 ;;otherwise we're at the last element
		 (list (car cons)))))

(defun tagged-list? (sql tag)
  (if (consp sql)
      (eq (intern 
	   (symbol-name (car sql))
	   :lisp-db)
	  tag)))



;; Here's an example of how to use the `define-syntax' macro.

;; First define a set of predicates.  A predicate is a function
;; which the evaluator will call for each expression it tries to
;; evaluate.  If the current expression is a select-clause for
;; example, the sql-select? predicate should return true.

;; Next, define a set of evaluators. An evaluator is a function
;; that the top-level evaluator will call (with the current
;; expression as it's only argument) when the previously
;; described predicate returns true.

;; The `define-syntax' macro simply allows the user to associate
;; predicates with their evaluators and gives the syntax a name.

;; With all this in place, you can define your top-level
;; evaluator function that will evaluate any expression in the
;; language defined by your syntax table.

;; The following code converts a lispy SQL Expression into a tree
;; of strings that can be printed out directly to a SQL Engine
;; (in this case the SQLite engine).

(defun sql-operations (type)
  (case type
    (infix '(and or like + = <> / ||))
    (core '(abs coalesce glob ifnull))
    (aggregate '(count hex last-insert-rowid length like 
		 lower ltrim max min nullif quote
		 random randomblob replace round rtrim 
		 soundex sqlite-version substr trim 
		 typeof upper zeroblob))
    (table '(table temporary-table))
    (constraint '(primary-key check unique-key))))

;; Parsing Predicates
 
(define-operators
  ((sql-select? . 'select)
   (sql-from? . 'from)
   (sql-where? . 'where)
   (sql-group? . 'group-by)
   (sql-order? . 'order)
   (sql-having? . 'having)
   (sql-case? . 'case)
   (sql-update? . 'update)
   (sql-set? . 'set)
   (sql-insert? . 'insert)
   (sql-subquery? . 'sub-query)
   (sql-join? . 'join)
   (sql-on? . 'on)
   (sql-using? . 'using)
   (sql-create? . 'create)
   (sql-column? . 'column)
   (sql-as? . 'as)))

(defun sql-symbol? (sql) (symbolp sql))
(defun sql-string? (sql) (stringp sql))
(defun sql-primitive? (sql) (numberp sql))
(defun sql-op? (sql) 
  (member (car sql)
	  (append (sql-operations 'core)
		  (sql-operations 'aggregate))))
(defun sql-infix? (sql) 
  (member (car sql)
	  (sql-operations 'infix)))
(defun sql-table? (sql)
  (member (car sql)
	  '(table temporary-table)))
(defun sql-constraint? (sql)
  (member (car sql)
	  (sql-operations 'constraint)))
(defun sql-fn? (sql)
  (member (car sql)
	  (append (sql-operations 'core)
		  (sql-operations 'aggregate))))

;; SQL Evaluators

(defun sql-select-evaluator (expr)
  (destructuring-bind (result &rest body)
      (cdr expr)
    (append (list "select"
		  (interpose "," (mapcar (lambda (col)
					   (if (consp col)
					       (eval-sql col)
					       (sql-symbol-evaluator col t)))
					 result)))
	    (if body
		(mapcar #'eval-sql body)))))

(defun sql-create-evaluator (expr)
  (destructuring-bind (tbl-head &rest tbl-body)
			(cdr expr)
    (append (list "create"
		  (eval-sql tbl-head)
		  (if (listp (car tbl-body))
		      ;; This means we've got a normal create table
		      ;; statement with a list of column specs (as
		      ;; opposed to "create table as .."
		      ;;
		      ;; we would like columns to have the syntax
		      ;; (col-name [col-type] [col-constraint]) but
		      ;; we have to give the parser some context so
		      ;; at this point we take each column definition
		      ;; written in the syntax above, and prefix it
		      ;; with 'column before sending it through the
		      ;; evaluator again
		      (list "("
			    (interpose ","
				    (mapcar (lambda (item)
					      (if (sql-constraint? item)
						  (eval-sql item)
						  (eval-sql (list 'column
								  item))))
					    tbl-body))
			    ")")

		      ;; Here, we've got a "create table as.."
		      ;; statement, so we just send it back through
		      ;; the evaluator
		      (eval-sql tbl-body))))))

(defun sql-table-evaluator (expr)
  (destructuring-bind (table-type table-name)
      expr
    (append
     (case table-type
       (table (list "table"))
       (temporary-table (list "temporary table")))
     (eval-sql table-name))))

(defun sql-as-evaluator (expr)
  (destructuring-bind (as &body body)
      expr
    (append (eval-sql as)
	    (eval-sql body))))

(defun sql-column-evaluator (expr)
  (destructuring-bind ((name &optional type constraint))
      (cdr expr)
    (append (eval-sql name)
	    (when type
	      (eval-sql type))
	    (when constraint 
	      (eval-sql constraint)))))

(defun sql-constraint-evaluator (expr)
  (destructuring-bind (constraint body)
      expr
    (flet ((eval-keys (keys)
	     (list "("
		   (interpose ","
			   (mapcar (lambda (key)
				     (eval-sql key))
				   keys))
		   ")")))
      (append (case (intern (symbol-name constraint)
			    :lisp-db)
		(primary-key (list "primary key"
				   (eval-keys body)))
		(unique-key (list "unique key"
				  (eval-keys body)))
		(foreign-key (list "foreign key"
				   (eval-keys body))))))))

(defun sql-from-evaluator (expr)
  (destructuring-bind (keyword body)
      expr
    (append (list (string-downcase (symbol-name keyword)))
	    (eval-sql body))))

(defun sql-group-evaluator (expr)
  (destructuring-bind (keyword body)
      expr
    (list (eval-sql keyword)
	  (eval-sql body))))

(defun sql-order-evaluator (expr)
  (destructuring-bind (columns &optional direction)
      (cdr expr)
    (append (list "order by")
	    (interpose "," (mapcar #'eval-sql columns))
	    (when direction
	      (eval-sql direction)))))

(defun sql-case-evaluator (expr)
  (destructuring-bind (expr &rest cases)
      (cdr expr)
    (append (list "case")
	    (eval-sql expr)
	    (loop for case in cases
	       collect (if (eq (car case) 't)
			   (list "else"
				 (eval-sql (cdr case)))
			   (list "when"
				 (eval-sql (car case))
				 "then"
				 (eval-sql (cdr case)))))
	    (list "end"))))

(defun sql-update-evaluator (expr)
  (destructuring-bind (table-name &rest rest)
      (cdr expr)
    (append (list "update")
	    (eval-sql table-name)
	    (mapcar 'eval-sql rest))))

(defun sql-set-evaluator (expr)
  (destructuring-bind (&rest assignments)
      (cdr expr)
    (append (list "set")
	    (interpose "," (mapcar (lambda (name val)
				  (format nil "~a=~a" name
					  (eval-sql val)))
				(loop for name in assignments by #'cddr
				     collect name)
				(loop for val in (cdr assignments) by #'cddr
				     collect val))))))

(defun sql-insert-evaluator (expr)
  (destructuring-bind ((table columns) values &key on-conflict)
      (cdr expr)
    (append (list "insert"
		  (when on-conflict
		    (list "or"
			  (eval-sql on-conflict)))
		  "into"
		  (eval-sql table))
	    (list "(" 
		  (interpose "," (mapcar #'eval-sql columns)) ")")
	    (list "values"
		  "(" (interpose "," (mapcar #'eval-sql values)) ")"))))

(defun sql-subquery-evaluator (expr)
  (destructuring-bind (op source)
      expr
    (declare (ignore op))
    (if (listp source)
	(append (list "("
		      (eval-sql source)
		      ")"))
	(eval-sql source))))

(defun sql-join-evaluator (expr))
  ;; (destructuring-bind (sources &key (side left)
  ;; 			       (method inner))
  ;;     (cdr expr)
  ;;   (let ((join (remove nil (list side method 'join))))
  ;;     (flet ((eval-source (source)
  ;; 	       (if (listp source)
  ;; 		   (list 'sub-query source)
  ;; 		   source))
  ;; 	     (eval-clause (a b on)
  ;; 	       (interpose join (eval-source a) 
	
  ;;     (append (mapcar eval-sql (if (listp left)
  ;; 			  (list 'sub-query left)
  ;; 			  left))
  ;; 	    (eval-sql side)
  ;; 	    (eval-sql method)
  ;; 	    (eval-sql op)
  ;; 	    (eval-sql (if (listp right)
  ;; 			  (list 'sub-query right)
  ;; 			  right))
  ;; 	    (eval-sql on))))

(defun sql-on-evaluator (expr)
  (destructuring-bind (op clause)
      expr
    (append (eval-sql op)
	    (eval-sql clause))))
	    


(defun sql-where-evaluator (expr)
  (destructuring-bind (keyword body)
      expr
    (list (eval-sql keyword)
	  (eval-sql body))))

(defun sql-join-source-evaluator (expr)
  (destructuring-bind (op source)
      expr
    (declare (ignore op))
    (if (listp (car source))
	(list "("
	      (eval-sql (car source))
	      ")"
	      "as" (eval-sql (cadr source)))
	(eval-sql source))))

(defun sql-having-evaluator (expr)
  (declare (ignore expr)))
  
;; Functions to evaluate primitive SQL expressions (e.g. strings, numbers
;; and symbols )

(defun sql-symbol-evaluator (expr &optional (quotep nil))
  "Downcases and swaps \"_\"s for \"-\"s and optionally wraps the
result in quotes"
  (list (substitute
	 #\_ #\-
	 (string-downcase 
	  (cond ((keywordp expr)
		 (format nil ":~a" expr))
		(t (if quotep
		       (format nil "\"~a\"" expr)
		       (format nil "~a" expr))))))))


(defun sql-string-evaluator (expr)
  (list (with-output-to-string (s)
	  (princ #\' s)
	  (loop for char across expr
	     do (progn
		  (when (char= #\' char)
		    (princ #\' s))
		  (princ char s)))
	  (princ #\' s))))


(defun sql-binop-evaluator (expr)
  (append (list "(")
	  (eval-sql (second expr))
	  (eval-sql (first expr))
	  (eval-sql (third expr))
	  (list ")")))



(defun sql-prefix-to-infix (expr)
  (list "(" (interpose (first expr)
		    (mapcar 'eval-sql (cdr expr)))
	")"))

(defun sql-default-evaluator (expr)
  (list (write-to-string expr)))

(defun sql-op-evaluator (expr)
  (destructuring-bind (name &rest args)
      expr
    (append (eval-sql name)
	    (list "(" (interpose "," (mapcar #'eval-sql args)) ")"))))

(defparameter *sql-syntax*
  '((sql-symbol? . sql-symbol-evaluator)
    (sql-string? . sql-string-evaluator)
    (sql-primitive? . sql-default-evaluator)
    (sql-infix? . sql-prefix-to-infix)
    (sql-op? . sql-op-evaluator)
    (sql-select? . sql-select-evaluator)
    (sql-from? . sql-from-evaluator)
    (sql-where? . sql-where-evaluator)
    (sql-group? . sql-group-evaluator)
    (sql-order? . sql-order-evaluator)
    (sql-having? . sql-having-evaluator)
    (sql-case? . sql-case-evaluator)
    (sql-update? . sql-update-evaluator)
    (sql-set? . sql-set-evaluator)
    (sql-insert? . sql-insert-evaluator)
    (sql-subquery? . sql-subquery-evaluator)
    (sql-join? . sql-join-evaluator)
    (sql-on? . sql-on-evaluator)
    (sql-create? . sql-create-evaluator)
    (sql-table? . sql-table-evaluator)
    (sql-column? . sql-column-evaluator)
    (sql-constraint? . sql-constraint-evaluator)
    (sql-as? . sql-as-evaluator)))


(defun flatten (node)
  (labels ((rec (node accumulation)
	     (cond ((null node) accumulation)
		   ((atom node) (cons node
				      accumulation))
		   (t (rec (car node)
			   (rec (cdr node) accumulation))))))
    (rec node nil)))

(defun print-sql (expr)
  (with-output-to-string (s)
    (loop for (fragment . tail) on (flatten (eval-sql expr))
	 do (progn
	      (princ fragment s)
	      (when tail
		(princ " " s))))))

(defun intern-expression (expr)
  (cond ((and (listp expr)
	      (symbolp (car expr)))
	 (cons (intern (symbol-name (car expr))
		       :lisp-db)
	       (cdr expr)))
	(t expr)))

(defun cache-hit (cache expr)
  (gethash expr cache))

(defun cache-miss (cache expr val)
  (setf (gethash expr cache)
	val))

(let ((cache (make-hash-table :test 'equal)))

  (defun eval-sql (expr)
    "Recursively evaluates an expression using the SQL syntax defined by
the predicates and evaluators above.

Because this is a rather expensive operation, the result is cached
and if the same expression is requested again, the cached result is
returned without computing the value again."
    (or (cache-hit cache expr)
	(cache-miss cache expr
          (let ((expr (intern-expression expr))
		(*syntax* *sql-syntax*))
	    (let ((evaluator (find-evaluator expr *syntax*)))
	      (funcall evaluator expr)))))))

(let ((cache (make-hash-table :test 'equal)))
  (defun sql (expr)
    "Returns a string representation of `expr' for execution on a DBMS"
    (or (cache-hit cache expr)
	(cache-miss cache expr (print-sql expr)))))






