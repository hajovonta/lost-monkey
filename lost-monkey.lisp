;; Project Codename: Lost Monkey
(defparameter version "2016-09-21")

(ql:quickload "hunchentoot")
(ql:quickload "drakma")
(ql:quickload "closure-html")
(ql:quickload "alexandria")

;; scavenged from http://john.freml.in/sbcl-optimise-plan
(defmacro with-returning-performance (&body body)
  (alexandria:with-unique-names (h0 l0 h1 l1)
    (let ((start-time (gensym))
	  (end-time (gensym))
	  (cycles (gensym))
	  (result (gensym)))
      `(multiple-value-bind (,h0 ,l0) 
	   (sb-impl::read-cycle-counter)
	 (let ((,start-time (get-internal-real-time))
	       (,result (progn ,@body))
	       (,end-time (get-internal-real-time)))
	   (multiple-value-bind (,h1 ,l1)
	       (sb-impl::read-cycle-counter)
	     (let ((,cycles (sb-impl::elapsed-cycles ,h0 ,l0 ,h1 ,l1)))
	       (values (list ,result ,cycles (- ,end-time ,start-time))))))))))

 ;; (floor (/ ,cycles (- ,end-time ,start-time)))

(defmacro with-bytes-consed (&body body)
  `(let ((before (get-bytes-consed)))
     (locally ,@body)
     (let ((after (get-bytes-consed)))
       (- after before))))

;a hidden form that accepts remote requests and evaluates them locally.
(defparameter my-handler (hunchentoot:define-easy-handler (hl :uri "/hl") (cmd)
			   (let ((*standard-output* *standard-output*))
;;			     (format *standard-output* "blah~%")
			     (let ((retval (if cmd
					       (handler-case
						   (progn
						     (format *standard-output* "Remote request: ~a~%" cmd)
						     (with-returning-performance (eval (read-from-string cmd))))
						 (error (e) (list 'error (type-of e))))
					       nil)))
			       (format nil "<html><body><div id=result>~a</div><form action=/hl method=POST><input type=hidden name=cmd></input><input type=submit></input></form></body></html>" retval)))))

;hunchentoot instance at given port
(defparameter my-instance (make-instance 'hunchentoot:easy-acceptor :port 4242 :access-log-destination nil))

(hunchentoot:start my-instance)

;a command that is evaluated at remote site.
(defun remote-command (url cmd)
  (let ((start-time (get-internal-real-time)))
    (let ((retval (read-from-string
		   (caddr (third (fourth 
				  (chtml:parse
				   (drakma:http-request url :method :post :parameters (list (cons "cmd" cmd)))
				   (chtml:make-lhtml-builder))))))))
      (let ((end-time (get-internal-real-time)))
	(values retval (- end-time start-time))))))

;class to hold information about nodes
(defclass computing-node ()
  ((name :initarg :name :accessor name)
   (url :initarg :url :accessor url)
   (status :initarg :status :accessor status)
   (tasks-performed :initarg :tasks-performed :accessor tasks-performed)
   (cycles-performed :initarg :cycles-performed :accessor cycles-performed)
   (performance :initarg :performance :accessor performance)))

(define-condition offline-node () ((node :initarg :node :reader node)))

(defun node-status (nodes-list)
  (loop for n in nodes-list collect (list (name n) (status n))))
(defun node-report (nodes-list)
  (let ((rank 0))
    (loop for n in nodes-list do
	 (incf rank)
	 (format t "Rank: ~a Name: ~a URL: ~a Status: ~a~%Tasks: ~a Cycles: ~:d Performance: ~a cycles/second~%" rank (name n) (url n) (status n) (tasks-performed n) (cycles-performed n) (performance n)))))

;hosts
(handler-case
    (load "~/nodes.lisp")
  (error () nil))

(defparameter nodes nil)
(push loc nodes)
(push rm1 nodes)
(push rm2 nodes)

(defparameter history nil)

;thread-pool
(defparameter thread-pool nil)

(defun print-node-status (node)
  (format *standard-output* "Node ~a status: ~a~%" (name node) (status node))
  (finish-output *standard-output*))

;probe a node
(defun probe-node (node)
  (handler-case
      (progn
	(remote-command (url node) (prin1-to-string t))
	(if (eq :offline (status node)) (progn
					  (setf (status node) :available)
					  (print-node-status node))))
    (error () (progn
		(setf (status node) :offline)
		(print-node-status node)))))

(defun probe-offline-nodes (nodes-list)
  "Probe offline nodes and if responding, switch them to :AVAILABLE status. Between probe cycles, sleep (length nodes) seconds."
  (loop while t do
       (progn
	 (sort nodes-list #'(lambda (x y) (> (performance x) (performance y))))
	 (let ((offline-nodes (remove-if-not #'(lambda (x) (eq :offline (status x))) nodes-list)))
	   (loop for n in offline-nodes do (handler-case
					       (probe-node n)
					     (offline-node () nil)))
	   (if nodes-list (sleep (length nodes-list)))))))

(defparameter probe-offline-thread (sb-thread:make-thread #'(lambda () (probe-offline-nodes nodes)) :name "probe-offline-thread"))

;get an available node (hangs until there is one)
(defvar node-mutex (sb-thread:make-mutex :name "node-mutex"))

(defun get-node (nodes-list)
  (let ((retval nil))
    (loop while (null retval) do
	 (sb-thread:with-mutex (node-mutex)
	   (loop until (remove-if-not #'(lambda (x) (eq :available (slot-value x 'status))) nodes-list) do (sb-thread:thread-yield))
	   (setf retval (first (remove-if-not #'(lambda (x) (eq :available (slot-value x 'status))) nodes-list)))))
    retval))

(defun calculate-performance (cycles milliseconds)
  "Calculate performance based on processor cycles. This helps ranking the nodes (faster nodes should have priority)."
  (if (> milliseconds 0)
      (floor (/ cycles milliseconds))
      0))

;a version of mapcar that uses nodes to calculate items parallel
(defmacro m-mapcar-helper (fn lst)
  `(loop for item in ,lst collect
	`(funcall #',(read-from-string ,fn)
		  ,item)))

(defmacro m-mapcar (fn lst)
  (let ((mylambda (prin1-to-string fn)))
    `(mapcar #'(lambda (x) (let ((retval (sb-thread:join-thread x)))
			     (setf thread-pool (remove x thread-pool))
			     retval))
	     (loop for command-to-run in (m-mapcar-helper (multiple-value-bind (flambda fclosure fname)
							      (function-lambda-expression ,fn)
							    (declare (ignore flambda))
							    (if fclosure (subseq ,mylambda 2) (prin1-to-string fname))) ,lst) collect
		  (let ((command-to-run command-to-run)) ;to prevent reusing closure - thanks to stassats and flip214
		    (let ((node (get-node nodes))) ;get an available node
		      (let ((th (sb-thread:make-thread
				 (lambda (standard-output)
				   (let ((*standard-output* standard-output))
				     (let ((result (run-command node command-to-run))) ;run the command on the node
				       (incf (tasks-performed node))
				       (incf (cycles-performed node) (second result))
				       (let ((perf (calculate-performance (second result) (third result)))) ;only overwrite performance when it's >0
					 (when (> perf 0) (setf (performance node) perf))) ;else it has the last value
				       (first result))))
				 :arguments (list *standard-output*))))
			(push th thread-pool)
			th)))))))

(defmethod run-command ((node computing-node) cmd)
;;  (format *standard-output* "Processing at ~a: ~a~%" (name node) (prin1-to-string cmd))
  (handler-case
      (progn
	(setf (status node) :working)
	(push (list (name node) cmd) history)
	(let ((retval (remote-command (slot-value node 'url) (prin1-to-string cmd))))
	  (when (eq (status node) :working) ; if the node was disabled during calculation, then don't give it back to the pool
	    (setf (status node) :available))
	  retval))
    (error () (progn
		(setf (status node) :offline)
		(print-node-status node)
		(run-command (get-node nodes) cmd)))))

(defmethod switch-node ((node computing-node)) ()
	   (cond ((eq :available (status node)) (setf (status node) :offline))
		 ((eq :working (status node)) (setf (status node) :offline))
		 ((eq :offline (status node)) (setf (status node) :available)))
	   (print-node-status node))

(defmethod disable-node ((node computing-node)) ()
	   (setf (status node) :disabled)
	   (print-node-status node))

(defmethod enable-node ((node computing-node)) ()
	   (setf (status node) :available)
	   (print-node-status node))

;use this to distribute common code to nodes
(defun m-defun (nodes-list fn)
  (mapcar #'(lambda (x) (run-command x fn)) (remove-if-not #'(lambda (x) (eq :available (slot-value x 'status))) nodes-list)))

;example common code and for benchmarking
(m-defun nodes '(defun naive-fibonacci (n)
		 (check-type n (integer 0 *))
		 (if (< n 2)
		     n
		     (+ (naive-fibonacci (1- n))
			(naive-fibonacci (- n 2))))))
(m-defun nodes '(defun tail-recursive-fibonacci (n)
		 (check-type n (integer 0 *))
		 (labels ((fib-aux (n f1 f2)
			    (if (zerop n) f1
				(fib-aux (1- n) f2 (+ f1 f2)))))
		   (fib-aux n 0 1))))
(m-defun nodes '(defun successive-squaring-fibonacci (n)
		 "Successive squaring method from SICP"
		 (check-type n (integer 0 *))
		 (labels ((fib-aux (a b p q count)
			    (cond ((= count 0) b)
				  ((evenp count)
				   (fib-aux a
					    b
					    (+ (* p p) (* q q))
					    (+ (* q q) (* 2 p q))
					    (/ count 2)))
				  (t (fib-aux (+ (* b q) (* a q) (* a p))
					      (+ (* b p) (* a q))
					      p
					      q
					      (- count 1))))))
		   (fib-aux 1 0 0 1 n))))

;; try this:
;; (m-mapcar #'naive-fibonacci (loop for i from 10 to 20 collect i))

;cleaning up of threads
(defun thread-cleanup ()
  (mapcar #'(lambda (x) (sb-thread:terminate-thread x)) (remove-if-not #'(lambda (th) (sb-thread:thread-alive-p th)) thread-pool))
  (setf thread-pool nil))

;; benchmarking functions

(defmacro m-timing (&body forms)
  (let ((start-time (gensym))
	(end-time (gensym))
	(result (gensym)))
    `(let ((,start-time (get-internal-real-time))
	   (,result (progn ,@forms))
	   (,end-time (get-internal-real-time)))
       (format *debug-io* ";;; Computation took: ~fms (real time)~%" (/ (- ,end-time ,start-time) 1000))
       ,result)))

(defmacro report-usage (&body forms)
  `(progn
     (setf history nil)
     (let ((retval (m-timing ,@forms)))
       (loop for n in nodes do
	    (let ((counter 0))
	      (loop for h in history when (equal (car h) (name n)) do (incf counter))
	      (format t "Node ~a performed ~a tasks. Performance: ~a cycles/second~%" (name n) counter (performance n))))
       retval)))

;; try this:
;; (loop for i from 25 to 35 do
;;      (format t "(m-mapcar #'fib (loop for j from 1 to 100 collect ~a))~%" i)
;;      (report-usage (m-mapcar #'fib (loop for j from 1 to 100 collect i))))

(node-status nodes)
