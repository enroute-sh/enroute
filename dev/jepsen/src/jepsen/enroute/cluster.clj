(ns jepsen.enroute.cluster
  "Runs Enroute nodes as local processes on 127.0.0.1. Each node runs as its
  own Linux user, so that iptables can tell their packets apart on loopback."
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [db :as db]
                    [store :as store]
                    [util :as util]]
            [jepsen.enroute.http :as http]))

(defn sh!
  "Runs a command and returns its stdout. Throws if it fails."
  [& args]
  (let [{:keys [exit out err]} (apply shell/sh args)]
    (when-not (zero? exit)
      (throw (ex-info (str "command failed: " (str/join " " args) ": " err)
                      {:exit exit :out out :err err})))
    out))

(defn- await!
  "Waits until `(pred)` is true. Throws after `timeout-ms`."
  [what timeout-ms pred]
  (util/await-fn #(or (pred) (throw (ex-info (str "waiting for " what) {})))
                 {:timeout timeout-ms, :retry-interval 100,
                  :log-interval 5000, :log-message (str "Waiting for " what)}))

;; Topology

(defn topology
  "Node names, roles and ports. `:split` is 3 ledger and 2 compute nodes;
  `:both` is 3 nodes with both roles; `:wide` is 5 ledger and 2 compute
  nodes, so that a log's 3 replicas have nodes to move to."
  [kind]
  (let [roles (case kind
                :split (array-map "n1" "ledger" "n2" "ledger" "n3" "ledger"
                                  "n4" "compute" "n5" "compute")
                :both  (array-map "n1" "both" "n2" "both" "n3" "both")
                :wide  (array-map "n1" "ledger" "n2" "ledger" "n3" "ledger"
                                  "n4" "ledger" "n5" "ledger"
                                  "n6" "compute" "n7" "compute"))]
    {:kind     kind
     :nodes    (vec (keys roles))
     :roles    roles
     :ledgers  (vec (for [[n r] roles :when (not= r "compute")] n))
     :computes (vec (for [[n r] roles :when (not= r "ledger")] n))}))

(defn- index [node] (Long/parseLong (subs node 1)))
(defn git-port [node] (+ 18000 (* 10 (index node))))
(defn api-port [node] (+ 18001 (* 10 (index node))))
(defn internal-port [node] (+ 18002 (* 10 (index node))))
(defn git-url [node] (str "http://127.0.0.1:" (git-port node)))

(defn uid [node] (str/trim (sh! "id" "-u" node)))

;; Files

;; One directory per test: a dead LazyFS mount then only breaks its own
;; test. The start time tells apart tests with the same name.
(defn- base [test]
  (str (:dir test) "/" (str/replace (str (:start-time test)) #"[^A-Za-z0-9]+" "-")))
(defn- data-dir [test node] (str (base test) "/" node))
(defn- log-file [test node] (str (base test) "/" node ".log"))
(defn- config-file [test node] (str (base test) "/" node ".toml"))
(defn- bucket [test] (str (base test) "/bucket"))
(defn- bin [test] (str (base test) "/enroute"))

(defn config
  "The deployment file of `node`. No [hooks] table: pushes and reads are open,
  and a push creates its repository."
  [test node]
  (let [role (get-in test [:topology :roles node])]
    (str "[bucket]\nuri = \"file://" (bucket test) "\"\n"
         "[node]\n"
         "role = \"" role "\"\n"
         "data_dir = \"" (data-dir test node) "\"\n"
         "internal_listen = \"127.0.0.1:" (internal-port node) "\"\n"
         (when (not= role "compute")
           (str "advertise_url = \"http://127.0.0.1:" (internal-port node) "\"\n"))
         "cpu_threads = 2\n"
         "[layers]\nflush_interval = \"300ms\"\n"
         (when-let [kb (:inline-layer-kb test)]
           (str "inline_layer = \"" kb " KiB\"\n"
                (when (not= role "compute")
                  (str "[ledger]\nmax_inline_layer = \"" kb " KiB\"\n"))))
         "[listen]\n"
         "git = \"127.0.0.1:" (git-port node) "\"\n"
         "api = \"127.0.0.1:" (api-port node) "\"\n")))

;; Processes

(defonce pids (atom {}))

(defn running? [node] (contains? @pids node))

(defn start!
  "Starts `node` as its own user. A clean environment keeps root's proxy settings out. The umask lets every node write to the
  directories that another node made in the shared bucket."
  [test node]
  (when-not (running? node)
    (let [cmd (str "(umask 000; exec setpriv --reuid=" node " --regid=" node
                   " --clear-groups env -i PATH=/usr/bin:/bin RUST_BACKTRACE=1 RUST_LOG=" (:rust-log test "warn")
                   " " (bin test) " --config file://" (config-file test node)
                   " < /dev/null >> " (log-file test node) " 2>&1) & echo $!")
          pid (str/trim (sh! "sh" "-c" cmd))]
      (swap! pids assoc node pid)
      (info "started" node "pid" pid))))

(defn signal! [node sig]
  (when-let [pid (get @pids node)]
    (shell/sh "kill" (str "-" sig) pid)))

(defn kill!
  "SIGKILL, and wait until the process is gone so that a restart can bind."
  [node]
  (when-let [pid (get @pids node)]
    (shell/sh "kill" "-KILL" pid)
    (await! (str node " to exit") 5000
            #(not (zero? (:exit (shell/sh "kill" "-0" pid)))))
    (swap! pids dissoc node)
    (info "killed" node)))

(defn ready?
  [node]
  (try (= 200 (:status (http/get (str "http://127.0.0.1:" (internal-port node) "/status")
                                 {:timeout 2000})))
       (catch Exception _ false)))

(defn await-ready [node timeout-ms]
  (await! (str node " to be ready") timeout-ms #(ready? node)))

(defn- consensus
  "The `consensus` object of `node`'s `GET /status`, or nil."
  [node]
  (try (:consensus (http/get-json (str "http://127.0.0.1:" (internal-port node) "/status")
                                  {:timeout 2000}))
       (catch Exception _ nil)))

(defn node-id
  "The ledger node ID that `node` runs as now, or nil."
  [node]
  (some-> (consensus node) (get "node") long))

(defn hosted-logs
  "How many repository logs `node` holds, or nil."
  [node]
  (some-> (consensus node) (get "repos") long))

(defn await-new-id
  "Waits until `node` answers with an ID other than `old`, and returns it."
  [node old timeout-ms]
  (await! (str node " to join with a new ID") timeout-ms
          #(when-let [id (node-id node)] (when (not= id old) id))))

(defn wipe!
  "Deletes everything in `node`'s data directory, as if it lost its disk.
  Call it while the node is down. It keeps the directory, which can be a
  LazyFS mount."
  [test node]
  (sh! "find" (data-dir test node) "-mindepth" "1" "-delete")
  (info "wiped the disk of" node))

(defn remove-member!
  "Runs `enroute members --remove id` on ledger node `via`, as `via`'s user:
  the command signs with the key in its data directory. True if the ledger
  removed `id`. Never forces: a removal that would leave no live majority is
  refused, and the caller retries it later."
  [test via id]
  (let [{:keys [exit err]}
        (shell/sh "setpriv" (str "--reuid=" via) (str "--regid=" via) "--clear-groups"
                  "env" "-i" "PATH=/usr/bin:/bin" "RUST_LOG=warn"
                  (bin test) "members" "--config" (str "file://" (config-file test via))
                  "--remove" (str id))]
    (if (zero? exit)
      (do (info via "removed member" id) true)
      (do (warn via "could not remove member" id ":" (str/trim err)) false))))

(defn await-serving
  "Waits until compute `node` reaches the ledger: /status answers before the
  node has joined the ledger."
  [node timeout-ms]
  (let [url (str (git-url node) "/jepsen-ready.git/info/refs?service=git-upload-pack")]
    (await! (str node " to reach the ledger") timeout-ms
            #(#{200 404} (:status (http/get url {:timeout 2000}))))))

;; Network faults. A SYN from a node's user marks the connection with the
;; node's index, so a rule can drop both directions of every connection
;; between two nodes, including ones open before the fault.

(def chain "ENROUTE_JEPSEN")

(defn net-setup! [test]
  (doseq [t ["mangle" "filter"]]
    (shell/sh "iptables" "-t" t "-N" chain)
    (sh! "iptables" "-t" t "-F" chain)
    (when-not (zero? (:exit (shell/sh "iptables" "-t" t "-C" "OUTPUT" "-o" "lo" "-j" chain)))
      (sh! "iptables" "-t" t "-I" "OUTPUT" "-o" "lo" "-j" chain)))
  (doseq [n (get-in test [:topology :nodes])]
    (sh! "iptables" "-t" "mangle" "-A" chain "-p" "tcp" "--syn"
         "-m" "owner" "--uid-owner" (uid n)
         "-j" "CONNMARK" "--set-mark" (str (index n)))))

(defn heal! [] (sh! "iptables" "-F" chain))

(defn drop!
  "Drops all traffic between `src` and the internal port of `dst`."
  [src dst]
  (doseq [dir ["--dport" "--sport"]]
    (sh! "iptables" "-A" chain "-p" "tcp" dir (str (internal-port dst))
         "-m" "connmark" "--mark" (str (index src)) "-j" "DROP")))

(defn partition!
  "Applies a Jepsen grudge: each node drops traffic from the nodes in its set."
  [grudge]
  (heal!)
  (doseq [[dst srcs] grudge, src srcs]
    (drop! src dst)))

(defn net-teardown! []
  (doseq [t ["mangle" "filter"]]
    (shell/sh "iptables" "-t" t "-D" "OUTPUT" "-o" "lo" "-j" chain)
    (shell/sh "iptables" "-t" t "-F" chain)
    (shell/sh "iptables" "-t" t "-X" chain)))

;; LazyFS. A ledger node's data directory can be a LazyFS mount, so that a
;; kill can also lose the writes that the node did not fsync.

(defn lazyfs? [test node]
  (and (:lazyfs test) (not= "compute" (get-in test [:topology :roles node]))))

(defn- lazy-root [test node] (str (base test) "/" node ".lazyfs-root"))
(defn- lazy-fifo [test node] (str (base test) "/" node ".lazyfs.fifo"))
(defn- lazy-config [test node] (str (base test) "/" node ".lazyfs.toml"))
(defn- lazy-log [test node] (str (base test) "/" node ".lazyfs.log"))

(defn- mounted? [dir]
  (zero? (:exit (shell/sh "mountpoint" "-q" dir))))

;; `mountpoint` fails on a dead FUSE mount (ENOTCONN), but the kernel still
;; lists it. Java's `slurp` fails on procfs, so read it with `cat`. An
;; unreadable table counts as unlisted.
(defn- listed-mount? [dir]
  (try (boolean (some #(= dir (second (str/split % #" ")))
                      (str/split-lines
                        (:out (shell/sh "cat" "/proc/self/mounts")))))
       (catch Exception _ false)))

(defn- detach!
  "Unmounts `dir`. Lazy mode also detaches a dead mount. Fails harmlessly
  when nothing is mounted."
  [dir]
  (when-not (zero? (:exit (shell/sh "fusermount3" "-u" dir)))
    (shell/sh "fusermount3" "-uz" dir)))

(defn- lazyfs-running? [test node]
  (zero? (:exit (shell/sh "pgrep" "-f" (lazy-config test node)))))

(defn lazyfs-mount! [test node]
  (let [dir (data-dir test node)]
    (sh! "mkdir" "-p" dir (lazy-root test node))
    (sh! "chmod" "777" dir (lazy-root test node))
    (spit (lazy-config test node)
          (str "[faults]\nfifo_path=\"" (lazy-fifo test node) "\"\n"
               "[cache]\napply_eviction=false\n"
               "[cache.simple]\ncustom_size=\"1gb\"\nblocks_per_page=1\n"
               "[filesystem]\nlog_all_operations=false\nlogfile=\"\"\n"))
    (sh! "sh" "-c" (str "(" (:lazyfs test) " " dir " --config-path " (lazy-config test node)
                        " -o allow_other -o modules=subdir -o subdir=" (lazy-root test node)
                        " < /dev/null >> " (lazy-log test node) " 2>&1) &"))
    ;; LazyFS fills its whole cache before it mounts, which is slow on a
    ;; small runner.
    (await! (str "lazyfs on " dir) 60000 #(mounted? dir))))

(defn lose-unsynced!
  "Drops the writes that `node` did not fsync. Call it while the node is
  down: LazyFS then acts as if the machine lost power."
  [test node]
  (when (lazyfs? test node)
    (sh! "sh" "-c" (str "echo lazyfs::clear-cache > " (lazy-fifo test node)))
    (info "lost unsynced writes of" node)))

(defn- lazyfs-unmount!
  "Unmounts and waits for LazyFS to exit: a daemon that is still alive holds
  its cache, and the next test needs that memory to mount. The unmount is
  best effort: a dead mount can only break its own test."
  [test node]
  (let [dir (data-dir test node)]
    (detach! dir)
    (when (listed-mount? dir)
      (warn "could not unmount" dir))
    (try (await! (str "lazyfs of " node " to exit") 30000
                 #(not (lazyfs-running? test node)))
         (catch Exception _
           (shell/sh "pkill" "-KILL" "-f" (lazy-config test node))))
    (when (.exists (io/file (lazy-log test node)))
      (io/copy (io/file (lazy-log test node))
               (store/path! test (str node ".lazyfs.log"))))
    (shell/sh "rm" "-rf" (lazy-root test node) (lazy-log test node)
                (lazy-config test node) (lazy-fifo test node))))

;; The DB

(defn- prepare! [test]
  (let [dir (base test)]
    (sh! "mkdir" "-p" dir (bucket test))
    ;; The node users must be able to reach their own directories.
    (sh! "chmod" "755" (:dir test) dir)
    (sh! "chmod" "777" (bucket test))
    (sh! "install" "-m" "755" (:binary test) (bin test))))

(defn db
  "Every node is a Jepsen node. The primary prepares the shared directories
  first; `setup!` runs on all nodes in parallel."
  []
  (let [prepared (promise)]
    (reify
      db/DB
      (setup! [_ test node]
        (if (= node (first (:nodes test)))
          (do (prepare! test) (net-setup! test) (deliver prepared true))
          @prepared)
        (if (lazyfs? test node)
          (lazyfs-mount! test node)
          (do (sh! "mkdir" "-p" (data-dir test node))
              (sh! "chown" (str node ":" node) (data-dir test node))))
        (spit (config-file test node) (config test node))
        (start! test node)
        (await-ready node 60000)
        (when (some #{node} (get-in test [:topology :computes]))
          (await-serving node 60000)))

      (teardown! [_ test node]
        (kill! node)
        (shell/sh "pkill" "-KILL" "-u" node)
        (when (= node (first (:nodes test))) (net-teardown!))
        (when (.exists (io/file (log-file test node)))
          (io/copy (io/file (log-file test node))
                   (store/path! test (str node ".log"))))
        (lazyfs-unmount! test node)
        (shell/sh "rm" "-rf" (data-dir test node) (log-file test node)
                  (config-file test node))
        (when (= node (first (:nodes test)))
          (shell/sh "rm" "-rf" (bucket test) (bin test)))
        ;; Fails while another node still uses the directory.
        (shell/sh "rmdir" (base test))))))
