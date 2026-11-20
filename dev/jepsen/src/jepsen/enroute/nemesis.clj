(ns jepsen.enroute.nemesis
  "Faults on local Enroute processes: partitions, kills, pauses, and changes
  to the ledger's members."
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen [generator :as gen]
                    [nemesis :as n]
                    [util :as util]]
            [jepsen.enroute [cluster :as cluster]
                            [http :as http]
                            [membership :as membership]]))

(defn- ledgers [test] (get-in test [:topology :ledgers]))
(defn- computes [test] (get-in test [:topology :computes]))

(defn- grudge
  "A grudge over the ledger nodes, or between a compute node and some ledger
  nodes."
  [test kind]
  (let [ls (shuffle (ledgers test))]
    (case kind
      :one      (n/complete-grudge (n/split-one ls))
      :majority (n/complete-grudge (n/bisect ls))
      :ring     (n/majorities-ring ls)
      :bridge   (n/bridge ls)
      :compute  (let [c   (rand-nth (computes test))
                      cut (take (inc (rand-int (count ls))) ls)]
                  (n/complete-grudge [[c] cut]))
      ;; One ledger node falls behind, and one compute node can reach only
      ;; that node, so it must follow again from a stale ledger.
      :lagging  (let [[l & others] ls
                      c (rand-nth (computes test))]
                  (merge-with into
                              (n/complete-grudge [[l] others])
                              (n/complete-grudge [[c] others]))))))

(defn- repo-status
  "What ledger `node` knows of `repo`'s log, from `GET /status/repos/<repo>`."
  [node repo]
  (try
    (some-> (http/get-json (str "http://127.0.0.1:" (cluster/internal-port node)
                                "/status/repos/" repo)
                           {:timeout 1000})
            (assoc :name node))
    (catch Exception _ nil)))

(defn leader
  "The sequencer of repository `r`, and how the suite knows it:
  - :majority: a majority of ledger nodes name it, at the highest such term.
  - :claim: no majority agrees; the node that says it leads, at the highest
    term. It can be a leader that a partition deposed.
  - :random: no node knows; any ledger node."
  [test r]
  (let [repo     (str "jepsen-" r)
        ;; In parallel: a paused or cut-off node takes the whole timeout.
        statuses (keep identity (pmap #(repo-status % repo) (ledgers test)))
        by-id    (into {} (map (juxt :node :name) statuses))
        agreed   (->> statuses
                      (filter :sequencer)
                      (group-by (juxt :term :sequencer))
                      ;; A log has 3 replicas, however many ledger nodes there are.
                      (filter #(<= (util/majority (min 3 (count (ledgers test)))) (count (val %))))
                      (sort-by (comp first key))
                      last)
        claim    (->> statuses (filter :leading) (sort-by :term) last)]
    (cond
      (and agreed (by-id (second (key agreed))))
      [(by-id (second (key agreed))) :majority]

      claim [(:name claim) :claim]
      :else [(rand-nth (ledgers test)) :random])))

(defn- target-leader
  "Finds the leader of the hot repository (the one most operations go to),
  and the op value that names it."
  [test]
  (let [r       @(:hot-repo test)
        [l how] (leader test r)]
    [l [l (str "jepsen-" r) how]]))

(defn- live-ledgers
  "The ledger nodes that are neither killed nor paused."
  [test killed paused]
  (remove (into @killed @paused) (ledgers test)))

(defn nemesis
  "Handles every fault. It remembers what it did so that the matching
  recovery undoes it."
  []
  (let [killed (atom #{}), paused (atom #{})]
    (reify
      n/Reflection
      (fs [_] #{:partition :heal :kill-ledger :kill-leader :kill-compute :start :pause-leader
                :pause-compute :resume})

      n/Nemesis
      (setup! [this test] this)
      (invoke! [_ test op]
        (assoc (case (:f op)
          :partition (let [g (grudge test (:value op))]
                       (cluster/partition! g)
                       (assoc op :value [(:value op) g]))
          :heal (do (cluster/heal!) (assoc op :value :healed))
          :kill-ledger (let [l (rand-nth (ledgers test))]
                         (cluster/kill! l)
                         (cluster/lose-unsynced! test l)
                         (swap! killed conj l)
                         (assoc op :value [l]))
          :kill-leader (let [[l v] (target-leader test)]
                         (cluster/kill! l)
                         (cluster/lose-unsynced! test l)
                         (swap! killed conj l)
                         (assoc op :value v))
          :kill-compute (let [n (rand-nth (computes test))]
                          (cluster/kill! n)
                          (swap! killed conj n)
                          (assoc op :value [n]))
          :start (let [ns @killed]
                   (doseq [n ns] (cluster/start! test n))
                   (doseq [n ns] (try (cluster/await-ready n 30000)
                                      (catch Exception e (warn (.getMessage e)))))
                   (reset! killed #{})
                   (assoc op :value ns))
          :pause-leader (let [[l v] (target-leader test)]
                          (cluster/signal! l "STOP")
                          (swap! paused conj l)
                          (assoc op :value v))
          :pause-compute (let [c (rand-nth (computes test))]
                           (cluster/signal! c "STOP")
                           (swap! paused conj c)
                           (assoc op :value [c]))
          :resume (let [ns @paused]
                    (doseq [n ns] (cluster/signal! n "CONT"))
                    (reset! paused #{})
                    (assoc op :value ns)))
               :type :info))
      (teardown! [_ test]
        (cluster/heal!)
        (doseq [n @paused] (cluster/signal! n "CONT"))))))

(def faults
  "Each fault: the ops that start it, and the op that ends it."
  {:partition-ledger {:start (fn [] {:f :partition, :value (rand-nth [:one :majority :ring :bridge])})
                      :stop  {:f :heal}}
   :partition-compute {:start (fn [] {:f :partition, :value :compute}), :stop {:f :heal}}
   :partition-lagging {:start (fn [] {:f :partition, :value :lagging}), :stop {:f :heal}}
   :kill-ledger      {:start (fn [] {:f :kill-ledger}), :stop {:f :start}}
   :kill-leader      {:start (fn [] {:f :kill-leader}), :stop {:f :start}}
   :kill-compute     {:start (fn [] {:f :kill-compute}), :stop {:f :start}}
   :pause-leader     {:start (fn [] {:f :pause-leader}), :stop {:f :resume}}
   :pause-compute    {:start (fn [] {:f :pause-compute}), :stop {:f :resume}}})

(def fault-names
  "Every fault: those above, and `members`, the member changes of
  [[jepsen.enroute.membership]]."
  (conj (set (map name (keys faults))) "members"))

(defn- cycles
  "Cycles: quiet, then one or two faults at once, held for a while, then
  their recovery."
  [names interval]
  (when (seq names)
    (mapcat (fn [_]
              (let [chosen (take (if (and (< 1 (count names)) (< (rand) 0.3)) 2 1)
                                 (shuffle names))
                    ;; Two partitions at once would replace each other.
                    chosen (if (every? #{:partition-ledger :partition-compute :partition-lagging} chosen)
                             (take 1 chosen)
                             chosen)]
                (concat [(gen/sleep interval)]
                        (map #((:start (faults %))) chosen)
                        [(gen/sleep interval)]
                        (distinct (map #(:stop (faults %)) chosen)))))
            (range))))

(defn package
  "The nemesis for `names`, its generator, and its final generator. Member
  changes run on their own schedule, beside the cycles of the other faults."
  [names interval]
  (let [members (when (some #{:members} names) (membership/package interval))
        names   (remove #{:members} names)]
    {:nemesis         (if members (n/compose [(nemesis) (:nemesis members)]) (nemesis))
     :generator       (apply gen/any (remove nil? [(cycles names interval) (:generator members)]))
     :final-generator (concat (map gen/once [{:f :heal} {:f :resume} {:f :start}])
                              (some-> (:final-generator members) vector))}))
