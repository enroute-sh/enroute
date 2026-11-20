(ns jepsen.enroute.checker
  "Checks beyond Elle: per-node monotonic reads, durability, phantoms, and
  fetches."
  (:require [jepsen [checker :as checker]
                    [history :as h]]))

(defn- prefix? [a b] (= a (take (count a) b)))

(defn- oks
  "Completed :ok client ops of function `f`."
  [history f]
  (->> history h/client-ops h/oks (h/filter-f f)))

(defn- read-ops
  "Completed :ok ref reads (not barrier reads), each with its invocation
  time."
  [history]
  (->> (oks history :read)
       (map (fn [op] (assoc op :invoke-time (:time (h/invocation history op)))))))

(defn- node-errors
  "Sweeps one node's ops in time order. `known` holds, per key, the longest
  list a completed read saw and every value a completed write appended."
  [node ops]
  (let [completions (vec (sort-by :time ops))
        reads       (sort-by :invoke-time (filter #(= :read (:f %)) ops))]
    (loop [reads reads, i 0, known {}, errors []]
      (if-let [b (first reads)]
        (if (and (< i (count completions))
                 (< (:time (completions i)) (:invoke-time b)))
          (let [a (completions i)
                known (if (= :read (:f a))
                        (reduce (fn [known [_ k l]]
                                  (if (< (count (get-in known [k :list])) (count l))
                                    (update known k assoc :list l :read a)
                                    known))
                                known (:value a))
                        (reduce (fn [known [f k v]]
                                  (if (= f :append)
                                    (update-in known [k :wrote] (fnil conj []) [a v])
                                    known))
                                known (:value a)))]
            (recur reads (inc i) known errors))
          (let [errs (for [[_ k seen] (:value b)
                           :let [{:keys [list read wrote]} (get known k)
                                 missing (remove (comp (set seen) second) wrote)]
                           e (concat
                               (when (and list (not (prefix? list seen)))
                                 [{:type :non-monotonic, :node node, :key k,
                                   :earlier (select-keys read [:index :process :time])
                                   :earlier-list list
                                   :later (select-keys b [:index :process :time])
                                   :later-list seen}])
                               (when (seq missing)
                                 [{:type :read-your-writes, :node node, :key k,
                                   :write (select-keys (ffirst missing) [:index :process :time :value])
                                   :missing (map second missing)
                                   :read (select-keys b [:index :process :time])
                                   :read-list seen}]))]
                       e)]
            (recur (rest reads) i known (into errors errs))))
        errors))))

(defn node-monotonic
  "Level 2 across the processes of one compute node. If op A completes on
  node N before read B starts on N, then B sees, for each key, a list that
  extends what A saw (monotonic reads) and that holds what A appended
  (read your writes)."
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [writes (oks history :write)
            errors (mapcat (fn [[node ops]] (node-errors node ops))
                           (group-by :node (concat (read-ops history) writes)))]
        {:valid?      (empty? errors)
         :error-count (count errors)
         :error-types (frequencies (map :type errors))
         :errors      (take 10 errors)}))))

(defn durability
  "Every :ok append is in every final read."
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [finals (oks history :final-read)
            acked  (for [op (oks history :write)
                         [f k v] (:value op) :when (= f :append)]
                     [k v op])
            lost   (for [fr finals
                         :let [lists (into {} (map (fn [[_ k l]] [k (set l)]) (:value fr)))]
                         [k v op] acked
                         :when (not (contains? (get lists k #{}) v))]
                     {:node (:node fr), :key k, :value v, :write-index (:index op)})]
        {:valid?      (cond (empty? finals) :unknown
                            (seq lost) false
                            :else true)
         :final-reads (count finals)
         :acked       (count acked)
         :lost-count  (count lost)
         :lost        (take 20 lost)}))))

(defn phantoms
  "No read returns a commit the harness never wrote."
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [ps (->> history h/client-ops
                    (filter #(= :phantom (first (:error %)))))]
        {:valid? (empty? ps), :count (count ps), :first (take 5 ps)}))))

(defn fetches
  "A fetch of tips that a read returned gets every object, and never fails
  with `not our ref`. Other fetch errors (timeouts during faults) are
  counted, not failed."
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [fs    (->> history h/client-ops h/oks (keep :fetch))
            nor   (filter :not-our-ref? fs)
            short (filter #(and (:objects %) (not= (:objects %) (:expected %))) fs)
            other (filter #(and (:error %) (not (:not-our-ref? %))) fs)
            gits  (->> history h/client-ops (h/filter-f :git-fetch)
                       (filter #(#{:ok :fail} (:type %))))
            fsck  (filter #(= :fsck (first (:error %))) gits)
            gnor  (filter #(re-find #"not our ref" (str (:error %))) gits)]
        {:valid?          (and (empty? nor) (empty? short) (empty? fsck) (empty? gnor))
         :fetches         (count fs)
         :not-our-ref     (count nor)
         :not-our-ref-ex  (take 5 nor)
         :incomplete      (take 5 short)
         :other-errors    (frequencies (map :error other))
         :git-fetches     (frequencies (map :type gits))
         :git-errors      (frequencies (map :error (filter #(= :fail (:type %)) gits)))}))))

(defn resolve-partial-pushes
  "Elle takes an :info transaction as all or nothing, but a push that is not
  atomic and timed out can land only some refs. The final reads say which:
  each commit is unique, and a landed one stays on its branch. Keeps only the
  appends that landed, or fails the push if none did."
  [history]
  (let [finals (oks history :final-read)
        seen   (into #{} (for [op finals, [_ k l] (:value op), v l] [k v]))
        fixed  (into {}
                     (for [op (->> history h/client-ops h/infos)
                           :when (and (= :write (:f op)) (false? (:atomic? op)))
                           :let [landed (filterv (fn [[_ k v]] (seen [k v])) (:value op))
                                 inv    (h/invocation history op)]
                           [i op'] [[(:index inv) (assoc inv :value landed)]
                                    [(:index op) (if (seq landed)
                                                   (assoc op :value landed)
                                                   (assoc op :type :fail, :value landed,
                                                          :resolved :none-landed))]]]
                       [i op']))]
    (if (empty? finals)
      history
      (h/map #(get fixed (:index %) %) history))))

(defn members
  "Member changes end: once the faults stop, every log moves onto members
  only (see `jepsen.enroute.membership`)."
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [settles (filter #(and (= :nemesis (:process %)) (= :settle (:f %))
                                  (some? (:value %)))
                            history)]
        (if-let [op (last settles)]
          {:valid? (= :settled (:value op)), :result (:value op)}
          {:valid? true, :result :no-member-changes})))))
