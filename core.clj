(ns project.core
  (:require [clojure.string :as str]
            [parallel.core :as p]
            )
  (:gen-class))


(use 'clojure.java.io)

(defn outGoingLinksMap [file]

  (def outLinksMap {})
  (with-open [rdr (reader file)]
    (doseq [line (line-seq rdr)]
      (def outLinksMap (assoc outLinksMap (first (str/split line #" ")) (rest(str/split line #" ") )))
      )
    )
  )


(defn outGoingLinksCount [file]

  (def outCountMap {})
  (with-open [rdr (reader file)]
    (doseq [line (line-seq rdr)]
      (def outCountMap (assoc outCountMap (first (str/split line #" ")) (count(rest(str/split line #" ")))))
      )
    )
  )


(defn initialRank [file]
  
  (def initialPageRank {})
  (with-open [rdr (reader file)]
    (doseq [line (line-seq rdr)]
      (def initialPageRank (assoc initialPageRank (first (str/split line #" ")) 1))
      )
    )
  )


(def inComingLinksMap
  (reduce #(merge-with into %1 %2) (map #(zipmap %2 (repeat (count %2) [%1])) (keys outLinksMap) (vals outLinksMap))))


(def damping-factor 0.85)


(defn calcPageRank [ranksMap page]
 ; result = pr(b)/ol(b) + pr(c)/ol(c)
  (reduce +
          
            (for [link (get inComingLinksMap page)]
             (/ (get ranksMap link) (get outCountMap link)))
              )
                 )
              

(defn calcIndexPageRank [ranksMap page]
  (+ (- 1 damping-factor) (* damping-factor (calcPageRank ranksMap page)))
 )


(defn updatePageRank [ranksMap threadCount]
  ;update page with new pagerank
  (let [klist (keys initialPageRank)]
      (def newPageRank (zipmap klist (p/pmap #(calcIndexPageRank ranksMap %) klist threadCount)))
     )
    )

(defn runThreeTimes [threadCount]
  
  (dotimes [x 3]
    (time(
    (updatePageRank initialPageRank threadCount)
    (dotimes [x 999]
      (updatePageRank newPageRank threadCount)
      )
          )
         )
    )
  )
  
  
    

(defn -main
  "I don't do a whole lot ... yet."
  [& args]

  (outGoingLinksMap "pages.txt")
  (outGoingLinksCount "pages.txt")
  (initialRank "pages.txt")
  
  (spit "times.txt" (with-out-str (runThreeTimes 1)))
  (spit "times.txt" "\n" :append true)
  (spit "times.txt" (with-out-str (runThreeTimes 2)) :append true)
  (spit "times.txt" "\n" :append true)
  (spit "times.txt" (with-out-str (runThreeTimes 4)) :append true)
  (spit "times.txt" "\n" :append true)
  (spit "times.txt" (with-out-str (runThreeTimes 8)) :append true)
  (spit "times.txt" "\n" :append true)
  (spit "times.txt" (with-out-str (runThreeTimes 16)) :append true)
  (spit "times.txt" "\n" :append true)
  (spit "times.txt" (with-out-str (runThreeTimes 32)) :append true)
  (spit "times.txt" "\n" :append true)
  (spit "times.txt" (with-out-str (runThreeTimes 64)) :append true)
  
  
  )
