(defproject flowctrl "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main flowctrl.core
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [enlive "1.1.1"]
                 [criterium "0.3.1"]
                 [org.clojure/tools.logging "0.2.3"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]
                 [org.slf4j/slf4j-log4j12 "1.6.6"]])
