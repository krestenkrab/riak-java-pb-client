(defproject mmcgrana/riak-java-pb-client "0.1.0-SNAPSHOT"
  :description
    "Java bindings to the Riak protocol buffers API."
  :java-source-path [["gen"] ["src"]]
  :javac-fork "true"
  :dependencies
    [[com.google.protobuf/protobuf-java "2.3.0"]]
  :dev-dependencies
    [[lein-javac "1.2.1-SNAPSHOT"]])
