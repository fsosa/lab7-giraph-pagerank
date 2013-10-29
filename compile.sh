javac -cp lib/giraph.jar:lib/hadoop-core.jar src/test/LiveJournalPageRank.java -d ./
cp lib/giraph.jar pagerank.jar
jar uf pagerank.jar test/LiveJournalPageRank.class
