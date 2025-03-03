mvn clean package

#a1
#hadoop jar target/assignments-1.0.jar ca.uwaterloo.cs451.a1.PairsPMI  -input data/Shakespeare.txt -output cs451-bigdatateach-a1-shakespeare-pairs -reducers 5 -threshold 10
#hadoop jar target/assignments-1.0.jar ca.uwaterloo.cs451.a1.StripesPMI  -input data/Shakespeare.txt -output cs451-bigdatateach-a1-shakespeare-pairs -reducers 5 -threshold 10

#a2
#spark-submit --class ca.uwaterloo.cs451.a2.ComputeBigramRelativeFrequencyPairs target/assignments-1.0.jar --input data/Shakespeare.txt --output cs451-lintool-a2-shakespeare-bigrams-pairs --reducers 5
#spark-submit --class ca.uwaterloo.cs451.a2.ComputeBigramRelativeFrequencyStripes target/assignments-1.0.jar --input data/Shakespeare.txt  --output cs451-lintool-a2-shakespeare-bigrams-stripes --reducers 5
#spark-submit --class ca.uwaterloo.cs451.a2.PairsPMI target/assignments-1.0.jar --input data/Shakespeare.txt --output cs451-lintool-a2-shakespeare-pmi-pairs --reducers 5 --threshold 10
#spark-submit --class ca.uwaterloo.cs451.a2.StripesPMI target/assignments-1.0.jar --input data/Shakespeare.txt --output cs451-lintool-a2-shakespeare-pmi-stripes --reducers 5 --threshold 10

#a3
hadoop jar target/assignments-1.0.jar ca.uwaterloo.cs451.a3.BuildInvertedIndexCompressed -input data/Shakespeare.txt -output cs451-bigdatateach-a3-index-shakespeare -reducers 4
#hadoop jar target/assignments-1.0.jar ca.uwaterloo.cs451.a3.BooleanRetrievalCompressed -index cs451-bigdatateach-a3-index-shakespeare -collection data/Shakespeare.txt -query "outrageous fortune AND"