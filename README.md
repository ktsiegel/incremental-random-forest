# ml-project
Tools to accelerate the process of building machine learning models

[How to View Event Logs]
Step 0. Set $SPARK environment variable to point to Spark distribution on machine
Step 1. Create directory 'log' at root
Step 2. Run LogisticRegressionSuite.scala
Step 3. Run $SPARK/sbin/start-history-server.sh log
Step 4. Open browser window at url localhost:18080