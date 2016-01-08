# What's Cooking
## Description
This is a Kaggle competition in which competitor are asked to determine the type of cuisine (e.g. 
Italian, Greek) based on the ingredients in the dish (e.g. tomatoes, shredded parmesan).

## Data
Download the data from the [Kaggle competition URL](https://www.kaggle.com/c/whats-cooking).

## Preprocessing
Before you can run the benchmark, you need to pre-process the data using the 
`scripts/preprocess_whats_cooking.js` script first (remember to use `npm install` to install 
the dependencies first).

This script will put each JavaScript object on a single line, allow subsampling, and allow selecting
a subset of the cuisine types.

## Benchmark
To run the benchmark, first preprocess the data (if you are doing two class classification, then
remember to pick a subset of the cusines).

Now, run

```
sbt clean && sbt assembly
```

Then, you can either run the benchmark in simple mode:

```
spark-submit —master local[4] —class "edu.mit.csail.db.ml.benchmarks.WhatsCooking" target/scala-2.11/ml.jar <path_to_preprocessed_whats_cooking_json> simple
```

This will train a Logistic Regression classifier. If you omit the "simple" positional argument,

```
spark-submit —master local[4] —class "edu.mit.csail.db.ml.benchmarks.WhatsCooking" target/scala-2.11/ml.jar <path_to_preprocessed_whats_cooking_json>
```

then the benchmark will train a one-vs-rest classifer based on Logistic Regression.
