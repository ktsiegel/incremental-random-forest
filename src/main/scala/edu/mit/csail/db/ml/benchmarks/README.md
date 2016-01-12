# What's Cooking
## Description
This is a Kaggle competition in which competitors are asked to determine the type of cuisine (e.g.
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

Then, you can run the binary benchmark to train a Logistic Regression model for binary classification
and specify whether to use Wahoo or Spark classes:

```
spark-submit --master local[4] --class "edu.mit.csail.db.ml.benchmarks.whatscooking.Binary" target/scala-2.11/ml.jar <path_to_preprocessed_whats_cooking_json> <wahoo OR spark>
```

You can also train a multiclass classifier (a one vs. rest classifier based on Logistic Regression).

```
spark-submit --master local[4] --class "edu.mit.csail.db.ml.benchmarks.whatscooking.Multiclass" target/scala-2.11/ml.jar <path_to_preprocessed_whats_cooking_json> <wahoo OR spark>
```

# MNIST
This is a Kaggle competition in which competitors are asked to classify pictures of handwritten digits
(e.g. picture of "1" -> 1).

## Data
Download the data from the [Kaggle competition URL](https://www.kaggle.com/c/digit-recognizer).

## Preprocessing
No preprocessing is required.

## Benchmark
First run

```
sbt clean && sbt assembly
```

Then, run

```
spark-submit --master local[4] --class "edu.mit.csail.db.ml.benchmarks.mnist.Classifier" target/scala-2.11/ml.jar <path_to_mnist_csv> <wahoo|spark>
```