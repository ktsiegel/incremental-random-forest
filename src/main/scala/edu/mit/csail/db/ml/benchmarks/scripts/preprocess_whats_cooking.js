/**
 * Execute this script as follows.
 *
 * node preprocess_whats_cooking.js <path_to_training_json> <num_samples_in_training_set>
 *
 * It will process the training json file (get it from the Kaggle competition 
 * https://www.kaggle.com/c/whats-cooking) and put each training example on a single line so that 
 * Spark's JSON reader can parse it.
 *
 * You can also control the number of samples to use in the training set. I recommend using about 
 * 1000 because Spark seems to be extremely slow and memory intensive on a single-node.
 *
 * The results will be printed to STDOUT, so make sure to redirect STDOUT to an output file.
 */
var fs = require("fs");

var args = process.argv.slice(2)
var path_to_training_json= args[0];

var train = JSON.parse(fs.readFileSync(path_to_training_json));

var num_samples = train.length;
if (args.length > 1) {
  num_samples = args[1];
}

var output = train.filter(function(obj, index) {
  return index <= num_samples;
}).map(function(obj) {
  return JSON.stringify(obj);
}).reduce(function(a, b) {
  return a + "\n" + b;
});

console.log(output)
