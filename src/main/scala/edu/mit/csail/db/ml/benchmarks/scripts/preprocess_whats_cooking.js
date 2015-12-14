/**
 * REMEMBER TO DO `npm install` first.
 *
 * Execute this script as follows.
 *
 * node preprocess_whats_cooking.js -i <path_to_input> -c <num_classes> -s <num_samples>
 * -s = The number of training examples to use.
 * -c = If num_classes = k, this will only use the top k-1 (in terms of frequency) classes.
 *  The rest will be thrown into a class called "other".
 * -i = The path to the input training file.
 *
 * The script will process the training json file (get it from the Kaggle competition 
 * https://www.kaggle.com/c/whats-cooking) and put each training example on a single line so that 
 * Spark's JSON reader can parse it.
 *
 * You can control the number of samples in the training set. Use the value "all" if you'd like
 * to use all the samples. You can also select the number of classes to use.
 *
 * The results will be printed to STDOUT, so make sure to redirect STDOUT to an output file.
 */

// TODO: While this is NOT a problem right now, if you find that this script runs slowly because the
// dataset is too large. You may want to convert the map/filter/reduce calls into for-loops to reduce
// the number of scans through the dataset and the number of arrays created in memory.

var fs = require("fs");
var argv = require("optimist").argv;

// Figure out where the input file is and read from it.
var path_to_training_json = argv.i;
var train = JSON.parse(fs.readFileSync(path_to_training_json));

// Figure out the number of desired samples.
var num_samples = train.length;
if (argv.s) {
  num_samples = Math.min(parseInt(argv.s, 10), num_samples);
}

// Construct the (cuisine -> frequency) table.
var frequency_table = train.map(function(obj) {
  return obj.cuisine;
}).reduce(function(prev_dict, curr_cuisine) {
  if (prev_dict[curr_cuisine] === undefined) {
    prev_dict[curr_cuisine] = 0;
  }
  prev_dict[curr_cuisine]++;
  return prev_dict;
}, {});

// Figure out the number of desired classes.
var num_classes = Object.keys(frequency_table).length;
if (argv.c) {
  num_classes = Math.min(parseInt(argv.c, 10), num_classes);
}

// Choose the top num_classes classes.
var chosen_classes = Object.keys(frequency_table).map(function(key) {
  return [key, frequency_table[key]];
}).sort(function(a, b) {
  return b[1] - a[1];
}).slice(0, num_classes - 1).map(function(pair) {
  return pair[0];
});

// Preprocess the data.
var output = train.filter(function(obj, index) {
  return index <= num_samples;
}).map(function(obj) {
  var copied = JSON.parse(JSON.stringify(obj));
  if (chosen_classes.indexOf(copied.cuisine) < 0) {
    copied.cuisine = "other";
  }
  return JSON.stringify(copied);
}).reduce(function(a, b) {
  return a + "\n" + b;
});

console.log(output)
