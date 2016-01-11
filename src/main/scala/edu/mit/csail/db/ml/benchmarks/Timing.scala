package edu.mit.csail.db.ml.benchmarks

object Timing {
  /**
    * Executes a function f() and measures the time taken and passes that time to handleTime.
    * @param name - The name (or some other identifying tag) of the function being timed.
    * @param f - The function whose execution will be timed.
    * @param handleTime - The function to execute with the name and elapsed time (in ms). By
    *                   default, it is just a println. However, you can provide your own
    *                   implementation to write to a logging system, write to a file, etc.
    */
  def time(name: String)(f: () => Unit)(handleTime: (String, Double) => Unit = (name, elapTime) => {
    println(name + " time: " + elapTime + " seconds")
  }): Unit = {
    val s = System.nanoTime
    f()
    handleTime(name, (System.nanoTime - s)/1e9)
  }
}
