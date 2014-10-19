package ch7

import java.util.concurrent._


/**
 * User: jooyunghan
 * Date: 10/19/14 7:26 PM
 */

object Par {
  type Par[A] = ExecutorService => Future[A]

  private case class UnitFuture[A](get: A) extends Future[A] {
    def isDone = true
    def get(time: Long, unit: TimeUnit) = get
    def isCancelled = false
    def cancel(evenIfRunning: Boolean): Boolean = false
  }
  private case class MapFuture[A, B, C](a: Future[A], b: Future[B], f: (A, B) => C) extends Future[C] {
    var isDone: Boolean = false

    def isCancelled = false

    def cancel(evenIfRunnig: Boolean): Boolean = false

    def get: C = f(a.get, b.get)

    def get(time: Long, unit: TimeUnit): C = {
      val millis = TimeUnit.MILLISECONDS.convert(time, unit)
      val start = System.currentTimeMillis
      val av = a.get(time, unit)
      val elapsed = System.currentTimeMillis - start
      val bv = b.get(millis - elapsed, TimeUnit.MILLISECONDS)
      val result = f(av, bv)
      isDone = true
      result
    }
  }

  // create Par using immediate result
  def unit[A](a: A): Par[A] = (es: ExecutorService) => UnitFuture(a)

  //
  def map2[A, B, C](a: Par[A], b: Par[B])(f: (A, B) => C): Par[C] =
    (es: ExecutorService) => MapFuture(a(es), b(es), f)

  // marks a computation for concurrent evaluation by run
  def fork[A](a: => Par[A]): Par[A] =
    (es: ExecutorService) => es.submit(new Callable[A] {
      def call = a(es).get
    })

  def lazyUnit[A](a: =>A): Par[A] = fork(unit(a))


  def run[A](es: ExecutorService)(a: Par[A]): A = a(es).get

  def run[A](es: ExecutorService, timeout: Long, unit:TimeUnit)(a: Par[A]): A =
    a(es).get(timeout, unit)

  def main(args: Array[String]): Unit = {
    println(run(Executors.newFixedThreadPool(2), 1, TimeUnit.MILLISECONDS)(sum(1.to(10000).toArray)))
    println(1L.to(10000L).sum)
  }

//  def sum(ints: IndexedSeq[Int]): Int =
//    if (ints.size <= 1)
//      ints.headOption getOrElse 0
//    else {
//      val (l, r) = ints.splitAt(ints.length / 2)
//      val sumL: Par[Int] = Par.unit(sum(l))
//      val sumR: Par[Int] = Par.unit(sum(r))
//      Par.get(sumL) + Par.get(sumR)
//

  def sum(ints: IndexedSeq[Int]): Par[Int] =
    if (ints.size <= 1)
      Par.unit(ints.headOption getOrElse 0)
    else {
      val (l, r) = ints.splitAt(ints.size / 2)
      Par.map2(sum(l), sum(r))(_ + _)
    }

}

