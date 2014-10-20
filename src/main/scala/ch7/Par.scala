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

  def map[A, B](a: Par[A])(f: A => B): Par[B] =
    map2(a, unit(()))((a, _) => f(a))

  // marks a computation for concurrent evaluation by run
  def fork[A](a: => Par[A]): Par[A] =
    (es: ExecutorService) => es.submit(new Callable[A] {
      def call = a(es).get
    })

  def lazyUnit[A](a: => A): Par[A] = fork(unit(a))

  def asyncF[A, B](f: A => B): A => Par[B] = (a: A) => lazyUnit(f(a))

  def sequence[A](ps: List[Par[A]]): Par[List[A]] =
    ps.foldRight(unit(List.empty[A]))((a, b) => map2(a, b)(_ :: _))

  def parMap[A, B](ps: List[A])(f: A => B): Par[List[B]] = {
    val fbs: List[Par[B]] = ps.map(asyncF(f))
    sequence(fbs)
  }

  def parFilter[A](as: List[A])(f: A => Boolean): Par[List[A]] =
    as.zip(as.map(asyncF(f))).foldRight(unit(List.empty[A]))({
      case ((a, p), b) => map2(p, b)((p, b) => if (p) a :: b else b)
    })

  def run[A](es: ExecutorService)(a: Par[A]): A = a(es).get

  def run[A](es: ExecutorService, timeout: Long, unit: TimeUnit)(a: Par[A]): A =
    a(es).get(timeout, unit)

  def main(args: Array[String]): Unit = {
    val es = Executors.newFixedThreadPool(10)
    val result = run(es)(parFilter(1.to(10).toList)(x => {
      print(".")
      Thread.sleep(1000)
      x % 2 == 0
    }))
    println(result)
  }

  def sum(ints: IndexedSeq[Int]): Par[Int] =
    if (ints.size <= 1)
      Par.unit(ints.headOption getOrElse 0)
    else {
      val (l, r) = ints.splitAt(ints.size / 2)
      Par.map2(sum(l), sum(r))(_ + _)
    }

}


