package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = Signal {
    val bv = b()
    bv*bv - 4*a()*c()
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = Signal {
    val av = a()
    val bv = b()
    delta() match {
      case dv if dv<0 => Set()
      case 0 => Set((-bv)/2/av)
      case dv => Set((-bv+Math.sqrt(dv))/2/av, (-bv-Math.sqrt(dv))/2/av)
    }
  }
}
