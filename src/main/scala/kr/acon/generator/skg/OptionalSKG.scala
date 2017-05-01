package kr.acon.generator.skg

import kr.acon.util.Parser

// opt = 1 2 4 (#1) (#2) (#3)  
class OptionalSKG(a: Double, b: Double, c: Double, d: Double, logn: Int, ratio: Int, opt: Int = 0)
    extends SKG(a: Double, b: Double, c: Double, d: Double, logn: Int, ratio: Int) {

  private[skg] final def determineEdge0nopt(gp: Double, recVec: Array[Double], sigmas: Array[Double], prev: Int, acc: Long = 0): Long = {
    var k = 0
    var i = 0
    var flag = true
    var p = prev
    while (i < prev) {
      if (recVec(i) <= gp && gp < recVec(i + 1) && flag) {
        k = i + 1
        flag = false
      }
      i += 1
    }
    k match {
      case 0 => {
        if (prev != 0) {
          determineEdge0nopt(gp, recVec, sigmas, prev - 1, 0)
        } else {
          0
        }
      }
      case _ => {
        val sigma = sigmas(k - 1)
        val sp = sigma * (gp - recVec(k - 1))
        (1l << (k - 1)) + determineEdge0nopt(sp, recVec, sigmas, prev - 1, 0)
      }
    }
  }

  // no optimization #1
  private[this] val noptF1: (Double, Array[Double], Array[Double], Int, Long) => Long = if (opt % 2 == 0) determineEdge0BinarySearch else determineEdge0nopt

  // no optimization #2
  private[this] val noptF2: (SKG.randomClass) => Double = if ((opt >>> 1) % 2 == 0) (r => r.nextDouble) else (r => {
    var i = 0
    var ret = new Array[Double](logn)
    while (i < logn) {
      ret(i) = r.nextDouble()
      i += 1
    }
    ret(logn - 1)
  })

  // no optimization #3
  private[this] val noptF3_1: (Int => Double) = if ((opt >>> 2) % 2 == 0) (x => abcd(x)) else (x => math.pow(a + b, logn - x) * math.pow(c + d, x))
  private[this] val noptF3_2: (Int => Double) = if ((opt >>> 2) % 2 == 0) (x => aab(x)) else (x => math.pow(a / (a + b), x))
  private[this] val noptF3_3: (Int => Double) = if ((opt >>> 2) % 2 == 0) (x => ccd(x)) else (x => math.pow(c / (c + d), x))
  private[this] val noptF3_4: ((Long, Array[Double]) => Array[Double]) = if ((opt >>> 2) % 2 == 0) ((x, y) => y) else ((x, y) => getRecVec(x))

  private[generator] override def getPout(vid: Long) = {
    val bs = bitSum(vid)
    noptF3_1(bs)
  }

  private[generator] override def getCDF(vid: Long, logto: Int) = {
    val bs = bitSum(vid >>> logto)
    val aab = noptF3_2(logn - logto - bs)
    val ccd = noptF3_3(bs)
    getPout(vid) * aab * ccd
  }

  private[generator] def this(p: Parser) {
    this(p.a, p.b, p.c, p.d, p.logn, p.ratio, p.opt)
  }

  @inline private[generator] override def determineEdge(u: Long = -1l, RecVec: Array[Double], sigmas: Array[Double], r: SKG.randomClass = random): Long = {
    val rv = noptF3_4(u, RecVec)
    val p = noptF2(r) * rv.last
    noptF1(p, rv, sigmas, logn, 0)
  }
}

