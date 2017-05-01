package kr.acon.generator.skg

import kr.acon.util.Parser

class NaiveNSKG(a: Double, b: Double, c: Double, d: Double, logn: Int, ratio: Int, noise: Double)
    extends SKG(a: Double, b: Double, c: Double, d: Double, logn: Int, ratio: Int) {
  private[generator] def this(p: Parser) {
    this(p.a, p.b, p.c, p.d, p.logn, p.ratio, p.noise)
  }
  
  @inline protected val c_ad = 2 / (a + d)
  @inline protected val c_c = 1 / c
  @inline protected val c_adadab = (a - d) / ((a + d) * (a + b))
  @inline protected val c_adadcd = (a - d) / ((a + d) * (c + d))

  @inline protected val kmu = Array.tabulate(logn)(x => -noise + 2 * noise * math.random)

  @inline protected final def getPoutWithoutNoise(vid: Long) = super.getPout(vid)
  
  @inline private[generator] override def getPout(vid: Long) = {
    val bs = bitSum(vid)
    var tab = 1d
    var x = 0
    while (x < logn) {
      val t = if ((vid >>> x) % 2 == 0) {
        1d - c_adadab * kmu(x)
      } else {
        1d + c_adadcd * kmu(x)
      }
      tab *= t
      x += 1
    }
    abcd(bs) * tab
  }
  
  @inline protected final def getCDFWithoutNoise(vid: Long, logto: Int) = super.getCDF(vid, logto)

  @inline private[generator] override def getCDF(vid: Long, logto: Int) = {
    val bs = bitSum(vid >>> logto)
    val aab = this.aab(logn - logto - bs)
    val ccd = this.ccd(bs)
    var tab = 1d
    var x = 0
    while (x < logn) {
      val t = if (x < logn - logto) {
        if ((vid >>> x) % 2 == 0) {
          1d - c_ad * kmu(x)
        } else {
          1d + c_c * kmu(x)
        }
      } else {
        if ((vid >>> x) % 2 == 0) {
          1d - c_adadab * kmu(x)
        } else {
          1d + c_adadcd * kmu(x)
        }
      }
      tab *= t
      x += 1
    }
    getPout(vid) * aab * ccd * tab
  }
}