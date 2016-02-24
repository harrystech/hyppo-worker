package com.harrys.hyppo.worker.scheduling

/**
  * Created by jpetty on 2/19/16.
  */
object Sigmoid {

  /**
    * An implementation of a [[https://en.wikipedia.org/wiki/Gompertz_function Gompertz Sigmoid Function]] that asymtotes
    * at 1.0. The parameters provided are used to produce a value between 0.0 and 1.0 which represents the probability
    * that a worker should re-attempt to acquire a resource that has previously failed. Conceptually, this means the
    * coefficient `a` is hard-coded to the value 1 and not configurable.
    *
    * @param seconds The number of seconds since the last failure to acquire the resource, or the `t` value to compute the `y` for
    * @param scaleFactor The growth rate of the Gompertz function, corresponding to the coefficient `c`
    * @param delayFactor The shift along the x-axis of the Gompertz function, corresponding to the coefficient `b`
    * @return A value between 0.0 and 1.0 indicating the probability that a worker should re-attempt to acquire a resource
    */
  def gompertzCurveBackoffFactor(seconds: Int, scaleFactor: Double, delayFactor: Double): Double = {
    if (seconds < 0) {
      throw new IllegalArgumentException(s"The number of seconds must be greater than or equal to 0. Received: $seconds")
    }
    if (scaleFactor <= 0.0 || delayFactor <= 0.0) {
      throw new IllegalArgumentException(s"The scaleFactor and delayFactor must be greater than 0. Received scaleFactor: $scaleFactor delayFactor: $delayFactor")
    }
    val ct = scaleFactor * seconds
    val be = delayFactor * Math.pow(Math.E, -ct)
    Math.pow(Math.E, -be)
  }
}
