package com.harrys.hyppo.worker.scheduling

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, PropSpec}

/**
  * Created by jpetty on 2/22/16.
  */
class GompertzFunctionTests extends PropSpec with Matchers with GeneratorDrivenPropertyChecks {

  property("should never produce values outside of 0 < x <= 1.0") {
    forAll(
      Gen.choose(0, Int.MaxValue).label("seconds"),
      Gen.choose(0.0, Double.MaxValue).label("scaleFactor"),
      Gen.choose(0.0, Double.MaxValue).label("delayFactor")) {
      (seconds: Int, scale: Double, delay: Double) =>
        val factor = Sigmoid.gompertzCurveBackoffFactor(seconds, scale, delay)
        factor should be > 0.0
        factor should be <= 1.0
    }
  }

  property("should throw IllegalArgumentExceptions for invalid input values") {
    forAll(
      Gen.choose(Int.MinValue, 0).label("seconds"),
      Gen.choose(0.0, Double.MaxValue).label("scaleFactor"),
      Gen.choose(0.0, Double.MaxValue).label("delayFactor")) {
      (seconds: Int, scale: Double, delay: Double) =>
        an [IllegalArgumentException] shouldBe thrownBy { Sigmoid.gompertzCurveBackoffFactor(seconds, scale, delay) }
    }
    info("negative value for seconds is recognized")

    forAll(
      Gen.choose(0, Int.MaxValue).label("seconds"),
      Gen.choose(Double.MinValue, 0.0).label("scaleFactor"),
      Gen.choose(0.0, Double.MaxValue).label("delayFactor")) {
      (seconds: Int, scale: Double, delay: Double) =>
        an [IllegalArgumentException] shouldBe thrownBy { Sigmoid.gompertzCurveBackoffFactor(seconds, scale, delay) }
    }
    info("negative value for scaleFactor is recognized")

    forAll(
      Gen.choose(0, Int.MaxValue).label("seconds"),
      Gen.choose(0.0, Double.MaxValue).label("scaleFactor"),
      Gen.choose(Double.MinValue, 0.0).label("delayFactor")) {
      (seconds: Int, scale: Double, delay: Double) =>
        an [IllegalArgumentException] shouldBe thrownBy { Sigmoid.gompertzCurveBackoffFactor(seconds, scale, delay) }
    }
    info("negative value for delayFactor is recognized")
  }
}
