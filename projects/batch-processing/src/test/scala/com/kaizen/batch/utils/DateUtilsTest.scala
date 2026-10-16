package com.kaizen.batch.utils

import java.time.{Clock, Instant, LocalDate, ZoneOffset}

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DateUtilsTest extends AnyFunSuite {

  test("load window starts one hour before fromDate and ends at toDate 00:00") {
    assert(DateUtils.windowStart("2026-10-14") == "2026-10-13 23:00:00")
    assert(DateUtils.windowEnd("2026-10-15") == "2026-10-15 00:00:00")
    assert(DateUtils.postgresTimestamp("2026-10-13 23:00:00") == "2026-10-13 23:00:00+07")
  }

  test("fromDate must be before toDate and dates must be yyyy-MM-dd") {
    DateUtils.checkDateRange("2026-10-14", "2026-10-15")
    assert(intercept[IllegalArgumentException](DateUtils.checkDateRange("2026-10-15", "2026-10-15"))
      .getMessage.contains("fromDate must be before toDate"))
    assert(intercept[IllegalArgumentException](DateUtils.checkDateRange("14/10/2026", "2026-10-15"))
      .getMessage.contains("fromDate must be yyyy-MM-dd"))
  }

  test("today is the Vietnam date") {
    // 2026-10-14 18:30 UTC is 2026-10-15 01:30 in Vietnam / 18:30 UTC là 01:30 hôm sau ở Việt Nam
    val clock = Clock.fixed(Instant.parse("2026-10-14T18:30:00Z"), ZoneOffset.UTC)
    assert(DateUtils.today(clock) == LocalDate.of(2026, 10, 15))
  }
}
