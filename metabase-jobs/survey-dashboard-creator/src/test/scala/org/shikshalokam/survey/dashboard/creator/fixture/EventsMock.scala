package org.shikshalokam.survey.dashboard.creator.fixture

object EventsMock {

  val METABASE_DASHBOARD_EVENT: String = """{"reportType":"Survey","publishedAt":"2025-05-05 20:24:07","dashboardData":{"targetedProgram":"67937ab3922600d2c0c8cb58","targetedSolution":"67937ab6922600d2c0c8cb85", "admin":"1"},"_id":"c6a07fed-f4b9-4462-b5e1-6a0174e9fc01"}"""

  val METABASE_DASHBOARD_EVENT_WITHOUT_ADMIN: String = """{"reportType":"Survey","publishedAt":"2025-05-05 20:24:07","dashboardData":{"targetedProgram":"67937ab3922600d2c0c8cb58","targetedSolution":"67937ab6922600d2c0c8cb85"},"_id":"c6a07fed-f4b9-4462-b5e1-6a0174e9fc02"}"""

  val METABASE_DASHBOARD_EVENT_1_WITHOUT_PROGRAM: String = """{"reportType":"Survey","publishedAt":"2025-05-05 20:24:07","dashboardData":{"targetedSolution":"67937ab6922600d2c0c8cb85"},"_id":"c6a07fed-f4b9-4462-b5e1-6a0174e9fc03"}"""

}
