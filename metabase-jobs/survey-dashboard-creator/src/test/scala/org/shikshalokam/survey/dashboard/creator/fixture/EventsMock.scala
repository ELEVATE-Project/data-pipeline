package org.shikshalokam.survey.dashboard.creator.fixture

object EventsMock {

  val METABASE_DASHBOARD_EVENT: String = """{"reportType":"Survey","publishedAt":"2025-05-05 20:24:07","dashboardData":{"targetedProgram":"67937ab3922600d2c0c8cb58","targetedSolution":"67937ab6922600d2c0c8cb85", "admin":"1"},"_id":"c6a07fed-f4b9-4462-b5e1-6a0174e9fc01"}"""

  val METABASE_DASHBOARD_EVENT_WITHOUT_ADMIN: String = """{"reportType":"Survey","publishedAt":"2025-05-05 20:24:07","dashboardData":{"targetedProgram":"67937ab3922600d2c0c8cb58","targetedSolution":"67937ab6922600d2c0c8cb85"},"_id":"c6a07fed-f4b9-4462-b5e1-6a0174e9fc02"}"""

  val METABASE_DASHBOARD_EVENT_1_WITHOUT_PROGRAM: String = """{"reportType":"Survey","publishedAt":"2025-05-05 20:24:07","dashboardData":{"targetedSolution":"67937ab6922600d2c0c8cb85"},"_id":"c6a07fed-f4b9-4462-b5e1-6a0174e9fc03"}"""

  val MULTISOLUTION_EVENT: String = """{"reportType":"Survey","publishedAt":"2025-09-11 12:47:30","dashboardData":{"targetedSolution":"68b152412df127c1bf2b2f27"},"_id":"eb5942ac-cbcf-410a-a258-8250a5d51c90"}"""

  val UPDATE_FILTER_DATA_EVENT: String = """{"reportType":"Survey","publishedAt":"2025-09-22 17:24:37","dashboardData":{"targetedSolution":"68b152412df127c1bf2b2f27","filterTable":"68b152412df127c1bf2b2f27_survey_status","filterSync":"Yes"},"_id":"8235b8f6-0a7b-43d7-9b71-42fc7469db93"}"""

  val UPDATE_FILTER_DATA_EVENT_2: String = """{"reportType":"Survey","publishedAt":"2025-09-25 14:35:02","dashboardData":{"targetedSolution":"68b152412df127c1bf2b2f27","filterTable":"68b152412df127c1bf2b2f27_survey_status","filterSync":"Yes"},"_id":"9afda0b9-3d79-466e-9862-7afcfbe19f79"}"""
}
