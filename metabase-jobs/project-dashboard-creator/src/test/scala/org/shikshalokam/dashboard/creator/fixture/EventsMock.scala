package org.shikshalokam.dashboard.creator.fixture

object EventsMock {

  val METABASE_DASHBOARD_EVENT_1: String = """{"reportType":"Project","publishedAt":"2024-11-13 11:24:47","dashboardData":{"targetedProgram":"66386a1954afe7df153a54d3","targetedSolution":"667a72d1b070696248731d10","targetedDistrict":"66bf7f3560de1616f42cb993","admin":"1","targetedState":"66bf7eb960de1616f42cb984"},"_id":"2998015b-3705-49fb-8a5f-9271b623da18"}"""

  val SAAS_QA_DASHBOARD_EVENT_S1: String = """{"reportType":"Project","publishedAt":"2025-07-27 12:04:37","dashboardData":{"targetedProgram":"68820adcc44d7c00148ffada","targetedSolution":"68828212ab139a00141de7f4","targetedDistrict":"687a2e0bb19aea00144bcd9d","targetedState":"6866477033a96f0013aa8361"},"_id":"60f6e027-2b94-4dc8-ab1a-1cf72a9f9e6a"}"""

  val SAAS_QA_DASHBOARD_EVENT_S2: String = """{"reportType":"Project","publishedAt":"2025-07-27 12:04:38","dashboardData":{"targetedSolution":"6882818eab139a00141de595"},"_id":"03998be7-0074-4ea6-82ad-78d082fedf74"}"""

  val SAAS_QA_DASHBOARD_EVENT_S3: String = """{"reportType":"Project","publishedAt":"2025-07-27 12:04:38","dashboardData":{"targetedSolution":"68820b1fc44d7c00148ffb30"},"_id":"70914729-ca1c-42cb-aa49-1922b90ad58c"}"""

  val SAAS_QA_DASHBOARD_EVENT_S4: String = """{"reportType":"Project","publishedAt":"2025-07-27 12:04:39","dashboardData":{"targetedSolution":"68821e38c44d7c00148fff6c"},"_id":"5f54ccea-edcc-419f-9c3c-c7b84205bae5"}"""

  val SAAS_QA_DASHBOARD_EVENT_S5: String = """{"reportType":"Project","publishedAt":"2025-07-27 12:04:39","dashboardData":{"targetedSolution":"688281e6ab139a00141de727"},"_id":"e4883620-bcbd-4ab0-b530-9eb7c2f4e4ab"}"""

  val MULTISOLUTION_EVENT_1: String = """{"reportType":"Project","publishedAt":"2025-09-11 12:32:43","dashboardData":{"targetedProgram":"68b14cfdab139a00141e2e6c","targetedSolution":"68b7ec7dcd4236001401c5d5","targetedDistrict":"6863aa5f1d52e30014093b42","targetedState":"6863a9941d52e30014093ad9"},"_id":"17a4a3ff-8028-4f9e-82ae-72eee6a08048"}"""

  val TEST_EVENT_1: String = """{"reportType":"Project","publishedAt":"2025-09-23 22:50:27","dashboardData":{"targetedSolution":"68b7ec7dcd4236001401c5d9"},"_id":"f46d6318-5f10-4209-8be5-931037e78ca5"}"""

  val UPDATE_FILTER_DATA_EVENT: String = """{"reportType":"Project","publishedAt":"2025-09-25 16:26:35","dashboardData":{"targetedSolution":"68b7ec7dcd4236001401c5d9","filterTable":"local_projects","filterSync":"Yes"},"_id":"831c2c21-88a6-4d41-bc2c-24ed12000a2a"}"""
}
