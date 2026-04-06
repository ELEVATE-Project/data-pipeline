package org.shikshalokam.user.dashboard.creator.fixture

object EventsMock {

  val TENANT1: String = """{"reportType":"User Dashboard","publishedAt":"2025-07-30 14:57:14","dashboardData":{"tenantCode":"shikshalokam"},"_id":"2966fa2f-5e08-4fbd-ae8c-9cc087eb2fa0"}"""
  val TENANT2: String = """{"reportType":"User Dashboard","publishedAt":"2025-07-31 18:12:27","dashboardData":{"tenantCode":"shikshagrahanew"},"_id":"802be515-20e0-46ec-8317-933e598a5baa"}"""
  val SYNC_FILTER_1: String = """{"reportType":"User Dashboard","publishedAt":"2025-09-25 14:47:22","dashboardData":{"tenantCode":"shikshagrahanew","filterTable":"shikshagrahanew_users","filterSync":"Yes"},"_id":"4a32e137-254d-4edf-a63f-bf146a344bb5"}"""
  val SYNC_FILTER_2: String = """{"reportType":"User Dashboard","publishedAt":"2025-09-25 14:47:23","dashboardData":{"tenantCode":"shikshagrahanew","filterTable":"shikshagrahanew_users_metadata","filterSync":"Yes"},"_id":"8d831a9a-4f2d-4e2d-8b1f-849259587bef"}"""
}
