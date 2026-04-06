package org.shikshalokam.mentoring.dashboard.creator.fixture

object EventsMock {

  val EVENT1: String = """{"reportType":"Mentoring","publishedAt":"2025-09-29 14:57:38","dashboardData":{"tenantCode":"shikshagraha","orgId":"39","orgName":"Tripura"},"_id":"e33f1ad0-29cf-4781-8260-0d57283abda2"}"""
  val EVENT2: String = """{"reportType":"Mentoring","publishedAt":"2025-09-30 14:57:38","dashboardData":{"tenantCode":"shikshagraha","orgId":"8","orgName":"Bengaluru"},"_id":"e33f1ad0-29cf-4781-8260-0d57283axfr4"}"""
  val EVENT3: String = """{"reportType":"Mentoring","publishedAt":"2025-09-30 14:57:38","dashboardData":{"tenantCode":"shikshagraha","orgId":"9","orgName":"Mysore"},"_id":"e33f1ad0-29cf-4781-8260-0d57283axfr4"}"""
  val SYNC_FILTER_1: String = """{"reportType":"Mentoring","publishedAt":"2025-10-14 17:59:21","dashboardData":{"filterTable":"shikshagraha_org_roles","tenantCode":"shikshagraha","filterSync":"Yes"},"_id":"d5a3eec9-9bc0-47ee-b401-be29b062dc1f"}"""
  val SYNC_FILTER_2: String = """{"reportType":"Mentoring","publishedAt":"2025-10-14 12:39:46","dashboardData":{"filterTable":"shikshagraha_sessions","filterSync":"Yes"},"_id":"9e0e0c23-4c47-4496-9bc7-566c0620c9f7"}"""
  val SYNC_FILTER_3: String = """{"reportType":"Mentoring","publishedAt":"2025-10-14 12:39:46","dashboardData":{"filterTable":"shikshagraha_users","tenantCode":"shikshagraha","filterSync":"Yes"},"_id":"9e0e0c23-4c47-4496-9bc7-566c0620c9f7"}"""

}
