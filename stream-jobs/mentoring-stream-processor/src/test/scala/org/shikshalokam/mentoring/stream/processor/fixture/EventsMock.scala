package org.shikshalokam.mentoring.stream.processor.fixture

object EventsMock {

  val SESSION_CREATE: String = """{"eventType":"create","entity":"session","session_id":816,"mentor_id":"2064","name":"session 2 - with 3 mentees","description":"test","tenant_code":"shikshagrahanew","type":"PUBLIC","status":"PUBLISHED","start_date":"2025-10-30 09:57:21","end_date":"2025-10-30 10:57:21","org_id":"41","org_code":"karnataka","org_name":"Karnataka","platform":"Google meet","started_at":null,"completed_at":null,"created_at":"2025-09-30 15:27:47.857000+05:30","updated_at":"2025-10-15 21:28:13.708000+05:30","deleted_at":null,"recommended_for":["deo"],"categories":["educational_leadership"],"medium":["en_in"],"created_by":"1691","updated_by":"1691","deleted":false}"""

  val SESSION_UPDATE: String = """{"eventType":"update","entity":"session","session_id":181,"tenant_code":"shikshagrahanew","oldValues":{"mentor_id":"1858","name":"SSSSSSS","description":"SSSS","type":"PRIVATE","status":"LIVE","org_id":"39","org_code":"tripura","org_name":"Tripura","platform":"BigBlueButton (Default)","started_at":"2025-09-04 15:23:11+05:30","completed_at":null,"created_at":"2025-09-04 14:46:45.404000+05:30","deleted_at":null,"recommended_for":["co"],"categories":["sqaa"],"medium":["en_in"],"created_by":"1858","deleted":false},"newValues":{"status":"COMPLETED","completed_at":"2025-09-05T10:15:00Z"},"updated_by":"1858","updated_at":"2025-09-05T10:15:00Z"}"""

  val SESSION_DELETE: String = """{"eventType":"delete","entity":"session","tenant_code":"shikshagrahanew","session_id":181,"deleted":true,"status": "DELETED","deleted_at":"2025-09-06T08:00:00Z"}"""

  val SESSION_ATTENDANCE_CREATE: String = """{"eventType":"create","entity":"attendance","attendance_id":19,"tenant_code":"shikshagrahanew","session_id":973,"mentee_id":"136","joined_at":null,"left_at":null,"is_feedback_skipped":false,"type":"ENROLLED","created_at":"2025-05-19 11:36:28.870000+05:30","updated_at":"2025-05-19 11:36:28.870000+05:30","deleted_at":null,"deleted":false}"""

  val SESSION_ATTENDANCE_UPDATE: String = """{"eventType":"update","entity":"attendance","attendance_id":39,"tenant_code":"shikshagrahanew","oldValues":{"session_id":181,"mentee_id":"224","joined_at":"2025-05-22 13:12:35+05:30","left_at":null,"is_feedback_skipped":false,"type":"ENROLLED","created_at":"2025-05-22 13:12:07.676000+05:30","deleted_at":null,"deleted":false},"newValues":{"status":"COMPLETED","left_at":"2025-09-05T12:00:00Z"},"updated_at":"2025-09-05T12:00:00Z"}"""

  val ORG_MENTOR_RATING_CREATE: String = """{"eventType":"create","entity":"rating","tenant_code":"shikshagrahanew","org_id":"8","org_name":"Bengaluru","mentor_id":"1838","rating":4,"rating_updated_at":"2025-09-17 12:02:06.042000+05:30","deleted":false}"""

  val ORG_MENTOR_RATING_UPDATE: String = """{"eventType":"update","entity":"rating","tenant_code":"shikshagrahanew","oldValues":{"org_id":"8","org_name":"Bengaluru","mentor_id":"1838","rating":4,"deleted":false},"newValues":{"rating":5},"rating_updated_at":"2025-09-06T09:00:00Z"}"""

  val CONNECTIONS_CREATE: String = """{"eventType":"create","entity":"connections","connection_id":30,"tenant_code":"shikshagrahanew","user_id":"1701","friend_id":"1698","status":"REQUESTED","org_id":"39","created_by":"1701","updated_by":"1698","created_at":"2025-09-03 09:38:37.916000+05:30","updated_at":"2025-09-03 09:38:37.916000+05:30","deleted_at":null,"deleted":false}"""

  val CONNECTIONS_UPDATE: String = """{"eventType":"update","entity":"connections","connection_id":30,"tenant_code":"shikshagrahanew","oldValues":{"user_id":"1701","friend_id":"1698","status":"REQUESTED","org_id":"39","created_by":"1701","created_at":"2025-09-03 09:38:37.916000+05:30","deleted_at":null,"deleted":false},"newValues":{"status":"ACCEPTED"},"updated_by":"1698","updated_at":"2025-09-05T11:00:00Z"}"""

  val CONNECTIONS_DELETE: String = """{"eventType":"delete","entity":"connections","tenant_code":"shikshagrahanew","connection_id":30,"deleted":true,"status": "DELETED","deleted_at":"2025-09-06T08:30:00Z"}"""
}