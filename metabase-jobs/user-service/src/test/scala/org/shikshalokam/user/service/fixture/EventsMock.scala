package org.shikshalokam.user.service.fixture

object EventsMock {

  val USER_CREATE_EVENT: String = """{"entity":"user","eventType":"create","entityId":101,"changes":{},"created_by":4,"name":"Olympian Anthony Adam","username":"olympianan_vzqxd3x_uuyuer","email":"olympian1@yopmail.com","phone":"e5a1a59d50220e56e25f95edf58b5f7b","organization_id":22,"tenant_code":"shikshagraha","meta":{"block":{"name":"BADAMI","id":"67c82d7abad58c889bc5a627"},"state":{"name":"Karnataka","id":"67c82d0c538125889163f197"},"school":{"name":"GOVT- HIGHER PRIMARY SCHOOL ADAGAL","id":"67c82d9553812588916410d3"},"cluster":{"name":"HALAKURKI","id":"67c82d53538125889163f19a"},"district":{"name":"BAGALKOT","id":"67c82d7abad58c889bc5a628"},"professional_role":{"name":"Teacher","id":"681b07b49c57cdcf03c79ae3"},"professional_subroles":{"name":"Teacher (Class 3-5),Counsellor,Librarian","id":["682300d34e2812081f34265c","682301994e2812081f34267c","682301f14e2812081f34268c"]}},"status":"ACTIVE","deleted":false,"id":101,"user_roles":[{"title":"mentor","id":26}]}"""

  val USER_DELETE_EVENT: String = """{"entity":"user","eventType":"delete","entityId":101,"changes":{},"created_by":4,"organization_id":22,"tenant_code":"shikshagraha","status":"INACTIVE","deleted":true,"id":101}"""

}
