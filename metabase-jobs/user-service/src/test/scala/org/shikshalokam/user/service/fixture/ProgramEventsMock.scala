package org.shikshalokam.user.service.fixture

object ProgramEventsMock {

  val PROGRAM_CREATE_EVENT_WITH_EMAIL = """{"entity":"program","eventType":"create","username":"alex123","email":"programmanager3@yopmail.com","userId":101,"meta":{"programInformation":{"name":"DCPCR School Development Index 2018-19","externalId":"PROGRAM_EXT","id":"67c82d7abad58c889bc5a627"}}}"""

  val PROGRAM_CREATE_EVENT_WITHOUT_EMAIL = """{"entity":"program","eventType":"create","username":"alex123","userId":101,"meta":{"programInformation":{"name":"DCPCR School Development Index 2018-19","externalId":"PROGRAM_EXT","id":"67c82d7abad58c889bc5a627"}}}"""

  val PROGRAM_DELETE_EVENT_WITH_EMAIL = """{"entity":"program","eventType":"delete","username":"alex123","email":"programmanager3@yopmail.com","userId":101,"meta":{"programInformation":{"name":"DCPCR School Development Index 2018-19","externalId":"PROGRAM_EXT","id":"67c82d7abad58c889bc5a627"}}}"""

}
