package org.shikshalokam.user.service.fixture

object ProgramEventsMock {

  val PROGRAM_CREATE_EVENT_WITH_USERNAME = """{"entity":"program","eventType":"create","username":"alex0098","userId":101,"meta":{"programInformation":{"name":"DCPCR School Development Index 2018-19","externalId":"PROGRAM_EXT","id":"67c82d7abad58c889bc5a627"}}}"""

  val PROGRAM_DELETE_EVENT_WITH_USERNAME = """{"entity":"program","eventType":"delete","username":"alex0098","userId":101,"meta":{"programInformation":{"name":"DCPCR School Development Index 2018-19","externalId":"PROGRAM_EXT","id":"67c82d7abad58c889bc5a627"}}}"""

}
