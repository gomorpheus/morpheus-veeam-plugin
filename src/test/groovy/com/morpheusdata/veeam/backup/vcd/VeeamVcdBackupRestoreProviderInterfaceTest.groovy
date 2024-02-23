package com.morpheusdata.veeam.backup.vcd

import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupResult
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import spock.lang.Specification

class VeeamVcdBackupRestoreProviderInterfaceTest extends Specification {

	VeeamVcdBackupRestoreProviderInterface veeamVcdBackupRestoreProviderInterface

	def setup() {
		veeamVcdBackupRestoreProviderInterface = Mock(VeeamVcdBackupRestoreProviderInterface)
	}

	def "GetRestoreObjectRef"() {
		setup:
		String objectRef = "urn:vCloud:Vm:a2b0c55d-829a-4efe-bd95-125ee77ba9dd.vapp-123"

		when:
		def result = veeamVcdBackupRestoreProviderInterface.getRestoreObjectRef(objectRef)

		then:
		1 * veeamVcdBackupRestoreProviderInterface.getRestoreObjectRef(objectRef)
		result == "urn:vCloud:Vm:a2b0c55d-829a-4efe-bd95-125ee77ba9dd.123"

	}

	def "BuildApiRestoreOpts"() {
		setup:
		Map authConfig = [:]
		BackupResult backupResult = new BackupResult()
		Backup backup = new Backup()
		ComputeServer server = new ComputeServer()
		Cloud cloud = new Cloud()

		when:
		def result = veeamVcdBackupRestoreProviderInterface.buildApiRestoreOpts(authConfig, backupResult, backup, server, cloud)

		then:
		1 * veeamVcdBackupRestoreProviderInterface.buildApiRestoreOpts(authConfig, backupResult, backup, server, cloud)
		result instanceof Map
	}

	def "BuildRestoreSpec"() {
		setup:
		String restorePath = "path"
		String hierarchyRoot = "root"
		String backupType = "type"
		Map opts = [:]

		when:
		def result = veeamVcdBackupRestoreProviderInterface.buildRestoreSpec(restorePath, hierarchyRoot, backupType, opts)

		then:
		1 * veeamVcdBackupRestoreProviderInterface.buildRestoreSpec(restorePath, hierarchyRoot, backupType, opts)
		result instanceof String
	}
}
