package com.morpheusdata.veeam.backup.hyperv

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.BackupResult
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.veeam.backup.VeeamBackupExecutionProviderInterface
import com.morpheusdata.veeam.backup.VeeamBackupTypeProvider
import com.morpheusdata.veeam.services.ApiService
import groovy.util.logging.Slf4j

@Slf4j
class VeeamHypervBackupExecutionProvider implements VeeamBackupExecutionProviderInterface {

	Plugin plugin
	MorpheusContext morpheus
	ApiService apiService
	VeeamBackupTypeProvider backupTypeProvider

	VeeamHypervBackupExecutionProvider(Plugin plugin, MorpheusContext morpheus, VeeamBackupTypeProvider backupTypeProvider, ApiService apiService) {
		this.plugin = plugin
		this.morpheus = morpheus
		this.apiService = apiService
		this.backupTypeProvider = backupTypeProvider
	}


	@Override
	ServiceResponse startVeeamZip(Backup backup, BackupProvider backupProvider, LinkedHashMap<String, Object> authConfig, backupServerId, String veeamHierarchyRef, BackupResult lastResult, ComputeServer computeServer) {
		def error = morpheus.services.localization.get("gomorpheus.error.veeam.hypervOndemandFullBackupNotSupported")
		return ServiceResponse.error(error)
	}
}
