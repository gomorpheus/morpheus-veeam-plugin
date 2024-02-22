package com.morpheusdata.veeam.backup.scvmm

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.BackupRepository
import com.morpheusdata.model.BackupResult
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.veeam.backup.VeeamBackupExecutionProviderInterface
import com.morpheusdata.veeam.backup.VeeamBackupTypeProvider
import com.morpheusdata.veeam.services.ApiService
import groovy.util.logging.Slf4j

@Slf4j
class VeeamScvmmBackupExecutionProviderInterface implements VeeamBackupExecutionProviderInterface {

	Plugin plugin
	MorpheusContext morpheus
	ApiService apiService
	VeeamBackupTypeProvider backupTypeProvider

	VeeamScvmmBackupExecutionProviderInterface(Plugin plugin, MorpheusContext morpheus, VeeamBackupTypeProvider backupTypeProvider, ApiService apiService) {
		this.plugin = plugin
		this.morpheus = morpheus
		this.apiService = apiService
		this.backupTypeProvider = backupTypeProvider
	}

	@Override
	ServiceResponse startVeeamZip(Backup backup, BackupProvider backupProvider, LinkedHashMap<String, Object> authConfig, backupServerId, String veeamHierarchyRef, BackupResult lastResult, ComputeServer computeServer) {
		Locale locale = morpheus.services.webRequest.getLocale()
		def error = morpheus.services.webRequest.getMessage("gomorpheus.error.veeam.hypervOndemandFullBackupNotSupported", null, locale)
		return ServiceResponse.error(error)
	}
}
