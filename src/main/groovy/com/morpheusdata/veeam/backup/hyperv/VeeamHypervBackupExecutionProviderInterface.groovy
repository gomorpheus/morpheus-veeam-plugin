package com.morpheusdata.veeam.backup.hyperv

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.veeam.backup.VeeamBackupExecutionProviderInterface
import com.morpheusdata.veeam.backup.VeeamBackupTypeProvider
import com.morpheusdata.veeam.services.ApiService
import groovy.util.logging.Slf4j

@Slf4j
class VeeamHypervBackupExecutionProviderInterface implements VeeamBackupExecutionProviderInterface {

	Plugin plugin
	MorpheusContext morpheus
	ApiService apiService
	VeeamBackupTypeProvider backupTypeProvider

	VeeamHypervBackupExecutionProviderInterface(Plugin plugin, MorpheusContext morpheus, VeeamBackupTypeProvider backupTypeProvider, ApiService apiService) {
		this.plugin = plugin
		this.morpheus = morpheus
		this.apiService = apiService
		this.backupTypeProvider = backupTypeProvider
	}


}
