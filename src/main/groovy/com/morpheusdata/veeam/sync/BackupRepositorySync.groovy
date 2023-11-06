package com.morpheusdata.veeam.sync

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.veeam.VeeamPlugin
import groovy.util.logging.Slf4j

@Slf4j
class BackupRepositorySync {
	private VeeamPlugin plugin
	private MorpheusContext morpheusContext
	private BackupProvider backupProviderModel

	public BackupRepositorySync(BackupProvider backupProviderModel, VeeamPlugin plugin) {
		this.backupProviderModel = backupProviderModel
		this.plugin = plugin
		this.morpheusContext = plugin.morpheusContext
	}

	def execute() {
		try {

		} catch(Exception ex) {
			log.error("BackupRepositorySync error: {}", ex, ex)
		}
	}
}