package com.morpheusdata.veeam.sync

import com.morpheusdata.core.BulkCreateResult
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupJob
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.veeam.VeeamPlugin
import com.morpheusdata.veeam.services.ApiService
import com.morpheusdata.model.projection.BackupJobIdentityProjection
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single

@Slf4j
class BackupJobSync {
	private VeeamPlugin plugin
	private MorpheusContext morpheusContext
	private BackupProvider backupProviderModel
	private ApiService apiService

	public BackupJobSync(BackupProvider backupProviderModel, ApiService apiService, VeeamPlugin plugin) {
		this.backupProviderModel = backupProviderModel
		this.apiService = apiService
		this.plugin = plugin
		this.morpheusContext = plugin.morpheusContext
	}

	def execute() {
		try {
			log.debug("BackupJobSync execute")
			Map authConfig = apiService.getAuthConfig(backupProviderModel)
			def listResults = apiService.listBackupJobs(authConfig)
			if(listResults.success) {
				ArrayList<Map> cloudItems = listResults.jobs
				Observable<BackupJobIdentityProjection> existingItems = morpheusContext.async.backupJob.listIdentityProjections(backupProviderModel)
				SyncTask<BackupJobIdentityProjection, ArrayList<Map>, BackupJob> syncTask = new SyncTask<>(existingItems, cloudItems)
				syncTask.addMatchFunction { BackupJobIdentityProjection domainObject, Map cloudItem ->
					domainObject.externalId == cloudItem.externalId
				}.onDelete { List<BackupJobIdentityProjection> removeItems ->
					deleteBackupJobs(removeItems)
				}.onUpdate { List<SyncTask.UpdateItem<BackupJob, Map>> updateItems ->
					updateMatchedBackupJobs(updateItems)
				}.onAdd { itemsToAdd ->
					addMissingBackupJobs(itemsToAdd)
				}.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<BackupJobIdentityProjection, Map>> updateItems ->
					return morpheusContext.async.backupJob.listById(updateItems.collect { it.existingItem.id } as List<Long>)
				}.start()
			} else {
				log.error("Error listing backup jobs")
				return Single.just(false).toObservable()
			}
		} catch(Exception ex) {
			log.error("BackupJobSync error: {}", ex, ex)
		}
	}

	private addMissingBackupJobs(itemsToAdd) {
		log.debug "addMissingBackupJobs: ${itemsToAdd}"

		def adds = []
		def objCategory = "veeam.job.${backupProviderModel.id}"
		for(cloudItem in itemsToAdd) {
			println "BOBW : BackupJobSync.groovy:65 : cloudItem : ${cloudItem}"
			def addConfig = [account       : backupProviderModel.account, backupProvider: backupProviderModel, code: objCategory + '.' + cloudItem.uid,
			                 category      : objCategory, name: cloudItem.name, externalId: cloudItem.externalId,
			                 source        : 'veeam', enabled: (cloudItem.scheduleEnabled == 'true'), platform: (cloudItem.platform?.toLowerCase() ?: 'all'),
			                 cronExpression: cloudItem.scheduleCron
			]
			//backup server
			def backupServerRef = cloudItem.links?.link?.find { it.type == 'BackupServerReference' }
			if (backupServerRef)
				addConfig.internalId = ApiService.extractVeeamUuid(backupServerRef.href)
			def add = new BackupJob(addConfig)
			add.setConfigMap(cloudItem)
			adds << add
		}

		if(adds) {
			log.debug "adding backup jobs: ${adds}"
			BulkCreateResult<BackupJob> result =  morpheusContext.async.backupJob.bulkCreate(adds).blockingGet()
			if(!result.success) {
				log.error "Error adding backup jobs: ${result.errorCode} - ${result.msg}"
			}
		}
	}

	private deleteBackupJobs(List<BackupJobIdentityProjection> removeItems) {
		log.debug "deleteBackupJobs: ${removeItems}"
		for(BackupJobIdentityProjection removeItem in removeItems) {
			log.debug "removing backup job ${removeItem.name}"
			morpheusContext.async.backupJob.remove(removeItem).blockingGet()
		}
	}

	private updateMatchedBackupJobs(List<SyncTask.UpdateItem<BackupJob, Map>> updateItems) {
		log.debug "updateMatchedBackupJobs"
		for(SyncTask.UpdateItem<BackupJob, Map> update in updateItems) {
			Map masterItem = update.masterItem
			BackupJob existingItem = update.existingItem

			Boolean doSave = false
			def jobName = masterItem.name.replace("-${existingItem.account.id}", "")
			if (existingItem.name != jobName) {
				existingItem.name = jobName
				doSave = true
			}
			if (existingItem.cronExpression != masterItem.scheduleCron) {
				existingItem.cronExpression = masterItem.scheduleCron
				doSave = true
			}
			def masterItemEnabled = masterItem.scheduleEnabled == 'true'
			if (existingItem.enabled != masterItemEnabled) {
				existingItem.enabled = masterItemEnabled
			}
			if (doSave == true) {
				log.debug "updating backup job!! ${existingItem.name}"
				morpheusContext.async.backupJob.save(existingItem).blockingGet()
			}
		}
	}
}