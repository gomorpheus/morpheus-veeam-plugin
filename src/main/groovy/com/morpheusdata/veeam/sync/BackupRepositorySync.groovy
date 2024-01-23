package com.morpheusdata.veeam.sync

import com.morpheusdata.core.BulkCreateResult
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.BackupJob
import com.morpheusdata.model.BackupRepository
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.projection.BackupJobIdentityProjection
import com.morpheusdata.model.projection.BackupRepositoryIdentityProjection
import com.morpheusdata.veeam.VeeamBackupProvider
import com.morpheusdata.veeam.VeeamPlugin
import com.morpheusdata.veeam.services.ApiService
import com.morpheusdata.veeam.utils.VeeamUtils
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single

@Slf4j
class BackupRepositorySync {
	private VeeamPlugin plugin
	private MorpheusContext morpheusContext
	private BackupProvider backupProviderModel
	private ApiService apiService

	public BackupRepositorySync(BackupProvider backupProviderModel, ApiService apiService, VeeamPlugin plugin) {
		this.backupProviderModel = backupProviderModel
		this.apiService = apiService
		this.plugin = plugin
		this.morpheusContext = plugin.morpheusContext
	}

	def execute() {
		try {
			log.debug("BackupRepositorySync execute")
			Map authConfig = apiService.getAuthConfig(backupProviderModel)
			def listResults = apiService.getBackupRepositories(authConfig)
			if(listResults.success) {
				ArrayList<Map> cloudItems = listResults.repositories
				Observable<BackupRepositoryIdentityProjection> existingItems = morpheusContext.async.backupRepository.listIdentityProjections(backupProviderModel)
				SyncTask<BackupRepositoryIdentityProjection, ArrayList<Map>, BackupRepository> syncTask = new SyncTask<>(existingItems, cloudItems)
				syncTask.addMatchFunction { BackupRepositoryIdentityProjection domainObject, Map cloudItem ->
					domainObject.externalId == cloudItem.externalId
				}.onDelete { List<BackupRepositoryIdentityProjection> removeItems ->
					deleteBackupRepositories(removeItems)
				}.onUpdate { List<SyncTask.UpdateItem<BackupRepository, Map>> updateItems ->
					updateMatchedBackupRepositories(updateItems)
				}.onAdd { itemsToAdd ->
					addMissingBackupRepositories(itemsToAdd)
				}.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<BackupRepositoryIdentityProjection, Map>> updateItems ->
					return morpheusContext.async.backupRepository.listById(updateItems.collect { it.existingItem.id } as List<Long>)
				}.start()
			} else {
				log.error("Error listing backup repositories")
				return Single.just(false).toObservable()
			}
		} catch(Exception ex) {
			log.error("BackupRepositorySync error: {}", ex, ex)
		}
	}

	private addMissingBackupRepositories(itemsToAdd) {
		log.debug "addMissingBackupRepositories: ${itemsToAdd}"

		def adds = []
		for(cloudItem in itemsToAdd) {
			def objCategory = "veeam.repository.${backupProviderModel.id}"
			def addConfig = [account   : backupProviderModel.account, backupProvider: backupProviderModel, code: objCategory + '.' + cloudItem.externalId,
			                 category  : objCategory, name: cloudItem.name, enabled: true, externalId: cloudItem.externalId,
			                 internalId: VeeamUtils.extractVeeamUuid(cloudItem.href)
			]
			//set platform
			if (cloudItem.kind?.indexOf('indows') > -1)
				addConfig.platform = 'windows'
			//enabled?
			//capacity
			def maxStorage = cloudItem.capacity?.toLong()
			def freeStorage = cloudItem.freeSpace?.toLong()
			if (maxStorage) {
				addConfig.maxStorage = maxStorage
				if (freeStorage)
					addConfig.usedStorage = maxStorage - freeStorage
			}
			def add = new BackupRepository(addConfig)
			add.setConfigMap(cloudItem)
			adds << add
		}

		if(adds) {
			log.debug "adding backup repositories: ${adds}"
			BulkCreateResult<BackupRepository> result =  morpheusContext.async.backupRepository.bulkCreate(adds).blockingGet()
			if(!result.success) {
				log.error "Error adding backup repositories: ${result.errorCode} - ${result.msg}"
			}
		}
	}

	private deleteBackupRepositories(List<BackupRepositoryIdentityProjection> removeItems) {
		log.debug "deleteBackupRepositories: ${removeItems}"
		for(BackupRepositoryIdentityProjection removeItem in removeItems) {
			log.debug "removing backup repository ${removeItem.name}"
			morpheusContext.async.backupRepository.remove(removeItem).blockingGet()
		}
	}

	private updateMatchedBackupRepositories(List<SyncTask.UpdateItem<BackupRepository, Map>> updateItems) {
		log.debug "updateMatchedBackupRepositories"
		for(SyncTask.UpdateItem<BackupRepository, Map> update in updateItems) {
			Map masterItem = update.masterItem
			BackupRepository existingItem = update.existingItem

			def doSave = false
			if(existingItem.name != masterItem.name) {
				existingItem.name = masterItem.name
				doSave = true
			}
			//capacity
			def maxStorage = masterItem.capacity?.toLong()
			def freeStorage = masterItem.freeSpace?.toLong()
			if(maxStorage && existingItem.maxStorage != maxStorage) {
				existingItem.maxStorage = maxStorage
				doSave = true
			}
			if(maxStorage && freeStorage) {
				def usedStorage = maxStorage - freeStorage
				if(existingItem.usedStorage != usedStorage) {
					existingItem.usedStorage = usedStorage
					doSave = true
				}
			}
			def rawConfig = groovy.json.JsonOutput.toJson(masterItem).toString()
			if(rawConfig != existingItem.config) {
				existingItem.setConfigMap(masterItem)
				doSave = true
			}
			//save if needed
			if (doSave == true) {
				log.debug "updating backup repository!! ${existingItem.name}"
				morpheusContext.async.backupRepository.save(existingItem).blockingGet()
			}
		}
	}
}