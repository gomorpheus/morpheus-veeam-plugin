package com.morpheusdata.veeam.sync

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.ReferenceData
import com.morpheusdata.veeam.VeeamPlugin
import com.morpheusdata.veeam.services.ApiService
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single

@Slf4j
class BackupServerSync {
	private VeeamPlugin plugin
	private MorpheusContext morpheusContext
	private BackupProvider backupProviderModel
	private ApiService apiService

	public BackupServerSync(BackupProvider backupProviderModel, ApiService apiService, VeeamPlugin plugin) {
		this.backupProviderModel = backupProviderModel
		this.apiService = apiService
		this.plugin = plugin
		this.morpheusContext = plugin.morpheusContext
	}

	def execute() {
		try {
			log.debug("BackupServerSync execute")
			Map authConfig = apiService.getAuthConfig(backupProviderModel)
			def listResults = apiService.listBackupServers(authConfig)
			if(listResults.success) {
				ArrayList<Map> cloudItems = listResults.backupServers
				def objCategory = "veeam.backup.backupServer.${backupProviderModel.id}"
				Observable<ReferenceData> existingItems = morpheusContext.async.referenceData.list(new DataQuery().withFilters([
						new DataFilter('category', objCategory),
						new DataFilter('account.id', backupProviderModel.account.id)
				]))
				SyncTask<ReferenceData, ArrayList<Map>, ReferenceData> syncTask = new SyncTask<>(existingItems, cloudItems)
				syncTask.addMatchFunction { ReferenceData domainObject, Map cloudItem ->
					domainObject.keyValue == cloudItem.externalId
				}.onDelete { List<ReferenceData> removeItems ->
					deleteBackupServer(removeItems)
				}.onUpdate { List<SyncTask.UpdateItem<ReferenceData, Map>> updateItems ->
					updateMatchedBackupServers(updateItems)
				}.onAdd { itemsToAdd ->
					addMissingBackupServers(itemsToAdd, objCategory)
				}.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<ReferenceData, Map>> updateItems ->
					return morpheusContext.async.referenceData.list( new DataQuery().withFilters([
					        new DataFilter('id', 'in', updateItems.collect { it.existingItem.id } as List<Long>)
					]))
				}.start()
			} else {
				log.error("Error listing backup servers")
				return Single.just(false).toObservable()
			}
		} catch(Exception ex) {
			log.error("BackupServerSync error: {}", ex, ex)
		}
	}

	private addMissingBackupServers(itemsToAdd, objCategory) {
		log.debug "addMissingBackupServers: ${itemsToAdd}"

		def adds = []
		for(cloudItem in itemsToAdd) {
			def addConfig = [account:backupProviderModel.account, code:objCategory + '.' + cloudItem.externalId, category:objCategory,
			                 name:cloudItem.name, keyValue:cloudItem.externalId, value:cloudItem.externalId, type: 'string']
			def add = new ReferenceData(addConfig)
			add.setConfigMap(cloudItem)

			adds << add
		}

		if(adds) {
			log.debug "adding backup servers: ${adds}"
			def success =  morpheusContext.async.referenceData.create(adds).blockingGet()
			if(!success) {
				log.error "Error adding backup servers"
			}
		}
	}

	private deleteBackupServer(List<ReferenceData> removeItems) {
		log.debug "deleteBackupServer: ${removeItems}"
		for(ReferenceData removeItem in removeItems) {
			log.debug "removing backup server ${removeItem.name}"
			morpheusContext.async.referenceData.remove(removeItem).blockingGet()
		}
	}

	private updateMatchedBackupServers(List<SyncTask.UpdateItem<ReferenceData, Map>> updateItems) {
		log.debug "updateMatchedBackupServers"
		for(SyncTask.UpdateItem<ReferenceData, Map> update in updateItems) {
			Map masterItem = update.masterItem
			ReferenceData existingItem = update.existingItem

			def doSave = false
			if(existingItem.name != masterItem.name) {
				existingItem.name = masterItem.name
				doSave = true
			}
			if(doSave == true) {
				log.debug "updating backup server!! ${existingItem.name}"
				morpheusContext.async.referenceData.save(existingItem).blockingGet()
			}
		}
	}
}