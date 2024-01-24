package com.morpheusdata.veeam.sync

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.ReferenceData
import com.morpheusdata.veeam.VeeamBackupProvider
import com.morpheusdata.veeam.VeeamPlugin
import com.morpheusdata.veeam.services.ApiService
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.core.Single

@Slf4j
class ManagedServerSync {
	private VeeamPlugin plugin
	private MorpheusContext morpheusContext
	private BackupProvider backupProviderModel
	private ApiService apiService

	public ManagedServerSync(BackupProvider backupProviderModel, ApiService apiService, VeeamPlugin plugin) {
		this.backupProviderModel = backupProviderModel
		this.apiService = apiService
		this.plugin = plugin
		this.morpheusContext = plugin.morpheusContext
	}

	def execute() {
		try {
			log.debug("ManagedServerSync execute")
			Map authConfig = apiService.getAuthConfig(backupProviderModel)
			def listResults = apiService.listManagedServers(authConfig)
			log.debug("ManagedServerSync listResults: ${listResults}")
			if(listResults.success) {
				ArrayList<Map> cloudItems = listResults.managedServers.findAll { it.managedServerType != "Local" }
				
				// find the correlating Hierarchy Root Object, use for operations during backup/restore
				for(cloudItem in cloudItems) {
					def rootSearchResult = apiService.getManagedServerRoot(authConfig, cloudItem.name)
					if(rootSearchResult.success && rootSearchResult.data?.uid) {
						cloudItem.hierarchyRootUid = rootSearchResult.data.uid
					}
				}
				
				def objCategory = "veeam.backup.managedServer.${backupProviderModel.id}"
				Observable<ReferenceData> existingItems = morpheusContext.async.referenceData.list(new DataQuery().withFilters([
						new DataFilter('category', objCategory),
						new DataFilter('account.id', backupProviderModel.account.id)
				]))
				SyncTask<ReferenceData, ArrayList<Map>, ReferenceData> syncTask = new SyncTask<>(existingItems, cloudItems)
				syncTask.addMatchFunction { ReferenceData domainObject, Map cloudItem ->
					domainObject.keyValue == cloudItem.externalId
				}.onDelete { List<ReferenceData> removeItems ->
					deleteManagedServer(removeItems)
				}.onUpdate { List<SyncTask.UpdateItem<ReferenceData, Map>> updateItems ->
					updateMatchedManagedServers(updateItems, authConfig)
				}.onAdd { itemsToAdd ->
					addMissingManagedServers(itemsToAdd, objCategory)
				}.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<ReferenceData, Map>> updateItems ->
					return morpheusContext.async.referenceData.list( new DataQuery().withFilters([
							new DataFilter('id', 'in', updateItems.collect { it.existingItem.id } as List<Long>)
					]))
				}.start()
			} else {
				log.error("Error listing managed servers")
				return Single.just(false).toObservable()
			}
		} catch(Exception ex) {
			log.error("ManagedServerSync error: {}", ex, ex)
		}
	}

	private addMissingManagedServers(itemsToAdd, objCategory) {
		log.debug "addMissingManagedServers: ${itemsToAdd}"

		def adds = []
		for(cloudItem in itemsToAdd) {
			def addConfig = [account:backupProviderModel.account, code:objCategory + '.' + cloudItem.uid, category:objCategory,
			                 name:cloudItem.name, keyValue:cloudItem.externalId, value:cloudItem.externalId, typeValue:cloudItem.managedServerType, type: 'string']
			def add = new ReferenceData(addConfig)
			add.setConfigMap(cloudItem)
			
			adds << add
		}

		if(adds) {
			log.debug "adding managed servers: ${adds}"
			def success =  morpheusContext.async.referenceData.create(adds).blockingGet()
			if(!success) {
				log.error "Error adding managed servers"
			}
		}
	}

	private deleteManagedServer(List<ReferenceData> removeItems) {
		log.debug "deleteManagedServer: ${removeItems}"
		for(ReferenceData removeItem in removeItems) {
			log.debug "removing managed server ${removeItem.name}"
			morpheusContext.async.referenceData.remove(removeItem).blockingGet()
		}
	}

	private updateMatchedManagedServers(List<SyncTask.UpdateItem<ReferenceData, Map>> updateItems, authConfig) {
		log.debug "updateMatchedManagedServers"
		for(SyncTask.UpdateItem<ReferenceData, Map> update in updateItems) {
			Map masterItem = update.masterItem
			ReferenceData existingItem = update.existingItem

			def doSave = false
			if(existingItem.name != masterItem.name) {
				existingItem.name = masterItem.name
				doSave = true
			}
			if(existingItem.typeValue != masterItem.managedServerType) {
				existingItem.typeValue = masterItem.managedServerType
				doSave = true
			}
			if(existingItem.keyValue != masterItem.externalId) {
				existingItem.keyValue = masterItem.externalId
				existingItem.value = masterItem.externalId
				existingItem.setConfigMap(masterItem)
				doSave = true
			}

			if(!existingItem.getConfigProperty('hierarchyRootUid') || !existingItem.getConfigProperty('hierarchyRootCorrected')) {
				def rootResults = apiService.getManagedServerRoot(authConfig, masterItem.name)
				def hierarchyRoot = rootResults.data
				if(hierarchyRoot) {
					existingItem.setConfigProperty('hierarchyRootUid', hierarchyRoot.uid)
					existingItem.setConfigProperty('hierarchyRootCorrected', true)
					doSave = true
				}
			}
			def rawConfig = groovy.json.JsonOutput.toJson(masterItem).toString()
			if(rawConfig != existingItem.config) {
				existingItem.setConfigMap(masterItem)
				doSave = true
			}
			
			if(doSave == true) {
				log.debug "updating managed server!! ${existingItem.name}"
				morpheusContext.async.referenceData.save(existingItem).blockingGet()
			}
		}
	}
}