package com.morpheusdata.veeam

import com.morpheusdata.core.AbstractOptionSourceProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataAndFilter
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.model.Account
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.BackupRepository
import com.morpheusdata.model.BackupType
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.Workload
import com.morpheusdata.veeam.services.ApiService
import com.morpheusdata.veeam.utils.VeeamUtils
import groovy.util.logging.Slf4j
import com.morpheusdata.model.Permission.ResourceType as PermissionResourceType

@Slf4j
class VeeamOptionSourceProvider extends AbstractOptionSourceProvider {

	VeeamPlugin plugin
	MorpheusContext morpheusContext
	ApiService apiService

	VeeamOptionSourceProvider(VeeamPlugin plugin, MorpheusContext context, ApiService) {
		this.plugin = plugin
		this.morpheusContext = context
		this.apiService = apiService
	}

	@Override
	MorpheusContext getMorpheus() {
		return this.morpheusContext
	}

	@Override
	Plugin getPlugin() {
		return this.plugin
	}

	@Override
	String getCode() {
		return 'veeam-option-source-plugin'
	}

	@Override
	String getName() {
		return 'Veeam Option Source Plugin'
	}

	@Override
	List<String> getMethodNames() {
		return new ArrayList<String>(['veeamManagedServer', 'veeamBackupRepository'])
	}

	def veeamManagedServer(args) {
		args = args instanceof Object[] ? args.getAt(0) : args
		log.debug("veeamManagedServer: ${args}")
		List<Map> managedServers = []
		Account account = args.currentUser?.account?.id ? new Account(id: args.currentUser.account.id) : null
		log.debug("veeamManagedServer: account: ${account}")
		BackupType backupType = args?.backupTypeId ? morpheus.services.backup.type.get(args.backupTypeId.toLong()) : null
		log.debug("veeamManagedServer: backupType: ${backupType}")
		String managedServerType =  backupType ? plugin.getProviderByCode(backupType.code)?.managedServerType : null
		log.debug("veeamManagedServer: managedServerType: ${managedServerType}")
		Cloud cloud = null
		if(args.zoneId) {
			cloud = morpheus.services.cloud.get(args.zoneId.toLong())
		} else if(args.containerId) {
			Workload workload = morpheus.services.workload.find( new DataQuery(account).withFilter('id', args.containerId.toLong()).withJoins("server", "server.zone") )
			cloud = workload?.server?.zone
		}
		log.debug("veeamManagedServer: cloud: ${cloud}")

		String repoBackupServerId
		if(args.repositoryId) {
			BackupRepository repo = morpheus.services.backupRepository.get(args.repositoryId.toLong())
			Map repoConfig = repo.getConfigMap()
			String backupServerHref = repoConfig?.links?.link?.find { it.type == "BackupServerReference" }?.href
			repoBackupServerId = VeeamUtils.extractVeeamUuid(backupServerHref)
		}
		if(account) {
			BackupProvider backupProvider = cloud && cloud.backupProvider ? morpheus.services.backupProvider.find( new DataQuery(account).withFilter('id', cloud.backupProvider.id) ) : null
			if(backupProvider) {
				List<Long> accessibleResourceIds = morpheus.async.permission.listAccessibleResources(account.id, PermissionResourceType.ManagedServer, null, null).toList().blockingGet()
				DataQuery query = new DataQuery().withFilters(
					new DataFilter('account.id', backupProvider.account.id),
					new DataFilter('category',"veeam.backup.managedServer.${backupProvider.id}"),
					new DataFilter("typeValue", managedServerType),
					new DataFilter('enabled', true),
				)
				if(accessibleResourceIds.size() > 0) {
					query = query.withFilters(
						new DataOrFilter(
							new DataFilter('account.id', account.id),
							new DataAndFilter(
								new DataFilter('account.masterAccount', true),
								new DataFilter('visibility', 'public')
							),
							new DataFilter('id', 'in', accessibleResourceIds)
						)
					)
				} else {
					query = query.withFilters(
						new DataOrFilter(
							new DataFilter('account.id', account.id),
							new DataAndFilter(
								new DataFilter('account.masterAccount', true),
								new DataFilter('visibility', 'public')
							)
						)
					)
				}

				def existingManagedServers = morpheus.services.referenceData.list(query)
				if (existingManagedServers.size() > 0) {
					existingManagedServers.each { managedServer ->
						def managedServerConfig = managedServer.getConfigMap()
						if(managedServerConfig?.managedServerType == managedServerType) {
							String backupServerName = ""
							def id = null
							def backupServerId
							if(managedServerConfig.managedServerId && managedServerConfig.backupServerId) {
								id = managedServerConfig.managedServerId + ":" + managedServerConfig.backupServerId
								backupServerName = " (Backup Server: " + managedServerConfig.backupServerName + ")"
								backupServerId = managedServerConfig.backupServerId
							} else {
								def managedServerLink = managedServerConfig.links?.link?.find { it.type == "ManagedServerReference" }
								def backupServerLink = managedServerConfig.links?.link?.find { it.type == "BackupServer" }
								def managedServerId = VeeamUtils.extractVeeamUuid(managedServerLink.href)
								backupServerId = VeeamUtils.extractVeeamUuid(backupServerLink.href)
								backupServerName = " (Backup Server: " + (backupServerLink?.name ?: "N/A") + ")"
								id = "${managedServerId}:${backupServerId}"
							}
							if(!backupServerId || !repoBackupServerId || (repoBackupServerId == backupServerId)) {
								def value = managedServer.name + backupServerName
								managedServers << [name: value, id: id, value: id]
							}
						}
					}
				} else {
					managedServers << [name: "No managed servers setup in Veeam", id:'']
				}

			} else {
				managedServers << [name: "No Veeam backup provider found.", id:'']
			}
		}

		return managedServers
	}

	def veeamBackupRepository(args) {
		args = args instanceof Object[] ? args.getAt(0) : args
		def repos = []
		Account account = args.currentUser?.account?.id ? new Account(id: args.currentUser.account.id) : null
		if(account && args.zoneId) {
			Cloud cloud = morpheus.services.cloud.get(args.zoneId.toLong())
			BackupProvider backupProvider = cloud && cloud.backupProvider ? morpheus.services.backupProvider.find( new DataQuery(account).withFilter('id', cloud.backupProvider.id) ) : null
			if(backupProvider && cloud) {
				List<Long> accessibleResourceIds = morpheus.async.permission.listAccessibleResources(account.id, PermissionResourceType.BackupRepository, null, null).toList().blockingGet()
				DataQuery query = new DataQuery().withFilters(
					new DataFilter('id', backupProvider.id),
					new DataFilter('enabled', true),
				)
				if(accessibleResourceIds.size() > 0) {
					query = query.withFilters(
						new DataOrFilter(
							new DataFilter('account.id', account.id),
							new DataAndFilter(
								new DataFilter('account.masterAccount', true),
								new DataFilter('visibility', 'public')
							),
							new DataFilter('id', 'in', accessibleResourceIds)
						)
					)
				} else {
					query = query.withFilters(
						new DataOrFilter(
							new DataFilter('account.id', account.id),
							new DataAndFilter(
								new DataFilter('account.masterAccount', true),
								new DataFilter('visibility', 'public')
							)
						)
					)
				}
				List<BackupRepository> existingRepositories = morpheus.services.backupRepository.list(query)
				if(existingRepositories?.size() > 0) {
					for(backupRepository in existingRepositories) {
						Map repoConfig = backupRepository.getConfigMap()
						String backupServer = repoConfig?.links?.link?.find { it.type == "BackupServerReference" }?.name
						String repoName = backupServer ? "${backupRepository.name} (Backup Server: ${backupServer})" : backupRepository.name
						repos << [id: backupRepository.id, name: repoName, code: backupRepository.code, internalId: backupRepository.internalId, value: backupRepository.id]
					}
				} else {
					repos << [name: "No repositories setup in Veeam", id:'']
				}
			}
		}

		return repos
	}

}
