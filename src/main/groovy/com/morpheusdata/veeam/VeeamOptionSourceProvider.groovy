package com.morpheusdata.veeam

import com.morpheusdata.core.AbstractOptionSourceProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.Cloud
import groovy.util.logging.Slf4j

@Slf4j
class VeeamOptionSourceProvider extends AbstractOptionSourceProvider {

	VeeamPlugin plugin
	MorpheusContext morpheusContext

	VeeamOptionSourceProvider(VeeamPlugin plugin, MorpheusContext context) {
		this.plugin = plugin
		this.morpheusContext = context
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
		return new ArrayList<String>(['rubrikSlaDomains'])
	}

	def rubrikSlaDomains(args) {
		args = args instanceof Object[] ? args.getAt(0) : args
		log.debug("plugin rubrikSlaDomains args: ${args}")
		def rtn = []
		Cloud cloud
		BackupProvider backupProvider
		// if(args.containerId && !args.zoneId) {
		// 		zone = Container.where{ account == tmpAccount && id == args.containerId.toLong() }.get()?.server?.zone
		// }
		if(!cloud && args.zoneId) {

			cloud = morpheus.cloud.getCloudById(Long.parseLong(args.zoneId)).blockingGet()
			log.debug("cloud: $cloud")
		}
		if(cloud && cloud.backupProvider) {
			backupProvider = morpheusContext.backupProvider.listById([cloud.backupProvider.id]).toList().blockingGet().getAt(0)
		}
		log.debug("Plugin Backup provider: ${backupProvider}")
		if(backupProvider) {
			List<com.morpheusdata.model.ReferenceData> results = morpheusContext.referenceData.listByCategory("${backupProvider?.type?.code}.backup.slaDomain.${backupProvider?.id}").toList().blockingGet()
			if(results.size() > 0) {
				results.each { policy ->
					rtn << [name: policy.name, id: policy.id, value: policy.id]
				}
			} else {
				rtn << [name: "No SLA Domains found for Rubrik", id:'']
			}
		} else {
			rtn << [name: "No Rubrik backup provider found.", id:'']
		}
		return rtn
	}

}
