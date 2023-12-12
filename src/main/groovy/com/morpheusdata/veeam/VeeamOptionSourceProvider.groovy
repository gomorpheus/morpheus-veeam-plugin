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
		return new ArrayList<String>([])
	}

}
