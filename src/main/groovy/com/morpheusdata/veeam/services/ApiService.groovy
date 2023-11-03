package com.morpheusdata.veeam.services

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.BackupProvider
import groovy.util.logging.Slf4j

@Slf4j
class ApiService {

	static buildHeaders(Map headers, String token, Map opts=[:]) {
		def rtn = [:]
		if(token) {
			rtn.'X-RestSvcSessionId' = token
		}
		if(opts.format == 'json') {
			rtn.Accept = 'application/json'
		} else {
			rtn.Accept = 'application/xml'
		}

		return rtn + headers
	}

	Map getAuthConfig(BackupProvider backupProviderModel) {
		def rtn = [
			apiUrl: backupProviderModel.host,
			basePath: '/api',
			username: backupProviderModel.credentialData?.username ?: backupProviderModel.username,
			password: backupProviderModel.credentialData?.password ?: backupProviderModel.password
		]
		log.debug("getAuthConfig: ${rtn}")
		return rtn
	}

	def loginSession(BackupProvider backupProviderModel) {
		def authConfig = getAuthConfig(backupProviderModel)
		return loginSession(authConfig)
	}

	def loginSession(Map authConfig) {
		log.debug("loginSession: {}, {}", authConfig.apiUrl, authConfig.username)
		def rtn = [success:false]
		def response = getToken(authConfig)
		if(response.success) {
			rtn.success = true
			rtn.token = response.token
			rtn.sessionId = response.sessionId
		} else {
			rtn.errorCode = response.errorCode
			rtn.msg = response.msg
			rtn.content = response.content
			rtn.error = true
		}
		return rtn
	}

	def logoutSession(apiUrl, token, sessionId) {
		if(token && sessionId) {
			logout(apiUrl, token, sessionId)
		}
	}

	static getToken(Map authConfig) {
		def rtn = [success:false]
		def requestToken = true
		if(authConfig.token) {
			rtn.success = true
			rtn.token = authConfig.token
			rtn.sessionId = authConfig.sessionId
			requestToken = false
		}
		//if need a new one
		if(requestToken == true) {
			def apiPath = authConfig.basePath + '/sessionMngr/'
			def headers = buildHeaders([:], null)
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers)
			HttpApiClient httpApiClient = new HttpApiClient()
			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, authConfig.username, authConfig.password, requestOpts, 'POST')
			rtn.success = results?.success && results?.error != true
			if(rtn.success == true) {
				rtn.token = results.headers['X-RestSvcSessionId']
				rtn.sessionId = results.data.SessionId.toString()
				authConfig.token = rtn.token
				authConfig.sessionId = rtn.sessionId
			} else {
				rtn.content = results.content
				rtn.data = results.data
				rtn.errorCode = results.errorCode
				rtn.headers = results.headers
			}
		}
		return rtn
	}

	static logout(url, token, sessionId){
		def rtn = [success:false]
		def headers = buildHeaders([:], token)
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers)
		def results = httpApiClient.callXmlApi(url, "/api/logonSessions/${sessionId}", requestOpts, 'DELETE')
		log.debug("got: ${results}")
		rtn.success = results?.success
		return rtn
	}
}
