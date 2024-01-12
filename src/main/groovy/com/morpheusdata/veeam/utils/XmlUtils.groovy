package com.morpheusdata.veeam.utils

import java.util.regex.Matcher
import java.util.regex.Pattern

class XmlUtils {

	// XML Utils
	static xmlToMap(String xml, Boolean camelCase = false) {
		def rtn = xml ? xmlToMap(new groovy.util.XmlSlurper().parseText(xml), camelCase) : [:]
	}

	static xmlToMap(groovy.util.slurpersupport.NodeChild node, Boolean camelCase = false) {
		def rtn = [:]
		def children = node?.children()
		def attributeMap = node?.attributes()
		if(children) {
			children.each { child ->
				//node name
				def childName = child.name()
				if(camelCase == true)
					childName = getCamelKeyName(childName)
				//get value
				def childAttributeMap = child.attributes()
				if(child.childNodes()) {
					def childResult = xmlToMap(child, camelCase)
					setMapXmlValue(rtn, childName, childResult, null)
					//has sub stuff
				} else if(childAttributeMap?.size() > 0) {
					if(camelCase == true) {
						def cloneMap = [:]
						childAttributeMap.each { key, value ->
							def keyName = getCamelKeyName(key)
							cloneMap[keyName] = value
						}
						setMapXmlValue(rtn, childName, cloneMap, child.text())
					} else {
						setMapXmlValue(rtn, childName, childAttributeMap, child.text())
					}
				} else {
					//just plain old value
					setMapXmlValue(rtn, childName, child.text(), null)
				}
			}
		}
		//attributes
		if(attributeMap?.size() > 0) {
			if(camelCase == true) {
				def cloneMap = [:]
				attributeMap.each { key, value ->
					def keyName = getCamelKeyName(key)
					cloneMap[keyName] = value
					rtn += cloneMap
				}
			} else {
				rtn += attributeMap
			}
		}
		return rtn
	}

	static getCamelKeyName(String key) {
		def rtn
		if(key == 'UID')
			rtn = 'uid'
		else if(key == 'ID')
			rtn = 'id'
		else
			rtn = lowerCamelCase(key)
		//return
		return rtn
	}

	static setMapXmlValue(Map target, String name, Object value, Object extraValue) {
		def current = target[name]
		if(current == null) {
			target[name] = value
			if(extraValue)
				value.value = extraValue
		} else {
			if(!(current instanceof List)) {
				target[name] = []
				target[name] << current
			}
			target[name] << value
			if(extraValue)
				value.value = extraValue
		}
	}

	private static lowerCamelCase( String lowerCaseAndUnderscoredWord) {
		return camelCase(lowerCaseAndUnderscoredWord,false);
	}

	private static camelCase( String lowerCaseAndUnderscoredWord, boolean uppercaseFirstLetter) {
		if (lowerCaseAndUnderscoredWord == null) return null;
		lowerCaseAndUnderscoredWord = lowerCaseAndUnderscoredWord.trim();
		if (lowerCaseAndUnderscoredWord.length() == 0) return "";
		if (uppercaseFirstLetter) {
			String result = lowerCaseAndUnderscoredWord;
			// Change the case at the beginning at after each underscore ...
			return replaceAllWithUppercase(result, "(^|_)(.)", 2);
		}
		if (lowerCaseAndUnderscoredWord.length() < 2) return lowerCaseAndUnderscoredWord;
		return "" + Character.toLowerCase(lowerCaseAndUnderscoredWord.charAt(0)) + camelCase(lowerCaseAndUnderscoredWord, true).substring(1);
	}

	private static String replaceAllWithUppercase( String input, String regex, int groupNumberToUppercase ) {
		Pattern underscoreAndDotPattern = Pattern.compile(regex);
		Matcher matcher = underscoreAndDotPattern.matcher(input);
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			matcher.appendReplacement(sb, matcher.group(groupNumberToUppercase).toUpperCase());
		}
		matcher.appendTail(sb);
		return sb.toString();
	}
}
