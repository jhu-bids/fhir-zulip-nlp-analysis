"""FHIR Zulip NLP Analysis"""
try:
	from fhir_zulip_nlp.fhir_zulip_nlp import run
except ModuleNotFoundError:
	from fhir_zulip_nlp import run


run()
