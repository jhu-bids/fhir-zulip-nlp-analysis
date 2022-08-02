"""FHIR Zulip NLP Analysis"""
try:
	from fhir_zulip_nlp.fhir_zulip_nlp import cli
except (ModuleNotFoundError, ImportError):
	from fhir_zulip_nlp import cli


cli()
