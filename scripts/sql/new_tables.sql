REATE TABLE mongo_issue_comments (
	repo character varying(255), owner character varying(255), issue_id INTEGER, comment_id TEXT,
	updated_at timestamp without time zone,
	gdpr BOOLEAN, rgpd BOOLEAN,
	dsgvo BOOLEAN, ccpa BOOLEAN,
	cpra BOOLEAN, privacy BOOLEAN,
	data_protection BOOLEAN, compliance BOOLEAN,
	legal BOOLEAN, consent BOOLEAN,
	law BOOLEAN, statute BOOLEAN,
	personal_data BOOLEAN,
	comply BOOLEAN, hipaa BOOLEAN,
	fcra BOOLEAN, ferpa BOOLEAN,
	glba BOOLEAN, ecpa BOOLEAN,
	coppa BOOLEAN, vppa BOOLEAN
) WITHOUT OIDS;

CREATE TABLE mongo_commits (
	"sha" character varying(40),
	message_flag BOOLEAN, filename_flag BOOLEAN,
	gdpr_added BOOLEAN, gdpr_removed BOOLEAN,
	rgpd_added BOOLEAN, rgpd_removed BOOLEAN,
	dsgvo_added BOOLEAN, dsgvo_removed BOOLEAN,
	ccpa_added BOOLEAN, ccpa_removed BOOLEAN,
	cpra_added BOOLEAN, cpra_removed BOOLEAN,
	privacy_added BOOLEAN, privacy_removed BOOLEAN,
	data_protection_added BOOLEAN, data_protection_removed BOOLEAN,
	compliance_added BOOLEAN, compliance_removed BOOLEAN,
	legal_added BOOLEAN, legal_removed BOOLEAN,
	consent_added BOOLEAN, consent_removed BOOLEAN,
	law_added BOOLEAN, law_removed BOOLEAN,
	statute_added BOOLEAN, statute_removed BOOLEAN,
	personal_data_added BOOLEAN, personal_data_removed BOOLEAN,
	comply_added BOOLEAN, comply_removed BOOLEAN,
	hipaa_added BOOLEAN, hipaa_removed BOOLEAN,
	fcra_added BOOLEAN, fcra_removed BOOLEAN,
	ferpa_added BOOLEAN, ferpa_removed BOOLEAN,
	glba_added BOOLEAN, glba_removed BOOLEAN,
	ecpa_added BOOLEAN, ecpa_removed BOOLEAN,
	coppa_added BOOLEAN, coppa_removed BOOLEAN,
	vppa_added BOOLEAN, vppa_removed BOOLEAN
) WITHOUT OIDS;

CREATE TABLE mongo_issues (
	repo character varying(255), owner character varying(255), issue_id INTEGER, open BOOLEAN,
	updated_at timestamp without time zone, closed_at timestamp without time zone,
	closed_by character varying(255), title_flag BOOLEAN,
	gdpr BOOLEAN, rgpd BOOLEAN,
	dsgvo BOOLEAN, ccpa BOOLEAN,
	cpra BOOLEAN, privacy BOOLEAN,
	data_protection BOOLEAN, compliance BOOLEAN,
	legal BOOLEAN, consent BOOLEAN,
	law BOOLEAN, statute BOOLEAN,
	personal_data BOOLEAN,
	comply BOOLEAN, hipaa BOOLEAN,
	fcra BOOLEAN, ferpa BOOLEAN,
	glba BOOLEAN, ecpa BOOLEAN,
	coppa BOOLEAN, vppa BOOLEAN
) WITHOUT OIDS;

CREATE TABLE mongo_pull_requests (
	repo character varying(255), owner character varying(255), pullreq_id INTEGER,
	merged_at timestamp without time zone, merged BOOLEAN,
	merged_by character varying(255)
) WITHOUT OIDS;

CREATE TABLE mongo_projects (
	name character varying(255), owner character varying(255),
	private BOOLEAN, pushed_at timestamp without time zone,
	has_homepage BOOLEAN, has_wiki BOOLEAN, has_pages BOOLEAN
) WITHOUT OIDS;
