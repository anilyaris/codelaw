CREATE TABLE mongo_issue_comments_extended (
	repo character varying(255), owner character varying(255), issue_id INTEGER, comment_id TEXT,
	created_at timestamp without time zone
) WITHOUT OIDS;

CREATE TABLE mongo_commits_extended (
	"sha" character varying(40), created_at timestamp without time zone,
	message_gdpr BOOLEAN, message_rgpd BOOLEAN,
	message_dsgvo BOOLEAN, message_ccpa BOOLEAN,
	message_cpra BOOLEAN, message_privacy BOOLEAN,
	message_data_protection BOOLEAN, message_compliance BOOLEAN,
	message_legal BOOLEAN, message_consent BOOLEAN,
	message_law BOOLEAN, message_statute BOOLEAN,
	message_personal_data BOOLEAN,
	message_comply BOOLEAN, message_hipaa BOOLEAN,
	message_fcra BOOLEAN, message_ferpa BOOLEAN,
	message_glba BOOLEAN, message_ecpa BOOLEAN,
	message_coppa BOOLEAN, message_vppa BOOLEAN,
	filename_gdpr BOOLEAN, filename_rgpd BOOLEAN,
	filename_dsgvo BOOLEAN, filename_ccpa BOOLEAN,
	filename_cpra BOOLEAN, filename_privacy BOOLEAN,
	filename_data_protection BOOLEAN, filename_compliance BOOLEAN,
	filename_legal BOOLEAN, filename_consent BOOLEAN,
	filename_law BOOLEAN, filename_statute BOOLEAN,
	filename_personal_data BOOLEAN,
	filename_comply BOOLEAN, filename_hipaa BOOLEAN,
	filename_fcra BOOLEAN, filename_ferpa BOOLEAN,
	filename_glba BOOLEAN, filename_ecpa BOOLEAN,
	filename_coppa BOOLEAN, filename_vppa BOOLEAN
) WITHOUT OIDS;

CREATE TABLE mongo_issues_extended (
	repo character varying(255), owner character varying(255), issue_id INTEGER,
	created_at timestamp without time zone,
	title_gdpr BOOLEAN, title_rgpd BOOLEAN,
	title_dsgvo BOOLEAN, title_ccpa BOOLEAN,
	title_cpra BOOLEAN, title_privacy BOOLEAN,
	title_data_protection BOOLEAN, title_compliance BOOLEAN,
	title_legal BOOLEAN, title_consent BOOLEAN,
	title_law BOOLEAN, title_statute BOOLEAN,
	title_personal_data BOOLEAN,
	title_comply BOOLEAN, title_hipaa BOOLEAN,
	title_fcra BOOLEAN, title_ferpa BOOLEAN,
	title_glba BOOLEAN, title_ecpa BOOLEAN,
	title_coppa BOOLEAN, title_vppa BOOLEAN
) WITHOUT OIDS;

CREATE TABLE mongo_projects_extended (
	name character varying(255), owner character varying(255),
	created_at timestamp without time zone
) WITHOUT OIDS;
