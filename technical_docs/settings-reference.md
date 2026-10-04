# Settings Reference

**Generated — never hand-edited.**

## `varco_casbin.config.CasbinSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_CASBIN_ADAPTER` | `adapter` | `typing.Literal['memory', 'file', 'sqlalchemy', 'beanie']` | `'memory'` | False |
| `VARCO_CASBIN_ADMIN_ROLE` | `admin_role` | `<class 'str'>` | `'admin'` | False |
| `VARCO_CASBIN_AUTO_SAVE` | `auto_save` | `<class 'bool'>` | `True` | False |
| `VARCO_CASBIN_DB_NAME` | `db_name` | `str | None` | `None` | False |
| `VARCO_CASBIN_DB_URL` | `db_url` | `str | None` | `None` | False |
| `VARCO_CASBIN_MODEL_PATH` | `model_path` | `str | None` | `None` | False |
| `VARCO_CASBIN_MODEL_PRESET` | `model_preset` | `<class 'str'>` | `'rbac'` | False |
| `VARCO_CASBIN_MODEL_TEXT` | `model_text` | `str | None` | `None` | False |
| `VARCO_CASBIN_POLICY_PATH` | `policy_path` | `str | None` | `None` | False |

## `varco_core.authority.config.AuthorizationConfig`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_AUTHORIZATION__ISSUERS` | `issuers` | `tuple[varco_core.authority.config.IssuerConfig, ...]` | `()` | False |

## `varco_core.cache.config.CacheSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_CACHE_DEFAULT_TTL` | `default_ttl` | `float | None` | `None` | False |

## `varco_core.config.VarcoSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|

## `varco_core.connection.base.ConnectionSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `HOST` | `host` | `<class 'str'>` | `'localhost'` | False |
| `PORT` | `port` | `<class 'int'>` | `0` | False |
| `SSL` | `ssl` | `varco_core.connection.ssl.SSLConfig | None` | `None` | False |

## `varco_core.di.report.DiagnoseSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_DIAGNOSE_STARTUP_REPORT` | `startup_report` | `<class 'bool'>` | `True` | False |
| `VARCO_DIAGNOSE_STARTUP_REPORT_JSON` | `startup_report_json` | `<class 'bool'>` | `False` | False |

## `varco_core.event.cloudevents.CloudEventsSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_CLOUDEVENTS_DATACONTENTTYPE` | `datacontenttype` | `<class 'str'>` | `'application/json'` | False |
| `VARCO_CLOUDEVENTS_SOURCE` | `source` | `<class 'str'>` | `<required>` | False |

## `varco_core.event.config.EventBusSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `CHANNEL_PREFIX` | `channel_prefix` | `<class 'str'>` | `''` | False |

## `varco_core.exception.settings.ErrorEnvelopeSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_ERROR_INCLUDE_DETAIL` | `include_detail` | `<class 'bool'>` | `False` | False |
| `VARCO_ERROR_INCLUDE_MESSAGE_KEY` | `include_message_key` | `<class 'bool'>` | `True` | False |
| `VARCO_ERROR_INCLUDE_PARAMS` | `include_params` | `<class 'bool'>` | `True` | False |
| `VARCO_ERROR_PROBLEM_DETAILS` | `problem_details` | `<class 'bool'>` | `False` | False |
| `VARCO_ERROR_PROBLEM_TYPE_BASE` | `problem_type_base` | `str | None` | `None` | False |
| `VARCO_ERROR_REDACT_PARAMS` | `redact_params` | `<class 'bool'>` | `True` | False |

## `varco_core.i18n.settings.I18nSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_I18N_CATALOG_DIR` | `catalog_dir` | `str | None` | `None` | False |
| `VARCO_I18N_DEFAULT_LOCALE` | `default_locale` | `<class 'str'>` | `'en'` | False |
| `VARCO_I18N_DOMAIN` | `domain` | `<class 'str'>` | `'messages'` | False |
| `VARCO_I18N_ENABLED` | `enabled` | `<class 'bool'>` | `False` | False |
| `VARCO_I18N_QUERY_PARAM` | `query_param` | `<class 'str'>` | `'lang'` | False |
| `VARCO_I18N_SET_CONTENT_LANGUAGE` | `set_content_language` | `<class 'bool'>` | `True` | False |
| `VARCO_I18N_SUPPORTED_LOCALES` | `supported_locales` | `tuple[str, ...]` | `('en',)` | False |

## `varco_core.idempotency.settings.IdempotencySettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_IDEMPOTENCY_ENABLED` | `enabled` | `<class 'bool'>` | `False` | False |
| `VARCO_IDEMPOTENCY_MAX_KEY_LENGTH` | `max_key_length` | `<class 'int'>` | `255` | False |
| `VARCO_IDEMPOTENCY_MAX_STORED_BODY_BYTES` | `max_stored_body_bytes` | `<class 'int'>` | `1048576` | False |
| `VARCO_IDEMPOTENCY_REPLAY_HEADER_ALLOWLIST` | `replay_header_allowlist` | `tuple[str, ...]` | `()` | False |
| `VARCO_IDEMPOTENCY_REQUIRE_KEY` | `require_key` | `<class 'bool'>` | `False` | False |
| `VARCO_IDEMPOTENCY_TTL_SECONDS` | `ttl_seconds` | `<class 'float'>` | `86400.0` | False |

## `varco_core.jwt.config.JwtVerificationSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_JWT_ALLOW_ANY_AUDIENCE` | `allow_any_audience` | `<class 'bool'>` | `False` | False |
| `VARCO_JWT_AUDIENCE` | `audience` | `str | None` | `None` | False |
| `VARCO_JWT_ENFORCE_ISS` | `enforce_issuer` | `<class 'bool'>` | `True` | False |
| `VARCO_JWT_JWKS_MIN_REFRESH_SECONDS` | `jwks_min_refresh_seconds` | `<class 'float'>` | `10.0` | False |
| `VARCO_JWT_JWKS_TTL_SECONDS` | `jwks_ttl_seconds` | `<class 'float'>` | `0.0` | False |
| `VARCO_JWT_LEEWAY_SECONDS` | `leeway_seconds` | `<class 'float'>` | `0.0` | False |
| `VARCO_JWT_REVOCATION_ENABLED` | `revocation_enabled` | `<class 'bool'>` | `True` | False |
| `VARCO_JWT_REVOCATION_FAILURE_MODE` | `revocation_failure_mode` | `<enum 'RevocationFailureMode'>` | `<RevocationFailureMode.FAIL_CLOSED: 'fail_closed'>` | False |
| `VARCO_JWT_REVOCATION_REQUIRE_JTI` | `revocation_require_jti` | `bool | None` | `None` | False |
| `VARCO_JWT_REVOCATION_SKEW_SECONDS` | `revocation_skew_seconds` | `<class 'float'>` | `60.0` | False |

## `varco_core.jwt.transform.config.JwtTransformSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_JWT_TRANSFORM_ACTOR_FIELD` | `actor_field` | `str | None` | `None` | False |
| `VARCO_JWT_TRANSFORM_ACTOR_REQUIRED` | `actor_required` | `<class 'bool'>` | `False` | False |
| `VARCO_JWT_TRANSFORM_ACTOR_SHAPE` | `actor_shape` | `<class 'str'>` | `'auto'` | False |
| `VARCO_JWT_TRANSFORM_ACTOR_STRIP_PREFIX` | `actor_strip_prefix` | `str | None` | `None` | False |
| `VARCO_JWT_TRANSFORM_GRANTS_FIELD` | `grants_field` | `str | None` | `None` | False |
| `VARCO_JWT_TRANSFORM_GRANTS_REQUIRED` | `grants_required` | `<class 'bool'>` | `False` | False |
| `VARCO_JWT_TRANSFORM_GRANTS_SHAPE` | `grants_shape` | `<class 'str'>` | `'auto'` | False |
| `VARCO_JWT_TRANSFORM_GRANTS_STRIP_PREFIX` | `grants_strip_prefix` | `str | None` | `None` | False |
| `VARCO_JWT_TRANSFORM_MERGE_SOURCES` | `merge_sources` | `<class 'bool'>` | `False` | False |
| `VARCO_JWT_TRANSFORM_METADATA_FIELDS` | `metadata_fields` | `str | None` | `None` | False |
| `VARCO_JWT_TRANSFORM_PATH_SEPARATOR` | `path_separator` | `<class 'str'>` | `'.'` | False |
| `VARCO_JWT_TRANSFORM_ROLES_FIELD` | `roles_field` | `str | None` | `None` | False |
| `VARCO_JWT_TRANSFORM_ROLES_REQUIRED` | `roles_required` | `<class 'bool'>` | `False` | False |
| `VARCO_JWT_TRANSFORM_ROLES_SHAPE` | `roles_shape` | `<class 'str'>` | `'auto'` | False |
| `VARCO_JWT_TRANSFORM_ROLES_STRIP_PREFIX` | `roles_strip_prefix` | `str | None` | `None` | False |
| `VARCO_JWT_TRANSFORM_SCOPES_FIELD` | `scopes_field` | `str | None` | `None` | False |
| `VARCO_JWT_TRANSFORM_SCOPES_REQUIRED` | `scopes_required` | `<class 'bool'>` | `False` | False |
| `VARCO_JWT_TRANSFORM_SCOPES_SHAPE` | `scopes_shape` | `<class 'str'>` | `'auto'` | False |
| `VARCO_JWT_TRANSFORM_SCOPES_STRIP_PREFIX` | `scopes_strip_prefix` | `str | None` | `None` | False |
| `VARCO_JWT_TRANSFORM_STRICT` | `strict` | `<class 'bool'>` | `False` | False |
| `VARCO_JWT_TRANSFORM_TENANT_FIELD` | `tenant_field` | `str | None` | `None` | False |
| `VARCO_JWT_TRANSFORM_TENANT_REQUIRED` | `tenant_required` | `<class 'bool'>` | `False` | False |
| `VARCO_JWT_TRANSFORM_TENANT_SHAPE` | `tenant_shape` | `<class 'str'>` | `'auto'` | False |
| `VARCO_JWT_TRANSFORM_TENANT_STRIP_PREFIX` | `tenant_strip_prefix` | `str | None` | `None` | False |
| `VARCO_JWT_TRANSFORM_TENANTS_FIELD` | `tenants_field` | `str | None` | `None` | False |
| `VARCO_JWT_TRANSFORM_TENANTS_REQUIRED` | `tenants_required` | `<class 'bool'>` | `False` | False |
| `VARCO_JWT_TRANSFORM_TENANTS_SHAPE` | `tenants_shape` | `<class 'str'>` | `'auto'` | False |
| `VARCO_JWT_TRANSFORM_TENANTS_STRIP_PREFIX` | `tenants_strip_prefix` | `str | None` | `None` | False |
| `VARCO_JWT_TRANSFORM_TOKEN_TYPE_FIELD` | `token_type_field` | `str | None` | `***` | True |
| `VARCO_JWT_TRANSFORM_TOKEN_TYPE_REQUIRED` | `token_type_required` | `<class 'bool'>` | `***` | True |
| `VARCO_JWT_TRANSFORM_TOKEN_TYPE_SHAPE` | `token_type_shape` | `<class 'str'>` | `***` | True |
| `VARCO_JWT_TRANSFORM_TOKEN_TYPE_STRIP_PREFIX` | `token_type_strip_prefix` | `str | None` | `***` | True |
| `VARCO_JWT_TRANSFORM_USER_ID_FIELD` | `user_id_field` | `str | None` | `None` | False |
| `VARCO_JWT_TRANSFORM_USER_ID_REQUIRED` | `user_id_required` | `<class 'bool'>` | `False` | False |
| `VARCO_JWT_TRANSFORM_USER_ID_SHAPE` | `user_id_shape` | `<class 'str'>` | `'auto'` | False |
| `VARCO_JWT_TRANSFORM_USER_ID_STRIP_PREFIX` | `user_id_strip_prefix` | `str | None` | `None` | False |

## `varco_core.migration.settings.MigrationSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_MIGRATE_DRY_RUN` | `dry_run` | `<class 'bool'>` | `False` | False |
| `VARCO_MIGRATE_LOCK_KEY` | `lock_key` | `<class 'str'>` | `'varco:migrate'` | False |
| `VARCO_MIGRATE_LOCK_TIMEOUT` | `lock_timeout` | `<class 'float'>` | `30.0` | False |
| `VARCO_MIGRATE_MODE` | `mode` | `typing.Literal['off', 'check', 'upgrade']` | `'off'` | False |
| `VARCO_MIGRATE_ON_FAILURE` | `on_failure` | `typing.Literal['fail', 'warn']` | `'fail'` | False |
| `VARCO_MIGRATE_TARGET_REV` | `target` | `<class 'str'>` | `'heads'` | False |
| `VARCO_MIGRATE_TIMEOUT` | `timeout` | `<class 'float'>` | `300.0` | False |

## `varco_core.tenancy.membership.TenantMembershipSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_TENANT_MEMBERSHIP_CLAIM` | `claim_key` | `<class 'str'>` | `'tenants'` | False |
| `VARCO_TENANT_MEMBERSHIP_ON_MISSING` | `on_missing_claim` | `<enum 'MissingClaimPolicy'>` | `<MissingClaimPolicy.DENY: 'deny'>` | False |

## `varco_core.tenancy.settings.TenancySettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_TENANCY_ASSERT_TENANT_FILTER` | `assert_tenant_filter` | `<class 'bool'>` | `False` | False |
| `VARCO_TENANCY_CATALOG_TTL` | `catalog_ttl_s` | `<class 'float'>` | `60.0` | False |
| `VARCO_TENANCY_DB_TEMPLATE` | `db_template` | `<class 'str'>` | `'db_{tenant_id}'` | False |
| `VARCO_TENANCY_ENFORCE_RLS` | `enforce_rls` | `<class 'bool'>` | `True` | False |
| `VARCO_TENANCY_FANOUT_FRAMEWORK_TABLES` | `fanout_framework_tables` | `<class 'bool'>` | `False` | False |
| `VARCO_TENANCY_GLOBAL_DSN` | `global_dsn` | `str | None` | `None` | False |
| `VARCO_TENANCY_GLOBAL_WRITABLE` | `global_writable` | `<class 'bool'>` | `False` | False |
| `VARCO_TENANCY_IDLE_TTL` | `idle_ttl_s` | `<class 'float'>` | `300.0` | False |
| `VARCO_TENANCY_ISOLATION` | `isolation` | `<enum 'TenantIsolation'>` | `<TenantIsolation.SHARED: 'shared'>` | False |
| `VARCO_TENANCY_MAX_ENTRIES` | `max_entries` | `<class 'int'>` | `50` | False |
| `VARCO_TENANCY_NODE_ID` | `node_id` | `str | None` | `None` | False |
| `VARCO_TENANCY_RLS_REQUIRE_TENANT` | `rls_require_tenant` | `<class 'bool'>` | `False` | False |
| `VARCO_TENANCY_RLS_SET_TENANT` | `rls_set_tenant` | `<class 'bool'>` | `False` | False |
| `VARCO_TENANCY_SCHEMA_TEMPLATE` | `schema_template` | `<class 'str'>` | `'t_{tenant_id}'` | False |
| `VARCO_TENANCY_STORE_ID` | `store_id` | `str | None` | `None` | False |

## `varco_core.tenancy.settings.TenantProvenanceSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_TENANT_BASE_DOMAINS` | `base_domains` | `tuple[str, ...]` | `()` | False |
| `VARCO_TENANT_CLAIM_METADATA_KEY` | `claim_metadata_key` | `<class 'str'>` | `'tenant_id'` | False |
| `VARCO_TENANT_CROSS_CHECK` | `cross_check` | `<enum 'CrossCheckMode'>` | `<CrossCheckMode.LENIENT: 'lenient'>` | False |
| `VARCO_TENANT_FORWARDED_HOST_HEADER` | `forwarded_host_header` | `<class 'str'>` | `'X-Forwarded-Host'` | False |
| `VARCO_TENANT_LEGACY_HEADER` | `legacy_header` | `<class 'str'>` | `'X-Tenant-Id'` | False |
| `VARCO_TENANT_MIN_TRUST` | `min_trust` | `<enum 'TenantTrust'>` | `<TenantTrust.LOW: 10>` | False |
| `VARCO_TENANT_RESERVED_LABELS` | `reserved_labels` | `tuple[str, ...]` | `('www', 'api', 'app', 'admin', 'static', 'cdn')` | False |
| `VARCO_TENANT_SOURCES` | `sources` | `tuple[str, ...]` | `()` | False |
| `VARCO_TENANT_TRUST_FORWARDED_HOST` | `trust_forwarded_host` | `<class 'bool'>` | `False` | False |

## `varco_core.tls.settings.TlsSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_TLS_CA_CERT` | `ca_cert` | `pathlib.Path | None` | `None` | False |
| `VARCO_TLS_CLIENT_CERT` | `client_cert` | `pathlib.Path | None` | `None` | False |
| `VARCO_TLS_CLIENT_KEY` | `client_key` | `pathlib.Path | None` | `None` | False |
| `VARCO_TLS_TRUST_STORE_DIR` | `trust_store_dir` | `pathlib.Path | None` | `None` | False |

## `varco_core.tz.settings.TimezoneSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_TZ_DEFAULT_TIMEZONE` | `default_timezone` | `<class 'str'>` | `'UTC'` | False |
| `VARCO_TZ_ENABLED` | `enabled` | `<class 'bool'>` | `False` | False |
| `VARCO_TZ_HEADER` | `header` | `<class 'str'>` | `'X-Timezone'` | False |
| `VARCO_TZ_QUERY_PARAM` | `query_param` | `<class 'str'>` | `'tz'` | False |

## `varco_core.webhook.settings.WebhookSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_WEBHOOK_ALLOW_INSECURE_HTTP` | `allow_insecure_http` | `<class 'bool'>` | `False` | False |
| `VARCO_WEBHOOK_ALLOW_LIST` | `allow_list` | `tuple[str, ...] | None` | `None` | False |
| `VARCO_WEBHOOK_DISABLE_AFTER_FAILURES` | `disable_after_failures` | `<class 'int'>` | `20` | False |
| `VARCO_WEBHOOK_EXTRA_DENY_RANGES` | `extra_deny_ranges` | `tuple[str, ...]` | `()` | False |
| `VARCO_WEBHOOK_INBOUND_REPLAY_TTL_SECONDS` | `inbound_replay_ttl_seconds` | `<class 'float'>` | `600.0` | False |
| `VARCO_WEBHOOK_INBOUND_TOLERANCE_SECONDS` | `inbound_tolerance_seconds` | `<class 'float'>` | `300.0` | False |
| `VARCO_WEBHOOK_PERSIST_ALL_DELIVERIES` | `persist_all_deliveries` | `<class 'bool'>` | `False` | False |
| `VARCO_WEBHOOK_REQUEST_TIMEOUT_SECONDS` | `request_timeout_seconds` | `<class 'float'>` | `10.0` | False |
| `VARCO_WEBHOOK_RETRY_BASE_DELAY_SECONDS` | `retry_base_delay_seconds` | `<class 'float'>` | `0.01` | False |
| `VARCO_WEBHOOK_RETRY_MAX_ATTEMPTS` | `retry_max_attempts` | `<class 'int'>` | `8` | False |
| `VARCO_WEBHOOK_RETRY_MAX_DELAY_SECONDS` | `retry_max_delay_seconds` | `<class 'float'>` | `0.2` | False |
| `VARCO_WEBHOOK_SIGNATURE_TOLERANCE_SECONDS` | `signature_tolerance_seconds` | `<class 'float'>` | `300.0` | False |

## `varco_fastapi.client.settings.ClientProfileSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_CLIENT_OTEL_ENABLED` | `otel_enabled` | `<class 'bool'>` | `True` | False |
| `VARCO_CLIENT_RETRY_DELAY` | `retry_delay` | `<class 'float'>` | `0.5` | False |
| `VARCO_CLIENT_RETRY_MAX` | `retry_max` | `<class 'int'>` | `0` | False |
| `VARCO_CLIENT_TIMEOUT` | `timeout` | `<class 'float'>` | `30.0` | False |

## `varco_fastapi.connection.HttpConnectionSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `AUTH` | `auth` | `varco_core.connection.auth.BasicAuthConfig | varco_core.connection.auth.OAuth2Config | None` | `***` | True |
| `BASE_URL` | `base_url` | `<class 'str'>` | `''` | False |
| `HOST` | `host` | `<class 'str'>` | `'localhost'` | False |
| `PORT` | `port` | `<class 'int'>` | `443` | False |
| `SSL` | `ssl` | `varco_core.connection.ssl.SSLConfig | None` | `None` | False |
| `TIMEOUT` | `timeout` | `<class 'float'>` | `30.0` | False |

## `varco_fastapi.middleware.body_limit.BodyLimitSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_BODY_LIMIT_ENABLED` | `enabled` | `<class 'bool'>` | `True` | False |
| `VARCO_BODY_LIMIT_EXEMPT_PATHS` | `exempt_paths` | `tuple[str, ...]` | `()` | False |
| `VARCO_BODY_LIMIT_MAX_BYTES` | `max_bytes` | `<class 'int'>` | `10485760` | False |
| `VARCO_BODY_LIMIT_TRUST_CONTENT_LENGTH` | `trust_content_length` | `<class 'bool'>` | `True` | False |

## `varco_fastapi.middleware.cors.CORSConfig`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_CORS_CREDENTIALS` | `allow_credentials` | `<class 'bool'>` | `***` | True |
| `VARCO_CORS_HEADERS` | `allow_headers` | `tuple[str, ...]` | `('Authorization', 'Content-Type', 'X-Request-ID', 'X-Correlation-ID', 'X-API-Key')` | False |
| `VARCO_CORS_METHODS` | `allow_methods` | `tuple[str, ...]` | `('GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS')` | False |
| `VARCO_CORS_ALLOW_ORIGIN_REGEX` | `allow_origin_regex` | `str | None` | `None` | False |
| `VARCO_CORS_ORIGINS` | `allow_origins` | `tuple[str, ...]` | `()` | False |
| `VARCO_CORS_ALLOW_PRIVATE_NETWORK` | `allow_private_network` | `<class 'bool'>` | `False` | False |
| `VARCO_CORS_EXPOSE_HEADERS` | `expose_headers` | `tuple[str, ...]` | `('X-Total-Count', 'X-Page-Size', 'Link')` | False |
| `VARCO_CORS_MAX_AGE` | `max_age` | `<class 'int'>` | `600` | False |

## `varco_fastapi.middleware.profiling.ProfilingSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_PROFILING_ATTACH_HEADERS` | `attach_headers` | `<class 'bool'>` | `False` | False |
| `VARCO_PROFILING_ENABLED` | `enabled` | `<class 'bool'>` | `False` | False |
| `VARCO_PROFILING_MEM_THRESHOLD_MB` | `mem_threshold_mb` | `<class 'float'>` | `0.0` | False |
| `VARCO_PROFILING_SKIP_PATHS` | `skip_paths` | `frozenset[str]` | `frozenset(['/health', '/metrics', '/readyz'])` | False |
| `VARCO_PROFILING_SLOW_THRESHOLD_MS` | `slow_threshold_ms` | `<class 'float'>` | `0.0` | False |
| `VARCO_PROFILING_TOP_N` | `top_n` | `<class 'int'>` | `15` | False |
| `VARCO_PROFILING_TRACK_RSS` | `track_rss` | `<class 'bool'>` | `True` | False |

## `varco_fastapi.middleware.rate_limit.RateLimitSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_RATE_LIMIT_EMIT_DRAFT_HEADERS` | `emit_draft_headers` | `<class 'bool'>` | `False` | False |
| `VARCO_RATE_LIMIT_ENABLED` | `enabled` | `<class 'bool'>` | `True` | False |
| `VARCO_RATE_LIMIT_ERROR_LOG_INTERVAL` | `error_log_interval` | `<class 'float'>` | `60.0` | False |
| `VARCO_RATE_LIMIT_EXEMPT_PATHS` | `exempt_paths` | `tuple[str, ...]` | `()` | False |
| `VARCO_RATE_LIMIT_FAIL_OPEN` | `fail_open` | `<class 'bool'>` | `True` | False |
| `VARCO_RATE_LIMIT_TRUSTED_PROXIES` | `trusted_proxies` | `tuple[str, ...]` | `()` | False |
| `VARCO_RATE_LIMIT_TRUSTED_PROXY_HOPS` | `trusted_proxy_hops` | `<class 'int'>` | `0` | False |

## `varco_fastapi.middleware.security_headers.SecurityHeadersSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_SECURITY_HEADERS_CSP` | `csp` | `str | None` | `"default-src 'none'; frame-ancestors 'none'"` | False |
| `VARCO_SECURITY_HEADERS_ENABLED` | `enabled` | `<class 'bool'>` | `True` | False |
| `VARCO_SECURITY_HEADERS_EXCLUDE_PATHS` | `exclude_paths` | `tuple[str, ...]` | `('/docs', '/redoc', '/openapi.json')` | False |
| `VARCO_SECURITY_HEADERS_HSTS_INCLUDE_SUBDOMAINS` | `hsts_include_subdomains` | `<class 'bool'>` | `True` | False |
| `VARCO_SECURITY_HEADERS_HSTS_MAX_AGE` | `hsts_max_age` | `<class 'int'>` | `31536000` | False |
| `VARCO_SECURITY_HEADERS_PERMISSIONS_POLICY` | `permissions_policy` | `str | None` | `'geolocation=(), microphone=(), camera=()'` | False |
| `VARCO_SECURITY_HEADERS_PRESET` | `preset` | `<enum 'SecurityHeadersPreset'>` | `<SecurityHeadersPreset.BALANCED: 'balanced'>` | False |
| `VARCO_SECURITY_HEADERS_REFERRER_POLICY` | `referrer_policy` | `str | None` | `'strict-origin-when-cross-origin'` | False |
| `VARCO_SECURITY_HEADERS_TRUSTED_PROXIES` | `trusted_proxies` | `tuple[str, ...]` | `()` | False |
| `VARCO_SECURITY_HEADERS_X_CONTENT_TYPE_OPTIONS` | `x_content_type_options` | `str | None` | `'nosniff'` | False |
| `VARCO_SECURITY_HEADERS_X_FRAME_OPTIONS` | `x_frame_options` | `str | None` | `'DENY'` | False |

## `varco_fastapi.posture.SecurityPostureSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_SECURITY_POSTURE_ENABLED` | `enabled` | `<class 'bool'>` | `True` | False |
| `VARCO_SECURITY_ENFORCE` | `enforce` | `str | None` | `None` | False |
| `VARCO_SECURITY_ENV` | `environment` | `<class 'str'>` | `'production'` | False |
| `VARCO_SECURITY_SUPPRESS` | `suppress` | `<class 'str'>` | `''` | False |

## `varco_kafka.channel.KafkaChannelManagerSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `None` | `admin_kwargs` | `dict[str, typing.Any]` | `not env-readable — pass via instance=` | False |
| `VARCO_KAFKA_ADMIN_BOOTSTRAP_SERVERS` | `bootstrap_servers` | `<class 'str'>` | `'localhost:9092'` | False |
| `VARCO_KAFKA_ADMIN_TOPIC_PREFIX` | `topic_prefix` | `<class 'str'>` | `''` | False |

## `varco_kafka.config.KafkaEventBusSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_KAFKA_AUTO_OFFSET_RESET` | `auto_offset_reset` | `<class 'str'>` | `'latest'` | False |
| `VARCO_KAFKA_BOOTSTRAP_SERVERS` | `bootstrap_servers` | `<class 'str'>` | `'localhost:9092'` | False |
| `VARCO_KAFKA_CHANNEL_PREFIX` | `channel_prefix` | `<class 'str'>` | `''` | False |
| `None` | `consumer_kwargs` | `dict[str, typing.Any]` | `not env-readable — pass via instance=` | False |
| `VARCO_KAFKA_DELIVERY_SEMANTICS` | `delivery_semantics` | `<enum 'KafkaDeliverySemantics'>` | `<KafkaDeliverySemantics.AT_LEAST_ONCE: 'at_least_once'>` | False |
| `VARCO_KAFKA_ENABLE_AUTO_COMMIT` | `enable_auto_commit` | `<class 'bool'>` | `True` | False |
| `VARCO_KAFKA_GROUP_ID` | `group_id` | `<class 'str'>` | `'varco-default'` | False |
| `None` | `producer_kwargs` | `dict[str, typing.Any]` | `not env-readable — pass via instance=` | False |
| `VARCO_KAFKA_TRANSACTIONAL_ID` | `transactional_id` | `str | None` | `None` | False |

## `varco_kafka.connection.KafkaConnectionSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `KAFKA_AUTH` | `auth` | `varco_core.connection.auth.SaslConfig | varco_core.connection.auth.BasicAuthConfig | None` | `***` | True |
| `KAFKA_BOOTSTRAP_SERVERS` | `bootstrap_servers` | `<class 'str'>` | `'localhost:9092'` | False |
| `KAFKA_GROUP_ID` | `group_id` | `<class 'str'>` | `'varco-default'` | False |
| `KAFKA_HOST` | `host` | `<class 'str'>` | `'localhost'` | False |
| `KAFKA_PORT` | `port` | `<class 'int'>` | `9092` | False |
| `KAFKA_SSL` | `ssl` | `varco_core.connection.ssl.SSLConfig | None` | `None` | False |

## `varco_memcached.cache.MemcachedCacheSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_MEMCACHED_CACHE_DEFAULT_TTL` | `default_ttl` | `float | None` | `None` | False |
| `VARCO_MEMCACHED_CACHE_HOST` | `host` | `<class 'str'>` | `'localhost'` | False |
| `VARCO_MEMCACHED_CACHE_KEY_PREFIX` | `key_prefix` | `<class 'str'>` | `''` | False |
| `VARCO_MEMCACHED_CACHE_POOL_SIZE` | `pool_size` | `<class 'int'>` | `2` | False |
| `VARCO_MEMCACHED_CACHE_PORT` | `port` | `<class 'int'>` | `11211` | False |

## `varco_nats.channel.NatsChannelManagerSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_NATS_ADMIN_CHANNEL_PREFIX` | `channel_prefix` | `<class 'str'>` | `''` | False |
| `None` | `connect_kwargs` | `dict[str, typing.Any]` | `not env-readable — pass via instance=` | False |
| `VARCO_NATS_ADMIN_DUPLICATE_WINDOW_SECONDS` | `duplicate_window_seconds` | `<class 'float'>` | `120.0` | False |
| `VARCO_NATS_ADMIN_SERVERS` | `servers` | `<class 'str'>` | `'nats://localhost:4222'` | False |
| `VARCO_NATS_ADMIN_STREAM_NAME` | `stream_name` | `<class 'str'>` | `'varco-events'` | False |
| `VARCO_NATS_ADMIN_SUBJECT_PREFIX` | `subject_prefix` | `<class 'str'>` | `'varco'` | False |

## `varco_nats.config.NatsEventBusSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_NATS_ACK_WAIT_SECONDS` | `ack_wait_seconds` | `<class 'float'>` | `30.0` | False |
| `VARCO_NATS_AUTO_CREATE_STREAM` | `auto_create_stream` | `<class 'bool'>` | `True` | False |
| `VARCO_NATS_CHANNEL_PREFIX` | `channel_prefix` | `<class 'str'>` | `''` | False |
| `None` | `connect_kwargs` | `dict[str, typing.Any]` | `not env-readable — pass via instance=` | False |
| `VARCO_NATS_DELIVERY_SEMANTICS` | `delivery_semantics` | `<enum 'NatsDeliverySemantics'>` | `<NatsDeliverySemantics.AT_LEAST_ONCE: 'at_least_once'>` | False |
| `VARCO_NATS_DUPLICATE_WINDOW_SECONDS` | `duplicate_window_seconds` | `<class 'float'>` | `120.0` | False |
| `VARCO_NATS_DURABLE_NAME` | `durable_name` | `<class 'str'>` | `'varco-default'` | False |
| `VARCO_NATS_MAX_DELIVER` | `max_deliver` | `<class 'int'>` | `5` | False |
| `VARCO_NATS_SERVERS` | `servers` | `<class 'str'>` | `'nats://localhost:4222'` | False |
| `VARCO_NATS_STREAM_NAME` | `stream_name` | `<class 'str'>` | `'varco-events'` | False |
| `VARCO_NATS_SUBJECT_PREFIX` | `subject_prefix` | `<class 'str'>` | `'varco'` | False |

## `varco_nats.connection.NatsConnectionSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `NATS_AUTH` | `auth` | `varco_core.connection.auth.BasicAuthConfig | None` | `***` | True |
| `NATS_HOST` | `host` | `<class 'str'>` | `'localhost'` | False |
| `NATS_PORT` | `port` | `<class 'int'>` | `4222` | False |
| `NATS_SERVERS` | `servers` | `<class 'str'>` | `'nats://localhost:4222'` | False |
| `NATS_SSL` | `ssl` | `varco_core.connection.ssl.SSLConfig | None` | `None` | False |
| `NATS_TOKEN` | `token` | `str | None` | `***` | True |

## `varco_redis.backplane.RedisBackplaneSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_REDIS_CACHE_BACKPLANE_CHANNEL` | `channel` | `<class 'str'>` | `'varco.cache.invalidate'` | False |
| `VARCO_REDIS_CACHE_BACKPLANE_HASH_KEYS` | `hash_keys` | `<class 'bool'>` | `False` | False |
| `None` | `redis_kwargs` | `dict[str, typing.Any]` | `not env-readable — pass via instance=` | False |
| `VARCO_REDIS_CACHE_BACKPLANE_URL` | `url` | `<class 'str'>` | `'redis://localhost:6379/0'` | False |

## `varco_redis.cache.LayeredCacheSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_LAYERED_CACHE_DECODE_RESPONSES` | `decode_responses` | `<class 'bool'>` | `False` | False |
| `VARCO_LAYERED_CACHE_DEFAULT_TTL` | `default_ttl` | `float | None` | `None` | False |
| `VARCO_LAYERED_CACHE_KEY_PREFIX` | `key_prefix` | `<class 'str'>` | `''` | False |
| `VARCO_LAYERED_CACHE_L1_DEFAULT_TTL` | `l1_default_ttl` | `float | None` | `None` | False |
| `VARCO_LAYERED_CACHE_L1_MAX_SIZE` | `l1_max_size` | `int | None` | `None` | False |
| `VARCO_LAYERED_CACHE_PROMOTE_TTL` | `promote_ttl` | `float | None` | `None` | False |
| `VARCO_LAYERED_CACHE_SOCKET_TIMEOUT` | `socket_timeout` | `float | None` | `None` | False |
| `VARCO_LAYERED_CACHE_URL` | `url` | `<class 'str'>` | `'redis://localhost:6379/0'` | False |
| `VARCO_LAYERED_CACHE_WRITE_MODE` | `write_mode` | `<class 'str'>` | `'write-through'` | False |

## `varco_redis.cache.RedisCacheSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_REDIS_CACHE_DECODE_RESPONSES` | `decode_responses` | `<class 'bool'>` | `False` | False |
| `VARCO_REDIS_CACHE_DEFAULT_TTL` | `default_ttl` | `float | None` | `None` | False |
| `VARCO_REDIS_CACHE_KEY_PREFIX` | `key_prefix` | `<class 'str'>` | `''` | False |
| `None` | `redis_kwargs` | `dict[str, typing.Any]` | `not env-readable — pass via instance=` | False |
| `VARCO_REDIS_CACHE_SOCKET_TIMEOUT` | `socket_timeout` | `float | None` | `None` | False |
| `VARCO_REDIS_CACHE_URL` | `url` | `<class 'str'>` | `'redis://localhost:6379/0'` | False |

## `varco_redis.config.RedisEventBusSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `VARCO_REDIS_CHANNEL_PREFIX` | `channel_prefix` | `<class 'str'>` | `''` | False |
| `VARCO_REDIS_DECODE_RESPONSES` | `decode_responses` | `<class 'bool'>` | `False` | False |
| `None` | `redis_kwargs` | `dict[str, typing.Any]` | `not env-readable — pass via instance=` | False |
| `VARCO_REDIS_SOCKET_TIMEOUT` | `socket_timeout` | `float | None` | `None` | False |
| `VARCO_REDIS_URL` | `url` | `<class 'str'>` | `'redis://localhost:6379/0'` | False |
| `VARCO_REDIS_USE_STREAMS` | `use_streams` | `<class 'bool'>` | `False` | False |

## `varco_redis.connection.RedisConnectionSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `REDIS_DB` | `db` | `<class 'int'>` | `0` | False |
| `REDIS_DECODE_RESPONSES` | `decode_responses` | `<class 'bool'>` | `False` | False |
| `REDIS_HOST` | `host` | `<class 'str'>` | `'localhost'` | False |
| `REDIS_PASSWORD` | `password` | `str | None` | `***` | True |
| `REDIS_PORT` | `port` | `<class 'int'>` | `6379` | False |
| `REDIS_SOCKET_TIMEOUT` | `socket_timeout` | `float | None` | `None` | False |
| `REDIS_SSL` | `ssl` | `varco_core.connection.ssl.SSLConfig | None` | `None` | False |
| `REDIS_USERNAME` | `username` | `str | None` | `None` | False |

## `varco_sa.connection.PostgresConnectionSettings`

| Env var | Field | Type | Default | Secret |
|---|---|---|---|---|
| `POSTGRES_AUTH` | `auth` | `varco_core.connection.auth.BasicAuthConfig | None` | `***` | True |
| `POSTGRES_DATABASE` | `database` | `<class 'str'>` | `'postgres'` | False |
| `POSTGRES_HOST` | `host` | `<class 'str'>` | `'localhost'` | False |
| `POSTGRES_MAX_OVERFLOW` | `max_overflow` | `<class 'int'>` | `10` | False |
| `POSTGRES_PASSWORD` | `password` | `<class 'str'>` | `***` | True |
| `POSTGRES_POOL_SIZE` | `pool_size` | `<class 'int'>` | `5` | False |
| `POSTGRES_POOL_TIMEOUT` | `pool_timeout` | `<class 'float'>` | `30.0` | False |
| `POSTGRES_PORT` | `port` | `<class 'int'>` | `5432` | False |
| `POSTGRES_SCHEMA_NAME` | `schema_name` | `<class 'str'>` | `'public'` | False |
| `POSTGRES_SSL` | `ssl` | `varco_core.connection.ssl.SSLConfig | None` | `None` | False |
| `POSTGRES_USERNAME` | `username` | `<class 'str'>` | `'postgres'` | False |
