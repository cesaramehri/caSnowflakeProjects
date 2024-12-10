CREATE OR REPLACE SECRET git_secret
    TYPE = password
    USERNAME = 'github username' --github username
    PASSWORD = 'PAT generated'; -- PAT generated

CREATE OR REPLACE API INTEGRATION git_api_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/')
    ALLOWED_AUTHENTICATION_SECRETS = (git_secret)
    ENABLED = TRUE;
