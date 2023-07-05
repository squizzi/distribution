package configuration

import (
	"bytes"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v2"
)

// configStruct is a canonical example configuration, which should map to configYamlV0_1
var configStruct = Configuration{
	Version: "0.1",
	Log: struct {
		AccessLog struct {
			Disabled bool `yaml:"disabled,omitempty"`
		} `yaml:"accesslog,omitempty"`
		Level        Loglevel               `yaml:"level,omitempty"`
		Formatter    string                 `yaml:"formatter,omitempty"`
		Fields       map[string]interface{} `yaml:"fields,omitempty"`
		Hooks        []LogHook              `yaml:"hooks,omitempty"`
		ReportCaller bool                   `yaml:"reportcaller,omitempty"`
	}{
		Level:  "info",
		Fields: map[string]interface{}{"environment": "test"},
	},
	Storage: Storage{
		"somedriver": Parameters{
			"string1": "string-value1",
			"string2": "string-value2",
			"bool1":   true,
			"bool2":   false,
			"nil1":    nil,
			"int1":    42,
			"url1":    "https://foo.example.com",
			"path1":   "/some-path",
		},
	},
	Auth: Auth{
		"silly": Parameters{
			"realm":   "silly",
			"service": "silly",
		},
	},
	Reporting: Reporting{
		Bugsnag: BugsnagReporting{
			APIKey: "BugsnagApiKey",
		},
	},
	Notifications: Notifications{
		Endpoints: []Endpoint{
			{
				Name: "endpoint-1",
				URL:  "http://example.com",
				Headers: http.Header{
					"Authorization": []string{"Bearer <example>"},
				},
				IgnoredMediaTypes: []string{"application/octet-stream"},
				Ignore: Ignore{
					MediaTypes: []string{"application/octet-stream"},
					Actions:    []string{"pull"},
				},
			},
		},
	},
	Catalog: Catalog{
		MaxEntries: 1000,
	},
	HTTP: struct {
		Addr         string        `yaml:"addr,omitempty"`
		Net          string        `yaml:"net,omitempty"`
		Host         string        `yaml:"host,omitempty"`
		Prefix       string        `yaml:"prefix,omitempty"`
		Secret       string        `yaml:"secret,omitempty"`
		RelativeURLs bool          `yaml:"relativeurls,omitempty"`
		DrainTimeout time.Duration `yaml:"draintimeout,omitempty"`
		TLS          struct {
			Certificate  string   `yaml:"certificate,omitempty"`
			Key          string   `yaml:"key,omitempty"`
			ClientCAs    []string `yaml:"clientcas,omitempty"`
			MinimumTLS   string   `yaml:"minimumtls,omitempty"`
			CipherSuites []string `yaml:"ciphersuites,omitempty"`
			LetsEncrypt  struct {
				CacheFile string   `yaml:"cachefile,omitempty"`
				Email     string   `yaml:"email,omitempty"`
				Hosts     []string `yaml:"hosts,omitempty"`
			} `yaml:"letsencrypt,omitempty"`
		} `yaml:"tls,omitempty"`
		Headers http.Header `yaml:"headers,omitempty"`
		Debug   struct {
			Addr       string `yaml:"addr,omitempty"`
			Prometheus struct {
				Enabled bool   `yaml:"enabled,omitempty"`
				Path    string `yaml:"path,omitempty"`
			} `yaml:"prometheus,omitempty"`
		} `yaml:"debug,omitempty"`
		HTTP2 struct {
			Disabled bool `yaml:"disabled,omitempty"`
		} `yaml:"http2,omitempty"`
	}{
		TLS: struct {
			Certificate  string   `yaml:"certificate,omitempty"`
			Key          string   `yaml:"key,omitempty"`
			ClientCAs    []string `yaml:"clientcas,omitempty"`
			MinimumTLS   string   `yaml:"minimumtls,omitempty"`
			CipherSuites []string `yaml:"ciphersuites,omitempty"`
			LetsEncrypt  struct {
				CacheFile string   `yaml:"cachefile,omitempty"`
				Email     string   `yaml:"email,omitempty"`
				Hosts     []string `yaml:"hosts,omitempty"`
			} `yaml:"letsencrypt,omitempty"`
		}{
			ClientCAs: []string{"/path/to/ca.pem"},
		},
		Headers: http.Header{
			"X-Content-Type-Options": []string{"nosniff"},
		},
		HTTP2: struct {
			Disabled bool `yaml:"disabled,omitempty"`
		}{
			Disabled: false,
		},
	},
}

// configYamlV0_1 is a Version 0.1 yaml document representing configStruct
var configYamlV0_1 = `
version: 0.1
log:
  level: info
  fields:
    environment: test
storage:
  somedriver:
    string1: string-value1
    string2: string-value2
    bool1: true
    bool2: false
    nil1: ~
    int1: 42
    url1: "https://foo.example.com"
    path1: "/some-path"
auth:
  silly:
    realm: silly
    service: silly
notifications:
  endpoints:
    - name: endpoint-1
      url:  http://example.com
      headers:
        Authorization: [Bearer <example>]
      ignoredmediatypes:
        - application/octet-stream
      ignore:
        mediatypes:
           - application/octet-stream
        actions:
           - pull
reporting:
  bugsnag:
    apikey: BugsnagApiKey
http:
  clientcas:
    - /path/to/ca.pem
  headers:
    X-Content-Type-Options: [nosniff]
`

// inmemoryConfigYamlV0_1 is a Version 0.1 yaml document specifying an inmemory
// storage driver with no parameters
var inmemoryConfigYamlV0_1 = `
version: 0.1
log:
  level: info
storage: inmemory
auth:
  silly:
    realm: silly
    service: silly
notifications:
  endpoints:
    - name: endpoint-1
      url:  http://example.com
      headers:
        Authorization: [Bearer <example>]
      ignoredmediatypes:
        - application/octet-stream
      ignore:
        mediatypes:
           - application/octet-stream
        actions:
           - pull
http:
  headers:
    X-Content-Type-Options: [nosniff]
`

type ConfigSuite struct {
	suite.Suite
	expectedConfig *Configuration
}

func (suite *ConfigSuite) SetupTest() {
	suite.expectedConfig = copyConfig(configStruct)
}

func TestConfigSuite(t *testing.T) {
	suite.Run(t, new(ConfigSuite))
}

// TestMarshalRoundtrip validates that configStruct can be marshaled and
// unmarshaled without changing any parameters
func (suite *ConfigSuite) TestMarshalRoundtrip() {
	configBytes, err := yaml.Marshal(suite.expectedConfig)
	suite.NoError(err)
	config, err := Parse(bytes.NewReader(configBytes))
	suite.T().Log(string(configBytes))
	suite.NoError(err)
	suite.Equal(suite.expectedConfig, config)
}

// TestParseSimple validates that configYamlV0_1 can be parsed into a struct
// matching configStruct
func (suite *ConfigSuite) TestParseSimple() {
	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	suite.NoError(err)
	suite.Equal(suite.expectedConfig, config)
}

// TestParseInmemory validates that configuration yaml with storage provided as
// a string can be parsed into a Configuration struct with no storage parameters
func (suite *ConfigSuite) TestParseInmemory() {
	suite.expectedConfig.Storage = Storage{"inmemory": Parameters{}}
	suite.expectedConfig.Reporting = Reporting{}
	suite.expectedConfig.Log.Fields = nil

	config, err := Parse(bytes.NewReader([]byte(inmemoryConfigYamlV0_1)))
	suite.NoError(err)
	suite.Equal(suite.expectedConfig, config)
}

// TestParseIncomplete validates that an incomplete yaml configuration cannot
// be parsed without providing environment variables to fill in the missing
// components.
func (suite *ConfigSuite) TestParseIncomplete() {
	incompleteConfigYaml := "version: 0.1"
	_, err := Parse(bytes.NewReader([]byte(incompleteConfigYaml)))
	suite.Error(err)

	suite.expectedConfig.Log.Fields = nil
	suite.expectedConfig.Storage = Storage{"filesystem": Parameters{"rootdirectory": "/tmp/testroot"}}
	suite.expectedConfig.Auth = Auth{"silly": Parameters{"realm": "silly"}}
	suite.expectedConfig.Reporting = Reporting{}
	suite.expectedConfig.Notifications = Notifications{}
	suite.expectedConfig.HTTP.Headers = nil

	// Note: this also tests that REGISTRY_STORAGE and
	// REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY can be used together
	suite.T().Setenv("REGISTRY_STORAGE", "filesystem")
	suite.T().Setenv("REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY", "/tmp/testroot")
	suite.T().Setenv("REGISTRY_AUTH", "silly")
	suite.T().Setenv("REGISTRY_AUTH_SILLY_REALM", "silly")

	config, err := Parse(bytes.NewReader([]byte(incompleteConfigYaml)))
	suite.NoError(err)
	suite.Equal(suite.expectedConfig, config)
}

// TestParseWithSameEnvStorage validates that providing environment variables
// that match the given storage type will only include environment-defined
// parameters and remove yaml-defined parameters
func (suite *ConfigSuite) TestParseWithSameEnvStorage() {
	suite.expectedConfig.Storage = Storage{"somedriver": Parameters{"region": "us-east-1"}}

	suite.T().Setenv("REGISTRY_STORAGE", "somedriver")
	suite.T().Setenv("REGISTRY_STORAGE_SOMEDRIVER_REGION", "us-east-1")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	suite.NoError(err)
	suite.Equal(suite.expectedConfig, config)
}

// TestParseWithDifferentEnvStorageParams validates that providing environment variables that change
// and add to the given storage parameters will change and add parameters to the parsed
// Configuration struct
func (suite *ConfigSuite) TestParseWithDifferentEnvStorageParams() {
	suite.expectedConfig.Storage.setParameter("string1", "us-west-1")
	suite.expectedConfig.Storage.setParameter("bool1", true)
	suite.expectedConfig.Storage.setParameter("newparam", "some Value")

	suite.T().Setenv("REGISTRY_STORAGE_SOMEDRIVER_STRING1", "us-west-1")
	suite.T().Setenv("REGISTRY_STORAGE_SOMEDRIVER_BOOL1", "true")
	suite.T().Setenv("REGISTRY_STORAGE_SOMEDRIVER_NEWPARAM", "some Value")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	suite.NoError(err)
	suite.Equal(suite.expectedConfig, config)
}

// TestParseWithDifferentEnvStorageType validates that providing an environment variable that
// changes the storage type will be reflected in the parsed Configuration struct
func (suite *ConfigSuite) TestParseWithDifferentEnvStorageType() {
	suite.expectedConfig.Storage = Storage{"inmemory": Parameters{}}

	suite.T().Setenv("REGISTRY_STORAGE", "inmemory")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	suite.NoError(err)
	suite.Equal(suite.expectedConfig, config)
}

// TestParseWithDifferentEnvStorageTypeAndParams validates that providing an environment variable
// that changes the storage type will be reflected in the parsed Configuration struct and that
// environment storage parameters will also be included
func (suite *ConfigSuite) TestParseWithDifferentEnvStorageTypeAndParams() {
	suite.expectedConfig.Storage = Storage{"filesystem": Parameters{}}
	suite.expectedConfig.Storage.setParameter("rootdirectory", "/tmp/testroot")

	suite.T().Setenv("REGISTRY_STORAGE", "filesystem")
	suite.T().Setenv("REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY", "/tmp/testroot")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	suite.NoError(err)
	suite.Equal(suite.expectedConfig, config)
}

// TestParseWithSameEnvLoglevel validates that providing an environment variable defining the log
// level to the same as the one provided in the yaml will not change the parsed Configuration struct
func (suite *ConfigSuite) TestParseWithSameEnvLoglevel() {
	suite.T().Setenv("REGISTRY_LOGLEVEL", "info")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	suite.NoError(err)
	suite.Equal(suite.expectedConfig, config)
}

// TestParseWithDifferentEnvLoglevel validates that providing an environment variable defining the
// log level will override the value provided in the yaml document
func (suite *ConfigSuite) TestParseWithDifferentEnvLoglevel() {
	suite.expectedConfig.Log.Level = "error"

	suite.T().Setenv("REGISTRY_LOG_LEVEL", "error")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	suite.NoError(err)
	suite.Equal(suite.expectedConfig, config)
}

// TestParseInvalidLoglevel validates that the parser will fail to parse a
// configuration if the loglevel is malformed
func (suite *ConfigSuite) TestParseInvalidLoglevel() {
	invalidConfigYaml := "version: 0.1\nloglevel: derp\nstorage: inmemory"
	_, err := Parse(bytes.NewReader([]byte(invalidConfigYaml)))
	suite.Error(err)

	suite.T().Setenv("REGISTRY_LOGLEVEL", "derp")

	_, err = Parse(bytes.NewReader([]byte(configYamlV0_1)))
	suite.Error(err)
}

// TestParseWithDifferentEnvReporting validates that environment variables
// properly override reporting parameters
func (suite *ConfigSuite) TestParseWithDifferentEnvReporting() {
	suite.expectedConfig.Reporting.Bugsnag.APIKey = "anotherBugsnagApiKey"
	suite.expectedConfig.Reporting.Bugsnag.Endpoint = "localhost:8080"
	suite.expectedConfig.Reporting.NewRelic.LicenseKey = "NewRelicLicenseKey"
	suite.expectedConfig.Reporting.NewRelic.Name = "some NewRelic NAME"

	suite.T().Setenv("REGISTRY_REPORTING_BUGSNAG_APIKEY", "anotherBugsnagApiKey")
	suite.T().Setenv("REGISTRY_REPORTING_BUGSNAG_ENDPOINT", "localhost:8080")
	suite.T().Setenv("REGISTRY_REPORTING_NEWRELIC_LICENSEKEY", "NewRelicLicenseKey")
	suite.T().Setenv("REGISTRY_REPORTING_NEWRELIC_NAME", "some NewRelic NAME")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	suite.NoError(err)
	suite.Equal(suite.expectedConfig, config)
}

// TestParseInvalidVersion validates that the parser will fail to parse a newer configuration
// version than the CurrentVersion
func (suite *ConfigSuite) TestParseInvalidVersion() {
	suite.expectedConfig.Version = MajorMinorVersion(CurrentVersion.Major(), CurrentVersion.Minor()+1)
	configBytes, err := yaml.Marshal(suite.expectedConfig)
	suite.NoError(err)
	_, err = Parse(bytes.NewReader(configBytes))
	suite.Error(err)
}

// TestParseExtraneousVars validates that environment variables referring to
// nonexistent variables don't cause side effects.
func (suite *ConfigSuite) TestParseExtraneousVars() {
	suite.expectedConfig.Reporting.Bugsnag.Endpoint = "localhost:8080"

	// A valid environment variable
	suite.T().Setenv("REGISTRY_REPORTING_BUGSNAG_ENDPOINT", "localhost:8080")

	// Environment variables which shouldn't set config items
	suite.T().Setenv("registry_REPORTING_NEWRELIC_LICENSEKEY", "NewRelicLicenseKey")
	suite.T().Setenv("REPORTING_NEWRELIC_NAME", "some NewRelic NAME")
	suite.T().Setenv("REGISTRY_DUCKS", "quack")
	suite.T().Setenv("REGISTRY_REPORTING_ASDF", "ghjk")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	suite.NoError(err)
	suite.Equal(suite.expectedConfig, config)
}

// TestParseEnvVarImplicitMaps validates that environment variables can set
// values in maps that don't already exist.
func (suite *ConfigSuite) TestParseEnvVarImplicitMaps() {
	readonly := make(map[string]interface{})
	readonly["enabled"] = true

	maintenance := make(map[string]interface{})
	maintenance["readonly"] = readonly

	suite.expectedConfig.Storage["maintenance"] = maintenance

	suite.T().Setenv("REGISTRY_STORAGE_MAINTENANCE_READONLY_ENABLED", "true")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	suite.NoError(err)
	suite.Equal(suite.expectedConfig, config)
}

// TestParseEnvWrongTypeMap validates that incorrectly attempting to unmarshal a
// string over existing map fails.
func (suite *ConfigSuite) TestParseEnvWrongTypeMap() {
	suite.T().Setenv("REGISTRY_STORAGE_SOMEDRIVER", "somestring")

	_, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	suite.Error(err)
}

// TestParseEnvWrongTypeStruct validates that incorrectly attempting to
// unmarshal a string into a struct fails.
func (suite *ConfigSuite) TestParseEnvWrongTypeStruct() {
	suite.T().Setenv("REGISTRY_STORAGE_LOG", "somestring")

	_, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	suite.Error(err)
}

// TestParseEnvWrongTypeSlice validates that incorrectly attempting to
// unmarshal a string into a slice fails.
func (suite *ConfigSuite) TestParseEnvWrongTypeSlice() {
	suite.T().Setenv("REGISTRY_LOG_HOOKS", "somestring")

	_, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	suite.Error(err)
}

// TestParseEnvMany tests several environment variable overrides.
// The result is not checked - the goal of this test is to detect panics
// from misuse of reflection.
func (suite *ConfigSuite) TestParseEnvMany() {
	suite.T().Setenv("REGISTRY_VERSION", "0.1")
	suite.T().Setenv("REGISTRY_LOG_LEVEL", "debug")
	suite.T().Setenv("REGISTRY_LOG_FORMATTER", "json")
	suite.T().Setenv("REGISTRY_LOG_HOOKS", "json")
	suite.T().Setenv("REGISTRY_LOG_FIELDS", "abc: xyz")
	suite.T().Setenv("REGISTRY_LOG_HOOKS", "- type: asdf")
	suite.T().Setenv("REGISTRY_LOGLEVEL", "debug")
	suite.T().Setenv("REGISTRY_STORAGE", "somedriver")
	suite.T().Setenv("REGISTRY_AUTH_PARAMS", "param1: value1")
	suite.T().Setenv("REGISTRY_AUTH_PARAMS_VALUE2", "value2")
	suite.T().Setenv("REGISTRY_AUTH_PARAMS_VALUE2", "value2")

	_, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	suite.NoError(err)
}

func checkStructs(t *testing.T, rt reflect.Type, structsChecked map[string]struct{}) {
	t.Helper()

	for rt.Kind() == reflect.Ptr || rt.Kind() == reflect.Map || rt.Kind() == reflect.Slice {
		rt = rt.Elem()
	}

	if rt.Kind() != reflect.Struct {
		return
	}
	if _, present := structsChecked[rt.String()]; present {
		// Already checked this type
		return
	}

	structsChecked[rt.String()] = struct{}{}

	byUpperCase := make(map[string]int)
	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)

		// Check that the yaml tag does not contain an _.
		yamlTag := sf.Tag.Get("yaml")
		if strings.Contains(yamlTag, "_") {
			t.Fatalf("yaml field name includes _ character: %s", yamlTag)
		}
		upper := strings.ToUpper(sf.Name)
		if _, present := byUpperCase[upper]; present {
			t.Fatalf("field name collision in configuration object: %s", sf.Name)
		}
		byUpperCase[upper] = i

		checkStructs(t, sf.Type, structsChecked)
	}
}

// TestValidateConfigStruct makes sure that the config struct has no members
// with yaml tags that would be ambiguous to the environment variable parser.
func (suite *ConfigSuite) TestValidateConfigStruct() {
	structsChecked := make(map[string]struct{})
	checkStructs(suite.T(), reflect.TypeOf(Configuration{}), structsChecked)
}

func copyConfig(config Configuration) *Configuration {
	configCopy := new(Configuration)

	configCopy.Version = MajorMinorVersion(config.Version.Major(), config.Version.Minor())
	configCopy.Loglevel = config.Loglevel
	configCopy.Log = config.Log
	configCopy.Catalog = config.Catalog
	configCopy.Log.Fields = make(map[string]interface{}, len(config.Log.Fields))
	for k, v := range config.Log.Fields {
		configCopy.Log.Fields[k] = v
	}

	configCopy.Storage = Storage{config.Storage.Type(): Parameters{}}
	for k, v := range config.Storage.Parameters() {
		configCopy.Storage.setParameter(k, v)
	}
	configCopy.Reporting = Reporting{
		Bugsnag:  BugsnagReporting{config.Reporting.Bugsnag.APIKey, config.Reporting.Bugsnag.ReleaseStage, config.Reporting.Bugsnag.Endpoint},
		NewRelic: NewRelicReporting{config.Reporting.NewRelic.LicenseKey, config.Reporting.NewRelic.Name, config.Reporting.NewRelic.Verbose},
	}

	configCopy.Auth = Auth{config.Auth.Type(): Parameters{}}
	for k, v := range config.Auth.Parameters() {
		configCopy.Auth.setParameter(k, v)
	}

	configCopy.Notifications = Notifications{Endpoints: []Endpoint{}}
	configCopy.Notifications.Endpoints = append(configCopy.Notifications.Endpoints, config.Notifications.Endpoints...)

	configCopy.HTTP.Headers = make(http.Header)
	for k, v := range config.HTTP.Headers {
		configCopy.HTTP.Headers[k] = v
	}

	return configCopy
}
