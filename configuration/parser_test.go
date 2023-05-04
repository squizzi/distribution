package configuration

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"
)

type localConfiguration struct {
	Version Version `yaml:"version"`
	Log     *Log    `yaml:"log"`
}

type Log struct {
	Formatter string `yaml:"formatter,omitempty"`
}

var expectedConfig = localConfiguration{
	Version: "0.1",
	Log: &Log{
		Formatter: "json",
	},
}

type ParserSuite struct {
	suite.Suite
}

func TestParserSuite(t *testing.T) {
	suite.Run(t, new(ConfigSuite))
}

func (suite *ParserSuite) TestParserOverwriteIninitializedPoiner() {
	config := localConfiguration{}

	suite.T().Setenv("REGISTRY_LOG_FORMATTER", "json")

	p := NewParser("registry", []VersionedParseInfo{
		{
			Version: "0.1",
			ParseAs: reflect.TypeOf(config),
			ConversionFunc: func(c interface{}) (interface{}, error) {
				return c, nil
			},
		},
	})

	err := p.Parse([]byte(`{version: "0.1", log: {formatter: "text"}}`), &config)
	suite.NoError(err)
	suite.Equal(expectedConfig, config)
}

func (suite *ParserSuite) TestParseOverwriteUnininitializedPoiner() {
	config := localConfiguration{}

	suite.T().Setenv("REGISTRY_LOG_FORMATTER", "json")

	p := NewParser("registry", []VersionedParseInfo{
		{
			Version: "0.1",
			ParseAs: reflect.TypeOf(config),
			ConversionFunc: func(c interface{}) (interface{}, error) {
				return c, nil
			},
		},
	})

	err := p.Parse([]byte(`{version: "0.1"}`), &config)
	suite.NoError(err)
	suite.Equal(expectedConfig, config)
}
