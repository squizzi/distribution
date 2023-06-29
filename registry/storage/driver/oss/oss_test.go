//go:build include_oss
// +build include_oss

package oss

import (
	"os"
	"strconv"
	"testing"

	alioss "github.com/denverdino/aliyungo/oss"
	"github.com/distribution/distribution/v3/context"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"
	"gopkg.in/check.v1"
)

const (
	envAccessKey       = "ALIYUN_ACCESS_KEY_ID"
	envSecretKey       = "ALIYUN_ACCESS_KEY_SECRET"
	envBucket          = "OSS_BUCKET"
	envRegion          = "OSS_REGION"
	envInternal        = "OSS_INTERNAL"
	envEncrypt         = "OSS_ENCRYPT"
	envSecure          = "OSS_SECURE"
	envEndpoint        = "OSS_ENDPOINT"
	envEncryptionKeyID = "OSS_ENCRYPTIONKEYID"
)

// Hook up gocheck into the "go test" runner.

var ossDriverConstructor func(rootDirectory string) (*Driver, error)

func init() {
	config := []struct {
		env   string
		value *string
	}{
		{envAccessKey, &accessKey},
		{envSecretKey, &secretKey},
		{envBucket, &bucket},
		{envRegion, &region},
		{envInternal, &internal},
		{envEncrypt, &encrypt},
		{envSecure, &secure},
		{envEndpoint, &endpoint},
		{envEncryptionKeyID, &encryptionKeyID},
	}

	for _, v := range config {
		*v.value = os.Getenv(v.env)
	}

	root, err := os.MkdirTemp("", "driver-")
	if err != nil {
		panic(err)
	}
	defer os.Remove(root)

	ossDriverConstructor = func(rootDirectory string) (*Driver, error) {
		encryptBool := false
		if encrypt != "" {
			encryptBool, err = strconv.ParseBool(encrypt)
			if err != nil {
				return nil, err
			}
		}

		secureBool := false
		if secure != "" {
			secureBool, err = strconv.ParseBool(secure)
			if err != nil {
				return nil, err
			}
		}

		internalBool := false
		if internal != "" {
			internalBool, err = strconv.ParseBool(internal)
			if err != nil {
				return nil, err
			}
		}

		parameters := DriverParameters{
			AccessKeyID:     accessKey,
			AccessKeySecret: secretKey,
			Bucket:          bucket,
			Region:          alioss.Region(region),
			Internal:        internalBool,
			ChunkSize:       minChunkSize,
			RootDirectory:   rootDirectory,
			Encrypt:         encryptBool,
			Secure:          secureBool,
			Endpoint:        endpoint,
			EncryptionKeyID: encryptionKeyID,
		}

		return New(parameters)
	}

	testsuites.RegisterSubTest(ossDriverConstructor, testsuites.WithSkipCheck(envAccessKey, envSecretKey, envRegion, envBucket, envEncrypt))
}

func TestEmptyRootList(t *testing.T) {
	if skipCheck() != "" {
		t.Skip(skipCheck())
	}

	validRoot := t.TempDir()

	rootedDriver, err := ossDriverConstructor(validRoot)
	if err != nil {
		t.Fatalf("unexpected error creating rooted driver: %v", err)
	}

	emptyRootDriver, err := ossDriverConstructor("")
	if err != nil {
		t.Fatalf("unexpected error creating empty root driver: %v", err)
	}

	slashRootDriver, err := ossDriverConstructor("/")
	if err != nil {
		t.Fatalf("unexpected error creating slash root driver: %v", err)
	}

	filename := "/test"
	contents := []byte("contents")
	ctx := context.Background()
	err = rootedDriver.PutContent(ctx, filename, contents)
	if err != nil {
		t.Fatalf("unexpected error creating content: %v", err)
	}
	defer rootedDriver.Delete(ctx, filename)

	keys, err := emptyRootDriver.List(ctx, "/")
	if err != nil {
		t.Fatalf("unexpected error listing empty root content: %v", err)
	}
	for _, path := range keys {
		if !storagedriver.PathRegexp.MatchString(path) {
			t.Fatalf("unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
		}
	}

	keys, err = slashRootDriver.List(ctx, "/")
	if err != nil {
		t.Fatalf("unexpected error listing slash root content: %v", err)
	}
	for _, path := range keys {
		if !storagedriver.PathRegexp.MatchString(path) {
			t.Fatalf("unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
		}
	}
}
