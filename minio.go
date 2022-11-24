package minio

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	mg "github.com/minio/minio-go"
)

var client *mg.Client
var hasBeenConfigured = false
var Endpoint, AccessKeyID, SecretAccessKey string
var UseSSL = true

func getClient() *mg.Client {
	if client != nil {
		return client
	}

	if !hasBeenConfigured {
		if endpoint, ok := os.LookupEnv("MINIO_ENDPOINT"); !ok {
			log.Fatal("Missing value for MINIO_ENDPOINT environmnet variable")
		} else {
			Endpoint = endpoint
		}
		if accessKeyID, ok := os.LookupEnv("MINIO_ACCESS_ID"); !ok {
			log.Fatal("Missing value for MINIO_ACCESS_ID environmnet variable")
		} else {
			AccessKeyID = accessKeyID
		}
		if secretAccessKey, ok := os.LookupEnv("MINIO_ACCESS_KEY"); !ok {
			log.Fatal("Missing value for MINIO_ACCESS_KEY environmnet variable")
		} else {
			SecretAccessKey = secretAccessKey
		}

		hasBeenConfigured = true
	}

	// Initialize minio client object.
	minioClient, err := mg.New(Endpoint, AccessKeyID, SecretAccessKey, UseSSL)
	if err != nil {
		log.Fatalln(err)
	}

	client = minioClient

	return client
}

func mapToReader(m map[string]interface{}) *bytes.Reader {
	data, err := json.Marshal(m)
	if err != nil {
		log.Fatal(err)
	}
	return bytes.NewReader(data)
}

// isValid returns true if the path is composed of at least two slash-separated
// segments, meaning it describes a bucket name followed by a path and/or file
// name.
func isValid(path string) bool {
	return strings.Contains(path, "/")
}

// splitPath accepts a slash-separated path and returns the first element (the
// bucket name) and the rest of the path (filename or path with filename).
func splitPath(path string) (string, string) {
	parts := strings.SplitN(path, "/", 2)
	return parts[0], parts[1]
}

func SaveMap(path string, m map[string]interface{}) {
	r := mapToReader(m)
	if !isValid(path) {
		log.Fatalf("invalid path: %v", path)
	}
	bucket, path := splitPath(path)
	_, err := getClient().PutObject(bucket, fmt.Sprint(path, ".json"), r, r.Size(), mg.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		log.Fatal(err)
	}
}

func SaveTextWithMetadata(path string, s string, m map[string]string) {
	r := strings.NewReader(s)
	if !isValid(path) {
		log.Fatalf("invalid path: %v", path)
	}
	bucket, path := splitPath(path)
	_, err := getClient().PutObject(bucket, fmt.Sprint(path, ".txt"), r, r.Size(), mg.PutObjectOptions{ContentType: "text/plain", UserMetadata: m})
	if err != nil {
		log.Fatal(err)
	}
}

func SaveText(path string, s string) {
	SaveTextWithMetadata(path, s, map[string]string{})
}

func LoadMap(path string) map[string]interface{} {
	if !isValid(path) {
		log.Fatalf("invalid path: %v", path)
	}
	bucket, path := splitPath(path)
	object, err := getClient().GetObject(bucket, fmt.Sprint(path, ".json"), mg.GetObjectOptions{})
	if err != nil {
		log.Fatal(err)
	}
	b := new(bytes.Buffer)
	b.ReadFrom(object)
	m := new(map[string]interface{})
	err = json.Unmarshal(b.Bytes(), m)
	if err != nil {
		log.Fatal(err)
	}
	return *m
}

func loadTextWithMetadata(path string, keys []string) (string, map[string]string) {
	if !isValid(path) {
		log.Fatalf("invalid path: %v", path)
	}
	bucket, path := splitPath(path)
	object, err := getClient().GetObject(bucket, fmt.Sprint(path, ".txt"), mg.GetObjectOptions{})
	if err != nil {
		log.Fatal(err)
	}
	b, err := io.ReadAll(object)
	if err != nil {
		log.Fatal(err)
	}
	m := map[string]string{}
	if len(keys) > 0 {
		options, err := object.Stat()
		if err != nil {
			log.Fatal(err)
		}
		for _, k := range keys {
			m[k] = options.Metadata.Get(fmt.Sprint("X-Amz-Meta-", k))
		}
	}
	return string(b), m
}

func LoadText(path string) string {
	text, _ := loadTextWithMetadata(path, []string{})
	return text
}
