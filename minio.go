package minio

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"

	mg "github.com/minio/minio-go"
)

var client *mg.Client
var hasBeenConfigured = false
var Endpoint, AccessKeyID, SecretAccessKey string
var UseSSL bool

func Configure(endpoint string, accessKeyID string, secretAccessKey string, useSSL bool) {
	Endpoint = endpoint
	AccessKeyID = accessKeyID
	SecretAccessKey = secretAccessKey
	UseSSL = useSSL
}

func getClient() *mg.Client {
	if client != nil {
		return client
	}

	if !hasBeenConfigured {
		log.Fatalln("failed to initialize client: did not call Configure() to supply configuration values")
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

func SaveMap(path string, m map[string]interface{}) {
	r := mapToReader(m)
	uploadInfo, err := getClient().PutObject("stats", fmt.Sprint(path, ".json"), r, r.Size(), mg.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		log.Fatal(err)
	}
	log.Print(uploadInfo)
}

func SaveText(path string, s string) {
	r := strings.NewReader(s)
	uploadInfo, err := getClient().PutObject("stats", fmt.Sprint(path, ".txt"), r, r.Size(), mg.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		log.Fatal(err)
	}
	log.Print(uploadInfo)
}

func LoadMap(path string) map[string]interface{} {
	object, err := getClient().GetObject("stats", fmt.Sprint(path, ".json"), mg.GetObjectOptions{})
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

func LoadText(path string) string {
	object, err := getClient().GetObject("stats", fmt.Sprint(path, ".txt"), mg.GetObjectOptions{})
	if err != nil {
		log.Fatal(err)
	}
	b, err := io.ReadAll(object)
	if err != nil {
		log.Fatal(err)
	}
	return string(b)
}
