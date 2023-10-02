package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

type AppFlagStruct struct {
	FilePath    string
	ContentType string
	BucketName  string
	ObjectPath  string
	KeyPath     string
	ExtraChecks bool
}

var (
	LogErr    *log.Logger
	LogWarn   *log.Logger
	LogInfo   *log.Logger
	LogAlways *log.Logger
)

func init() {
	LogErr = log.New(os.Stderr, "(GCP-Bucket-Uploader) ERROR: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	LogWarn = log.New(os.Stdout, "(GCP-Bucket-Uploader) WARNING: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	LogInfo = log.New(os.Stdout, "(GCP-Bucket-Uploader) INFO: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	LogAlways = log.New(os.Stdout, "(GCP-Bucket-Uploader) ALWAYS: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
}

var appFlag *AppFlagStruct

func GetAppFlag() *AppFlagStruct {

	appFlagObject := new(AppFlagStruct)
	appFlag = appFlagObject

	parseAppFlag()

	return appFlag

}

func parseAppFlag() {

	filePath := flag.String("file", "", "Path of local file will be uploaded. (Mandatory)")
	contentType := flag.String("type", "", "Name of IANA Media Type. (Optional)")
	bucketName := flag.String("bucket", "", "Name of the bucket will be used on GCP. (Mandatory)")
	objectPath := flag.String("object", "", "Path of the object will be placed under bucket on GCP. (Mandatory)")
	keyPath := flag.String("key", "", "Path of local json key file will be used to authenticate on GCP. (Mandatory)")
	extraChecks := flag.Bool("extra", false, "Can be set as 'true' to perform bucket and object checks on GCP. (Optional)")

	flag.Parse()

	appFlag.FilePath = *filePath
	appFlag.ContentType = *contentType
	appFlag.BucketName = *bucketName
	appFlag.ObjectPath = *objectPath
	appFlag.KeyPath = *keyPath
	appFlag.ExtraChecks = *extraChecks

}

func main() {

	start := time.Now()
	LogAlways.Println("HELLO MSG: Welcome to GCP-Bucket-Uploader v1.0 by EY!")

	appFlag = GetAppFlag()

	if appFlag.FilePath == "" || appFlag.BucketName == "" || appFlag.ObjectPath == "" || appFlag.KeyPath == "" {
		LogErr.Fatalln("FATAL ERROR: All mandatory parameters must be filled!")
	}

	uploadFile(appFlag.FilePath, appFlag.ContentType, appFlag.BucketName, appFlag.ObjectPath, appFlag.KeyPath)

	duration := fmt.Sprintf("%.1f", time.Since(start).Seconds())
	LogAlways.Println("BYE MSG: All done in " + duration + "s, bye!")

}

func uploadFile(filePath string, contentType string, bucketName string, objectPath string, keyPath string) {

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*60)
	defer cancel()

	client, err := storage.NewClient(ctx, option.WithCredentialsFile(keyPath))
	if err != nil {
		LogErr.Fatalln("FATAL ERROR: Cannot create new storage client! (" + err.Error() + ")")
	}
	defer client.Close()

	file, err := os.Open(filePath)
	if err != nil {
		LogErr.Fatalln("FATAL ERROR: Cannot open requested file! (" + err.Error() + ")")
	}
	defer file.Close()

	bkt := client.Bucket(bucketName)
	obj := bkt.Object(objectPath)

	if appFlag.ExtraChecks {
		_, err = bkt.Attrs(ctx)
		if err != nil {
			if err == storage.ErrBucketNotExist {
				LogErr.Fatalln("FATAL ERROR: Bucket does not exist!")
			} else {
				LogErr.Fatalln("FATAL ERROR: Cannot fetch bucket info! (" + err.Error() + ")")
			}
		}

		objAttrs, err := obj.Attrs(ctx)
		if err != nil {
			if err == storage.ErrObjectNotExist {
				LogWarn.Println("WARNING: Object does not exist, going to create a new one.")
			} else {
				LogErr.Fatalln("FATAL ERROR: Cannot fetch object info! (" + err.Error() + ")")
			}
		} else {
			LogWarn.Println("WARNING: Object exists, going to override it! (Existing Object's SIZE: " + strconv.FormatInt(objAttrs.Size, 10) + ", CRC32: " + strconv.FormatUint(uint64(objAttrs.CRC32C), 10) + ", GENERATION: " + strconv.FormatInt(objAttrs.Generation, 10) + ")")
		}
	}

	writer := obj.NewWriter(ctx)
	defer writer.Close()

	if appFlag.ContentType != "" {
		writer.ContentType = contentType
	}

	bytes, err := io.Copy(writer, file)
	if err != nil {
		LogErr.Fatalln("FATAL ERROR: Cannot copy file to bucket! (" + err.Error() + ")")
	}

	err = writer.Close()
	if err != nil {
		LogErr.Fatalln("FATAL ERROR: Cannot write file to bucket! (" + err.Error() + ")")
	}

	if appFlag.ExtraChecks {
		objAttrsNew, err := obj.Attrs(ctx)
		if err != nil {
			LogErr.Fatalln("FATAL ERROR: Cannot fetch object info! (" + err.Error() + ")")
		}

		LogInfo.Println("SUCCESS: Object uploaded to GCP Bucket. (Uploaded Object's SIZE: " + strconv.FormatInt(objAttrsNew.Size, 10) + ", CRC32: " + strconv.FormatUint(uint64(objAttrsNew.CRC32C), 10) + ", GENERATION: " + strconv.FormatInt(objAttrsNew.Generation, 10) + ")")
	} else {
		LogInfo.Println("SUCCESS: Object uploaded to GCP Bucket. (Written Bytes: " + strconv.FormatInt(bytes, 10) + ")")
	}

}
