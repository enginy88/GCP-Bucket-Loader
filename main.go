package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

const (
	Upload   = "upload"
	Download = "download"
)

type AppFlagStruct struct {
	ActionType    string
	FilePath      string
	BucketName    string
	ObjectPath    string
	KeyPath       string
	ContentType   string
	ExtraChecks   bool
	PublicRequest bool
	TimeoutValue  uint
}

type storageUnderlyingDataStruct struct {
	ctx    context.Context
	cancel context.CancelFunc
	client *storage.Client
}

var (
	LogErr    *log.Logger
	LogWarn   *log.Logger
	LogInfo   *log.Logger
	LogAlways *log.Logger
)

func init() {
	LogErr = log.New(os.Stderr, "(GCP-Bucket-Loader) ERROR: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	LogWarn = log.New(os.Stdout, "(GCP-Bucket-Loader) WARNING: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	LogInfo = log.New(os.Stdout, "(GCP-Bucket-Loader) INFO: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	LogAlways = log.New(os.Stdout, "(GCP-Bucket-Loader) ALWAYS: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
}

var appFlag *AppFlagStruct

func GetAppFlag() *AppFlagStruct {

	appFlagObject := new(AppFlagStruct)
	appFlag = appFlagObject

	parseAppFlag()

	return appFlag

}

func parseAppFlag() {

	actionType := flag.String("action", "", "Type of action, which can be either 'upload' or 'download'. (Mandatory)")
	filePath := flag.String("file", "", "Path of local file will be uploaded or downloaded. (Mandatory)")
	bucketName := flag.String("bucket", "", "Name of the bucket will be used on GCP. (Mandatory)")
	objectPath := flag.String("object", "", "Path of the object will be placed under bucket on GCP. (Mandatory)")
	keyPath := flag.String("key", "", "Path of local json key file will be used to authenticate on GCP. (Mandatory/Optional)")
	contentType := flag.String("type", "", "Name of IANA Media Type. (Optional)")
	extraChecks := flag.Bool("extra", false, "Can be set as 'true' to perform bucket and object checks on GCP. (Optional)")
	publicRequest := flag.Bool("public", false, "Can be set as 'true' to perform unauthenticated connection to GCP. (Optional)")
	timeoutValue := flag.Uint("timeout", 0, "Can be set to spesify timeout value in seconds (default 60s) for connection to GCP. (Optional)")

	flag.Parse()

	appFlag.ActionType = *actionType
	appFlag.FilePath = *filePath
	appFlag.BucketName = *bucketName
	appFlag.ObjectPath = *objectPath
	appFlag.KeyPath = *keyPath
	appFlag.ContentType = *contentType
	appFlag.ExtraChecks = *extraChecks
	appFlag.PublicRequest = *publicRequest
	appFlag.TimeoutValue = *timeoutValue

}

func main() {

	start := time.Now()
	LogAlways.Println("HELLO MSG: Welcome to GCP-Bucket-Loader v2.1 by EY!")

	appFlag = GetAppFlag()

	if appFlag.ActionType == "" || appFlag.FilePath == "" || appFlag.BucketName == "" || appFlag.ObjectPath == "" {
		LogErr.Fatalln("FATAL ERROR: All mandatory parameters must be filled!")
	}

	if !appFlag.PublicRequest && appFlag.KeyPath == "" {
		LogErr.Fatalln("FATAL ERROR: Key parameter is mandatory when public is not set!")
	}
	if appFlag.PublicRequest && appFlag.KeyPath != "" {
		LogWarn.Println("WARNING: Key parameter is unnessary and discarded when public is set!")
	}

	storageUnderlyingDataObject := new(storageUnderlyingDataStruct)
	storageUnderlyingDataObject.ctx, storageUnderlyingDataObject.cancel = createContext(int(appFlag.TimeoutValue))
	storageUnderlyingDataObject.client = createClient(storageUnderlyingDataObject.ctx, appFlag.PublicRequest, appFlag.KeyPath)

	if strings.EqualFold(appFlag.ActionType, Upload) {
		uploadFile(storageUnderlyingDataObject, appFlag.FilePath, appFlag.BucketName, appFlag.ObjectPath, appFlag.ContentType)
	} else if strings.EqualFold(appFlag.ActionType, Download) {
		downloadFile(storageUnderlyingDataObject, appFlag.FilePath, appFlag.BucketName, appFlag.ObjectPath)
	} else {
		LogErr.Fatalln("FATAL ERROR: Wrong action parameter specified!")
	}

	duration := fmt.Sprintf("%.1f", time.Since(start).Seconds())
	LogAlways.Println("BYE MSG: All done in " + duration + "s, bye!")

}

func createContext(timeoutValue int) (context.Context, context.CancelFunc) {

	var timeoutDuration time.Duration
	if timeoutValue <= 0 {
		timeoutDuration = time.Second * 60
	} else {
		timeoutDuration = time.Second * time.Duration(timeoutValue)
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, timeoutDuration)

	return ctx, cancel

}

func createClient(ctx context.Context, PublicRequest bool, keyPath string) *storage.Client {

	var clientOption option.ClientOption
	if PublicRequest {
		clientOption = option.WithoutAuthentication()
	} else {
		clientOption = option.WithCredentialsFile(keyPath)
	}

	client, err := storage.NewClient(ctx, clientOption)
	if err != nil {
		LogErr.Fatalln("FATAL ERROR: Cannot create new storage client! (" + err.Error() + ")")
	}

	return client

}

func uploadFile(storageUnderlyingDataObject *storageUnderlyingDataStruct, filePath string, bucketName string, objectPath string, contentType string) {

	ctx := storageUnderlyingDataObject.ctx
	cancel := storageUnderlyingDataObject.cancel
	client := storageUnderlyingDataObject.client

	defer cancel()
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

func downloadFile(storageUnderlyingDataObject *storageUnderlyingDataStruct, filePath string, bucketName string, objectPath string) {

	ctx := storageUnderlyingDataObject.ctx
	cancel := storageUnderlyingDataObject.cancel
	client := storageUnderlyingDataObject.client

	defer cancel()
	defer client.Close()

	if info, err := os.Stat(filePath); err == nil {
		if info.Mode().IsRegular() {
			LogWarn.Println("WARNING: File exists, going to override it! (Existing File's SIZE: " + strconv.FormatInt(info.Size(), 10) + ")")
		} else {
			LogWarn.Println("WARNING: Path exists but not a regular file!")
		}

	}

	file, err := os.Create(filePath)
	if err != nil {
		LogErr.Fatalln("FATAL ERROR: Cannot create requested file! (" + err.Error() + ")")
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
				LogErr.Fatalln("FATAL ERROR: Object does not exist!")
			} else {
				LogErr.Fatalln("FATAL ERROR: Cannot fetch object info! (" + err.Error() + ")")
			}
		} else {
			LogWarn.Println("WARNING: Object exists! (Existing Object's SIZE: " + strconv.FormatInt(objAttrs.Size, 10) + ", CRC32: " + strconv.FormatUint(uint64(objAttrs.CRC32C), 10) + ", GENERATION: " + strconv.FormatInt(objAttrs.Generation, 10) + ")")
		}
	}

	reader, err := obj.NewReader(ctx)
	if err != nil {
		LogErr.Fatalln("FATAL ERROR: Cannot create new reader! (" + err.Error() + ")")
	}
	defer reader.Close()

	bytes, err := io.Copy(file, reader)
	if err != nil {
		LogErr.Fatalln("FATAL ERROR: Cannot copy object from bucket! (" + err.Error() + ")")
	}

	err = reader.Close()
	if err != nil {
		LogErr.Fatalln("FATAL ERROR: Cannot read object from bucket! (" + err.Error() + ")")
	}

	LogInfo.Println("SUCCESS: Object downloaded from GCP Bucket. (Written Bytes: " + strconv.FormatInt(bytes, 10) + ")")

}
