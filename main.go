package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

const (
	Upload   = "upload"
	Download = "download"

	defaultBufferKB   = 256
	defaultTimeoutSec = 120
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

type AppFlagStruct struct {
	ActionType       string
	FilePath         string
	BucketName       string
	ObjectPath       string
	KeyPath          string
	ContentType      string
	ExtraChecks      bool
	PublicRequest    bool
	NoCompress       bool
	TimeoutValue     int
	FileTimeoutValue int
	BufferSize       int
	Workers          uint
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
	filePath := flag.String("file", "", "Path of local file(s). Accepts a single path, comma-separated paths, or a glob pattern e.g. \"*.txt\". (Mandatory)")
	bucketName := flag.String("bucket", "", "Name of the bucket will be used on GCP. (Mandatory)")
	objectPath := flag.String("object", "", "Path of the object on GCP. If not set, derived from the file path. Used as a prefix when set in multi-file mode. (Optional)")
	keyPath := flag.String("key", "", "Path of local json key file will be used to authenticate on GCP. (Mandatory/Optional)")
	contentType := flag.String("type", "", "Name of IANA Media Type. (Optional)")
	extraChecks := flag.Bool("extra", false, "Perform extra bucket/object checks on GCP and enable CRC32C verification on download. (Optional)")
	publicRequest := flag.Bool("public", false, "Can be set as 'true' to perform unauthenticated connection to GCP. (Optional)")
	noCompress := flag.Bool("no-compress", false, "Disable gzip compression for transfers. Compression is enabled by default. (Optional)")
	timeoutValue := flag.Int("timeout", 0, "Total timeout in seconds (default 60s) for the entire operation. (Optional)")
	fileTimeoutValue := flag.Int("file-timeout", 0, "Per-file timeout in seconds for multi-file operations. 0 (default) means no per-file limit, only the total timeout applies. (Optional)")
	bufferSize := flag.Int("buffer", defaultBufferKB, "I/O buffer size in KB. (Optional)")
	workers := flag.Uint("workers", 0, "Number of concurrent workers for multi-file upload. 0 (default) for sequential execution, N>0 for concurrent with N workers. (Optional)")

	flag.Parse()

	appFlag.ActionType = *actionType
	appFlag.FilePath = *filePath
	appFlag.BucketName = *bucketName
	appFlag.ObjectPath = *objectPath
	appFlag.KeyPath = *keyPath
	appFlag.ContentType = *contentType
	appFlag.ExtraChecks = *extraChecks
	appFlag.PublicRequest = *publicRequest
	appFlag.NoCompress = *noCompress
	appFlag.TimeoutValue = *timeoutValue
	appFlag.FileTimeoutValue = *fileTimeoutValue
	appFlag.BufferSize = *bufferSize
	appFlag.Workers = *workers

}

func resolveFiles(fileFlag string) ([]string, error) {
	var files []string

	if fileFlag == "" {
		return nil, fmt.Errorf("file parameter is required")
	}

	hasGlob := strings.ContainsAny(fileFlag, "*?[")
	hasComma := strings.Contains(fileFlag, ",")

	if hasGlob && hasComma {
		return nil, fmt.Errorf("file parameter cannot contain both glob characters (*?[) and commas — use one or the other")
	}

	if hasGlob {
		matches, err := filepath.Glob(fileFlag)
		if err != nil {
			return nil, fmt.Errorf("invalid glob pattern %q: %w", fileFlag, err)
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("no files matched pattern %q", fileFlag)
		}
		files = append(files, matches...)
	} else if hasComma {
		for p := range strings.SplitSeq(fileFlag, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				files = append(files, p)
			}
		}
	} else {
		files = append(files, fileFlag)
	}

	seen := make(map[string]bool)
	var unique []string
	for _, f := range files {
		if !seen[f] {
			seen[f] = true
			unique = append(unique, f)
		}
	}

	return unique, nil
}

func deriveObjectPath(filePath string) string {
	objPath := filepath.ToSlash(filepath.Clean(filePath))

	for strings.HasPrefix(objPath, "../") {
		objPath = objPath[3:]
	}
	objPath = strings.TrimPrefix(objPath, "/")

	if objPath == "" || objPath == ".." || objPath == "." {
		return filepath.Base(filePath)
	}
	return objPath
}

func getBufferSizeBytes() int {
	size := appFlag.BufferSize * 1024
	if size <= 0 {
		return defaultBufferKB * 1024
	}
	return size
}

func effectiveBufferKB() int {
	if appFlag.BufferSize <= 0 {
		return defaultBufferKB
	}
	return appFlag.BufferSize
}

func closeWithWarn(c io.Closer, label string) {
	if err := c.Close(); err != nil {
		LogWarn.Println("Cannot close " + label + ": " + err.Error())
	}
}

func computeCRC32C(r io.Reader) (uint32, error) {
	h := crc32.New(crc32cTable)
	if _, err := io.Copy(h, r); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

func main() {

	start := time.Now()
	LogAlways.Println("HELLO MSG: Welcome to GCP-Bucket-Loader v3.0 by EY!")

	appFlag = GetAppFlag()

	if appFlag.ActionType == "" || appFlag.FilePath == "" || appFlag.BucketName == "" {
		LogErr.Fatalln("FATAL ERROR: Action, file, and bucket parameters must be filled!")
	}

	if flag.NArg() > 0 {
		LogWarn.Println("WARNING: Unexpected positional arguments detected and ignored: " + strings.Join(flag.Args(), ", "))
	}

	files, err := resolveFiles(appFlag.FilePath)
	if err != nil {
		LogErr.Fatalln("FATAL ERROR: " + err.Error())
	}
	if len(files) == 0 {
		LogErr.Fatalln("FATAL ERROR: No input files specified!")
	}

	multiFile := len(files) > 1

	if !appFlag.PublicRequest && appFlag.KeyPath == "" {
		LogErr.Fatalln("FATAL ERROR: Key parameter is mandatory when public is not set!")
	}
	if appFlag.PublicRequest && appFlag.KeyPath != "" {
		LogWarn.Println("Key parameter is unnecessary and discarded when public is set!")
	}

	if multiFile && strings.EqualFold(appFlag.ActionType, Download) {
		LogErr.Fatalln("FATAL ERROR: Multi-file download is not supported! Use single file mode.")
	}

	if !multiFile && appFlag.Workers > 0 {
		LogInfo.Println("Workers flag ignored for single-file operation.")
	}
	if !multiFile && appFlag.FileTimeoutValue > 0 {
		LogInfo.Println("File-timeout flag ignored for single-file operation.")
	}

	if !appFlag.NoCompress {
		LogInfo.Println("Gzip compression is enabled (use -no-compress to disable).")
	}
	LogInfo.Println(fmt.Sprintf("I/O buffer size: %d KB", effectiveBufferKB()))

	storageUnderlyingDataObject := new(storageUnderlyingDataStruct)
	storageUnderlyingDataObject.ctx, storageUnderlyingDataObject.cancel = createContext(appFlag.TimeoutValue)
	defer storageUnderlyingDataObject.cancel()
	storageUnderlyingDataObject.client = createClient(storageUnderlyingDataObject.ctx, appFlag.PublicRequest, appFlag.KeyPath)
	defer storageUnderlyingDataObject.client.Close()

	uploadFailed := 0
	if strings.EqualFold(appFlag.ActionType, Upload) {
		if multiFile {
			uploadFailed = processMultiFileUpload(storageUnderlyingDataObject, files, appFlag.BucketName, appFlag.ObjectPath, appFlag.ContentType, appFlag.Workers, appFlag.FileTimeoutValue)
		} else {
			objPath := appFlag.ObjectPath
			if objPath == "" {
				objPath = deriveObjectPath(files[0])
				LogInfo.Println("Object path derived from file path: " + objPath)
			}
			if err := uploadFile(storageUnderlyingDataObject, files[0], appFlag.BucketName, objPath, appFlag.ContentType, false, 0); err != nil {
				LogErr.Fatalln("FATAL ERROR: " + err.Error())
			}
		}
	} else if strings.EqualFold(appFlag.ActionType, Download) {
		objPath := appFlag.ObjectPath
		if objPath == "" {
			objPath = deriveObjectPath(files[0])
			LogInfo.Println("Object path derived from file path: " + objPath)
		}
		downloadFile(storageUnderlyingDataObject, files[0], appFlag.BucketName, objPath)
	} else {
		LogErr.Fatalln("FATAL ERROR: Wrong action parameter specified!")
	}

	duration := fmt.Sprintf("%.1f", time.Since(start).Seconds())
	LogAlways.Println("BYE MSG: All done in " + duration + "s, bye!")

	if uploadFailed > 0 {
		os.Exit(1)
	}
}

func createContext(timeoutValue int) (context.Context, context.CancelFunc) {

	var timeoutDuration time.Duration
	if timeoutValue <= 0 {
		timeoutDuration = time.Second * defaultTimeoutSec
	} else {
		timeoutDuration = time.Second * time.Duration(timeoutValue)
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, timeoutDuration)

	return ctx, cancel

}

func createClient(ctx context.Context, publicRequest bool, keyPath string) *storage.Client {

	var clientOption option.ClientOption
	if publicRequest {
		clientOption = option.WithoutAuthentication()
	} else {
		clientOption = option.WithAuthCredentialsFile(option.ServiceAccount, keyPath)
	}

	client, err := storage.NewClient(ctx, clientOption)
	if err != nil {
		LogErr.Fatalln("FATAL ERROR: Cannot create new storage client! (" + err.Error() + ")")
	}

	return client

}

func deriveMultiFileObjectPath(objectPath, filePath string) string {
	if objectPath != "" {
		base := filepath.Base(filePath)
		if strings.HasSuffix(objectPath, "/") {
			return objectPath + base
		}
		return objectPath + "/" + base
	}
	return deriveObjectPath(filePath)
}

func processMultiFileUpload(storageData *storageUnderlyingDataStruct, files []string, bucketName, objectPath, contentType string, workers uint, fileTimeout int) int {
	LogInfo.Println(fmt.Sprintf("Multi-file upload: %d files detected.", len(files)))

	if objectPath == "" {
		LogInfo.Println("Object path not specified, each file's path will be used as its object path.")
	} else {
		LogInfo.Println("Object path prefix: " + objectPath)
	}

	if appFlag.ExtraChecks {
		bkt := storageData.client.Bucket(bucketName)
		_, err := bkt.Attrs(storageData.ctx)
		if err != nil {
			if errors.Is(err, storage.ErrBucketNotExist) {
				LogErr.Fatalln("FATAL ERROR: Bucket does not exist!")
			} else {
				LogErr.Fatalln("FATAL ERROR: Cannot fetch bucket info! (" + err.Error() + ")")
			}
		}
	}

	var failed int

	if workers == 0 {
		LogInfo.Println("Processing files sequentially...")
		for _, f := range files {
			objPath := deriveMultiFileObjectPath(objectPath, f)
			if err := uploadFile(storageData, f, bucketName, objPath, contentType, true, fileTimeout); err != nil {
				failed++
			}
		}
	} else {
		LogInfo.Println(fmt.Sprintf("Processing files concurrently with %d workers...", workers))

		jobs := make(chan string, len(files))
		var mu sync.Mutex
		var wg sync.WaitGroup

		for range workers {
			wg.Go(func() {
				for f := range jobs {
					objPath := deriveMultiFileObjectPath(objectPath, f)
					if err := uploadFile(storageData, f, bucketName, objPath, contentType, true, fileTimeout); err != nil {
						mu.Lock()
						failed++
						mu.Unlock()
					}
				}
			})
		}

		for _, f := range files {
			jobs <- f
		}
		close(jobs)
		wg.Wait()
	}

	succeeded := len(files) - failed
	LogInfo.Println(fmt.Sprintf("Multi-file upload complete: %d/%d succeeded, %d failed.", succeeded, len(files), failed))

	if failed > 0 {
		LogWarn.Println(fmt.Sprintf("%d file(s) failed to upload. Check warnings above for details.", failed))
	}
	return failed
}

func uploadFile(storageUnderlyingDataObject *storageUnderlyingDataStruct, filePath string, bucketName string, objectPath string, contentType string, multiFile bool, fileTimeout int) error {

	ctx := storageUnderlyingDataObject.ctx
	client := storageUnderlyingDataObject.client

	if fileTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Second*time.Duration(fileTimeout))
		defer cancel()
	}

	fileRoot, err := os.OpenRoot(filepath.Dir(filePath))
	if err != nil {
		msg := fmt.Sprintf("Cannot open directory for file %s! (%s)", filePath, err.Error())
		if multiFile {
			LogWarn.Println("WARNING: " + msg + " Skipping.")
			return err
		}
		LogErr.Fatalln("FATAL ERROR: " + msg)
	}
	defer fileRoot.Close()
	file, err := fileRoot.Open(filepath.Base(filePath))
	if err != nil {
		msg := fmt.Sprintf("Cannot open file %s! (%s)", filePath, err.Error())
		if multiFile {
			LogWarn.Println("WARNING: " + msg + " Skipping.")
			return err
		}
		LogErr.Fatalln("FATAL ERROR: " + msg)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		msg := fmt.Sprintf("Cannot stat file %s! (%s)", filePath, err.Error())
		if multiFile {
			LogWarn.Println("WARNING: " + msg + " Skipping.")
			return err
		}
		LogErr.Fatalln("FATAL ERROR: " + msg)
	}
	if !fileInfo.Mode().IsRegular() {
		msg := fmt.Sprintf("Path %s is not a regular file! Only regular files can be uploaded.", filePath)
		if multiFile {
			LogWarn.Println("WARNING: " + msg + " Skipping.")
			return fmt.Errorf("%s", msg)
		}
		LogErr.Fatalln("FATAL ERROR: " + msg)
	}

	localCRC32C, err := computeCRC32C(file)
	if err != nil {
		msg := fmt.Sprintf("Cannot compute CRC32C for %s! (%s)", filePath, err.Error())
		if multiFile {
			LogWarn.Println("WARNING: " + msg + " Skipping.")
			return err
		}
		LogErr.Fatalln("FATAL ERROR: " + msg)
	}

	if _, err = file.Seek(0, io.SeekStart); err != nil {
		msg := fmt.Sprintf("Cannot seek file %s! (%s)", filePath, err.Error())
		if multiFile {
			LogWarn.Println("WARNING: " + msg + " Skipping.")
			return err
		}
		LogErr.Fatalln("FATAL ERROR: " + msg)
	}

	bkt := client.Bucket(bucketName)
	obj := bkt.Object(objectPath)

	if appFlag.ExtraChecks {
		if !multiFile {
			_, err = bkt.Attrs(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrBucketNotExist) {
					LogErr.Fatalln("FATAL ERROR: Bucket does not exist!")
				} else {
					LogErr.Fatalln("FATAL ERROR: Cannot fetch bucket info! (" + err.Error() + ")")
				}
			}
		}

		objAttrs, err := obj.Attrs(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrObjectNotExist) {
				LogInfo.Println("Object does not exist, going to create a new one: " + objectPath)
			} else {
				msg := fmt.Sprintf("Cannot fetch object info for %s! (%s)", objectPath, err.Error())
				if multiFile {
					LogWarn.Println("WARNING: " + msg + " Skipping.")
					return err
				}
				LogErr.Fatalln("FATAL ERROR: " + msg)
			}
		} else {
			LogWarn.Println(fmt.Sprintf("WARNING: Object %s exists, going to override it! (SIZE: %d, CRC32C: %d, GENERATION: %d)", objectPath, objAttrs.Size, objAttrs.CRC32C, objAttrs.Generation))
		}
	}

	compress := !appFlag.NoCompress

	writer := obj.NewWriter(ctx)

	if contentType != "" {
		writer.ContentType = contentType
	}

	if compress {
		writer.ContentEncoding = "gzip"
		writer.Metadata = map[string]string{
			"x-original-crc32c": strconv.FormatUint(uint64(localCRC32C), 10),
		}
	} else {
		writer.SendCRC32C = true
		writer.CRC32C = localCRC32C
	}

	bufReader := bufio.NewReaderSize(file, getBufferSizeBytes())
	var bytesWritten int64

	if compress {
		gzWriter := gzip.NewWriter(writer)
		bytesWritten, err = io.Copy(gzWriter, bufReader)
		if err != nil {
			closeWithWarn(gzWriter, "gzip writer")
			closeWithWarn(writer, "storage writer")
			msg := fmt.Sprintf("Cannot copy file %s to bucket! (%s)", filePath, err.Error())
			if multiFile {
				LogWarn.Println("WARNING: " + msg)
				return err
			}
			LogErr.Fatalln("FATAL ERROR: " + msg)
		}
		if err = gzWriter.Close(); err != nil {
			closeWithWarn(writer, "storage writer")
			msg := fmt.Sprintf("Cannot finalize gzip stream for %s! (%s)", filePath, err.Error())
			if multiFile {
				LogWarn.Println("WARNING: " + msg)
				return err
			}
			LogErr.Fatalln("FATAL ERROR: " + msg)
		}
	} else {
		bytesWritten, err = io.Copy(writer, bufReader)
		if err != nil {
			closeWithWarn(writer, "storage writer")
			msg := fmt.Sprintf("Cannot copy file %s to bucket! (%s)", filePath, err.Error())
			if multiFile {
				LogWarn.Println("WARNING: " + msg)
				return err
			}
			LogErr.Fatalln("FATAL ERROR: " + msg)
		}
	}

	err = writer.Close()
	if err != nil {
		msg := fmt.Sprintf("Cannot write file %s to bucket! (%s)", filePath, err.Error())
		if multiFile {
			LogWarn.Println("WARNING: " + msg)
			return err
		}
		LogErr.Fatalln("FATAL ERROR: " + msg)
	}

	if appFlag.ExtraChecks {
		objAttrsNew, err := obj.Attrs(ctx)
		if err != nil {
			LogWarn.Println(fmt.Sprintf("WARNING: Upload succeeded but cannot verify object info for %s. (%s)", objectPath, err.Error()))
		} else {
			compressTag := ""
			if compress {
				compressTag = ", gzip-compressed"
			}

			LogInfo.Println(fmt.Sprintf("SUCCESS: %s uploaded to %s. (Original Bytes: %d, Object SIZE: %d, Object CRC32C: %d, Local CRC32C: %d, GENERATION: %d%s)", filePath, objectPath, bytesWritten, objAttrsNew.Size, objAttrsNew.CRC32C, localCRC32C, objAttrsNew.Generation, compressTag))
			return nil
		}
	}

	compressTag := ""
	if compress {
		compressTag = ", gzip-compressed"
	}

	LogInfo.Println(fmt.Sprintf("SUCCESS: %s uploaded to %s. (Written Bytes: %d, Local CRC32C: %d%s)", filePath, objectPath, bytesWritten, localCRC32C, compressTag))
	return nil

}

func downloadFile(storageUnderlyingDataObject *storageUnderlyingDataStruct, filePath string, bucketName string, objectPath string) {

	ctx := storageUnderlyingDataObject.ctx
	client := storageUnderlyingDataObject.client

	if info, err := os.Stat(filePath); err == nil {
		if info.Mode().IsRegular() {
			LogWarn.Println("WARNING: File exists, going to override it! (Existing File's SIZE: " + strconv.FormatInt(info.Size(), 10) + ")")
		} else {
			LogWarn.Println("WARNING: Path exists but not a regular file!")
		}
	}

	bkt := client.Bucket(bucketName)
	obj := bkt.Object(objectPath)

	var objAttrs *storage.ObjectAttrs

	if appFlag.ExtraChecks {
		_, err := bkt.Attrs(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrBucketNotExist) {
				LogErr.Fatalln("FATAL ERROR: Bucket does not exist!")
			} else {
				LogErr.Fatalln("FATAL ERROR: Cannot fetch bucket info! (" + err.Error() + ")")
			}
		}

		var attrErr error
		objAttrs, attrErr = obj.Attrs(ctx)
		if attrErr != nil {
			if errors.Is(attrErr, storage.ErrObjectNotExist) {
				LogErr.Fatalln("FATAL ERROR: Object does not exist!")
			} else {
				LogErr.Fatalln("FATAL ERROR: Cannot fetch object info! (" + attrErr.Error() + ")")
			}
		}

		LogInfo.Println(fmt.Sprintf("Object info: SIZE: %d, CRC32C: %d, GENERATION: %d, ContentEncoding: %s", objAttrs.Size, objAttrs.CRC32C, objAttrs.Generation, objAttrs.ContentEncoding))
	}

	reader, err := obj.ReadCompressed(true).NewReader(ctx)
	if err != nil {
		LogErr.Fatalln("FATAL ERROR: Cannot create new reader! (" + err.Error() + ")")
	}

	compressed := strings.EqualFold(reader.Attrs.ContentEncoding, "gzip")
	if compressed {
		LogInfo.Println("Object is gzip-compressed, decompressing during download.")
	}

	fileRoot, err := os.OpenRoot(filepath.Dir(filePath))
	if err != nil {
		closeWithWarn(reader, "storage reader")
		LogErr.Fatalln("FATAL ERROR: Cannot open directory for output file! (" + err.Error() + ")")
	}
	defer fileRoot.Close()
	file, err := fileRoot.Create(filepath.Base(filePath))
	if err != nil {
		closeWithWarn(reader, "storage reader")
		LogErr.Fatalln("FATAL ERROR: Cannot create requested file! (" + err.Error() + ")")
	}
	defer file.Close()

	crcHash := crc32.New(crc32cTable)

	var src io.Reader = reader
	if compressed {
		gzReader, gzErr := gzip.NewReader(reader)
		if gzErr != nil {
			closeWithWarn(reader, "storage reader")
			closeWithWarn(file, "output file")
			if removeErr := os.Remove(filePath); removeErr != nil {
				LogWarn.Println("WARNING: Cannot delete file after error: " + removeErr.Error())
			}
			LogErr.Fatalln("FATAL ERROR: Cannot decompress gzip data! (" + gzErr.Error() + ")")
		}
		defer gzReader.Close()
		src = gzReader
	}

	teeReader := io.TeeReader(src, crcHash)
	bufWriter := bufio.NewWriterSize(file, getBufferSizeBytes())

	bytesWritten, err := io.Copy(bufWriter, teeReader)
	if err != nil {
		closeWithWarn(reader, "storage reader")
		LogErr.Fatalln("FATAL ERROR: Cannot copy object from bucket! (" + err.Error() + ")")
	}

	if err = bufWriter.Flush(); err != nil {
		closeWithWarn(reader, "storage reader")
		LogErr.Fatalln("FATAL ERROR: Cannot flush buffer to file! (" + err.Error() + ")")
	}

	err = reader.Close()
	if err != nil {
		LogErr.Fatalln("FATAL ERROR: Cannot read object from bucket! (" + err.Error() + ")")
	}

	downloadedCRC32C := crcHash.Sum32()

	if appFlag.ExtraChecks && objAttrs != nil {
		if compressed {
			origStr, ok := objAttrs.Metadata["x-original-crc32c"]
			if ok {
				origVal, parseErr := strconv.ParseUint(origStr, 10, 32)
				if parseErr == nil && uint32(origVal) == downloadedCRC32C {
					LogInfo.Println(fmt.Sprintf("SUCCESS: Object downloaded and decompressed. (Written Bytes: %d, CRC32C verified: %d)", bytesWritten, downloadedCRC32C))
			} else if parseErr == nil {
				closeWithWarn(file, "output file")
				if removeErr := os.Remove(filePath); removeErr != nil {
					LogWarn.Println("WARNING: Cannot delete file after CRC32C mismatch: " + removeErr.Error())
				}
				LogErr.Fatalln(fmt.Sprintf("FATAL ERROR: CRC32C mismatch! File deleted. (Expected: %d, Got: %d, Written Bytes: %d)", uint32(origVal), downloadedCRC32C, bytesWritten))
				} else {
					LogWarn.Println(fmt.Sprintf("WARNING: Cannot parse original CRC32C from metadata. (Written Bytes: %d, Downloaded CRC32C: %d)", bytesWritten, downloadedCRC32C))
				}
			} else {
				LogInfo.Println(fmt.Sprintf("SUCCESS: Object downloaded and decompressed. (Written Bytes: %d, CRC32C: %d, no original checksum in metadata to validate)", bytesWritten, downloadedCRC32C))
			}
		} else {
			if objAttrs.CRC32C == downloadedCRC32C {
				LogInfo.Println(fmt.Sprintf("SUCCESS: Object downloaded. (Written Bytes: %d, CRC32C verified: %d)", bytesWritten, downloadedCRC32C))
			} else {
				closeWithWarn(file, "output file")
				if removeErr := os.Remove(filePath); removeErr != nil {
					LogWarn.Println("WARNING: Cannot delete file after CRC32C mismatch: " + removeErr.Error())
				}
				LogErr.Fatalln(fmt.Sprintf("FATAL ERROR: CRC32C mismatch! File deleted. (Expected: %d, Got: %d, Written Bytes: %d)", objAttrs.CRC32C, downloadedCRC32C, bytesWritten))
			}
		}
	} else {
		decompTag := ""
		if compressed {
			decompTag = " and decompressed"
		}
		LogInfo.Println(fmt.Sprintf("SUCCESS: Object downloaded%s. (Written Bytes: %d, CRC32C: %d)", decompTag, bytesWritten, downloadedCRC32C))
	}

}
