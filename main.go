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
	defaultWorkers    = 0
	defaultRetryCount = 0
	appVersion        = "v4.0"
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
	RetryCount       int
}

type gcsSession struct {
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
	appFlag = &AppFlagStruct{}
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
	timeoutValue := flag.Int("timeout", 0, "Total timeout in seconds for the entire operation. 0 or omitted means no limit. (Optional)")
	fileTimeoutValue := flag.Int("file-timeout", 0, "Per-file timeout in seconds for multi-file uploads. 0 (default) means no per-file limit; -timeout still applies when set. (Optional)")
	bufferSize := flag.Int("buffer", defaultBufferKB, "I/O buffer size in KB. (Optional)")
	workers := flag.Uint("workers", defaultWorkers, "Number of concurrent workers for multi-file upload. 0 (default) for sequential execution, N>0 for concurrent with N workers. (Optional)")
	retryCount := flag.Int("retry", defaultRetryCount, "Number of retry attempts for GCP/network operations on failure. 0 (default) means no retry. (Optional)")

	flag.Parse()

	appFlag.ActionType = strings.ToLower(*actionType) // normalize so == comparisons work everywhere
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
	appFlag.RetryCount = *retryCount

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
		if len(files) == 0 {
			return nil, fmt.Errorf("no file paths found in comma-separated list %q", fileFlag)
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

func effectiveBufferKB() int {
	if appFlag.BufferSize <= 0 {
		return defaultBufferKB
	}
	return appFlag.BufferSize
}

func getBufferSizeBytes() int {
	return effectiveBufferKB() * 1024
}

func closeWithWarn(c io.Closer, label string) {
	if err := c.Close(); err != nil {
		LogWarn.Println("Cannot close " + label + ": " + err.Error())
	}
}

// withRetry runs op up to 1+retryCount times, sleeping 1s between attempts.
func withRetry(retryCount int, op func() error) error {
	var err error
	for attempt := 0; attempt <= retryCount; attempt++ {
		if attempt > 0 {
			LogWarn.Printf("Retry attempt %d/%d after error: %s", attempt, retryCount, err.Error())
			time.Sleep(time.Second)
		}
		err = op()
		if err == nil {
			return nil
		}
	}
	return err
}

func computeCRC32C(r io.Reader) (uint32, error) {
	h := crc32.New(crc32cTable)
	if _, err := io.Copy(h, r); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

// checkBucketExists fetches bucket attrs (with retry) to verify the bucket is accessible.
func checkBucketExists(ctx context.Context, bkt *storage.BucketHandle) error {
	return withRetry(appFlag.RetryCount, func() error {
		_, e := bkt.Attrs(ctx)
		return e
	})
}

// handleUploadError logs and either returns (multiFile) or fatals (single file).
func handleUploadError(multiFile bool, msg string, err error) error {
	if err == nil {
		err = errors.New(msg)
	}
	if multiFile {
		LogWarn.Println(msg + " Skipping.")
		return err
	}
	LogErr.Fatalln(msg)
	return nil // unreachable
}

func main() {

	start := time.Now()
	LogAlways.Println("HELLO MSG: Welcome to GCP-Bucket-Loader " + appVersion + " by EY!")

	appFlag = GetAppFlag()

	if appFlag.ActionType == "" || appFlag.FilePath == "" || appFlag.BucketName == "" {
		LogErr.Fatalln("Action, file, and bucket parameters must be filled!")
	}

	if flag.NArg() > 0 {
		LogWarn.Println("Unexpected positional arguments detected and ignored: " + strings.Join(flag.Args(), ", "))
	}

	files, err := resolveFiles(appFlag.FilePath)
	if err != nil {
		LogErr.Fatalln(err.Error())
	}

	multiFile := len(files) > 1

	if !appFlag.PublicRequest && appFlag.KeyPath == "" {
		LogErr.Fatalln("Key parameter is mandatory when public is not set!")
	}
	if appFlag.PublicRequest && appFlag.KeyPath != "" {
		LogWarn.Println("Key parameter is unnecessary and discarded when public is set.")
	}

	if multiFile && appFlag.ActionType == Download {
		LogErr.Fatalln("Multi-file download is not supported! Use single file mode.")
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
	LogInfo.Printf("I/O buffer size: %d KB", effectiveBufferKB())

	session := new(gcsSession)
	session.ctx, session.cancel = createContext(appFlag.TimeoutValue)
	defer session.cancel()
	session.client = createClient(session.ctx, appFlag.PublicRequest, appFlag.KeyPath)
	defer session.client.Close()

	uploadFailed := 0
	if appFlag.ActionType == Upload {
		if multiFile {
			uploadFailed = processMultiFileUpload(session, files, appFlag.BucketName, appFlag.ObjectPath, appFlag.ContentType, appFlag.Workers, appFlag.FileTimeoutValue)
		} else {
			objPath := appFlag.ObjectPath
			if objPath == "" {
				objPath = deriveObjectPath(files[0])
				LogInfo.Println("Object path derived from file path: " + objPath)
			}
			if err := uploadFile(session, files[0], appFlag.BucketName, objPath, appFlag.ContentType, false, 0); err != nil {
				LogErr.Fatalln(err.Error())
			}
		}
	} else if appFlag.ActionType == Download {
		objPath := appFlag.ObjectPath
		if objPath == "" {
			objPath = deriveObjectPath(files[0])
			LogInfo.Println("Object path derived from file path: " + objPath)
		}
		downloadFile(session, files[0], appFlag.BucketName, objPath)
	} else {
		LogErr.Fatalln("Wrong action parameter specified!")
	}

	duration := fmt.Sprintf("%.1f", time.Since(start).Seconds())
	if uploadFailed > 0 {
		LogAlways.Printf("BYE MSG: Completed with %d failure(s) in %ss.", uploadFailed, duration)
		os.Exit(1)
	}
	LogAlways.Println("BYE MSG: All done in " + duration + "s, bye!")

}

func createContext(timeoutValue int) (context.Context, context.CancelFunc) {
	ctx := context.Background()
	if timeoutValue <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, time.Second*time.Duration(timeoutValue))
}

// createClient builds the GCS client. storage.NewClient is a local constructor
// (no network I/O), so it is not wrapped in withRetry.
func createClient(ctx context.Context, publicRequest bool, keyPath string) *storage.Client {

	var clientOption option.ClientOption
	if publicRequest {
		clientOption = option.WithoutAuthentication()
	} else {
		clientOption = option.WithAuthCredentialsFile(option.ServiceAccount, keyPath)
	}

	client, err := storage.NewClient(ctx, clientOption)
	if err != nil {
		LogErr.Fatalln("Cannot create new storage client! (" + err.Error() + ")")
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

func processMultiFileUpload(session *gcsSession, files []string, bucketName, objectPath, contentType string, workers uint, fileTimeout int) int {
	LogInfo.Printf("Multi-file upload: %d files detected.", len(files))

	if objectPath == "" {
		LogInfo.Println("Object path not specified, each file's path will be used as its object path.")
	} else {
		LogInfo.Println("Object path prefix: " + objectPath)
	}

	if appFlag.ExtraChecks {
		bkt := session.client.Bucket(bucketName)
		if err := checkBucketExists(session.ctx, bkt); err != nil {
			if errors.Is(err, storage.ErrBucketNotExist) {
				LogErr.Fatalln("Bucket does not exist!")
			} else {
				LogErr.Fatalln("Cannot fetch bucket info! (" + err.Error() + ")")
			}
		}
	}

	var failed int

	if workers == 0 {
		LogInfo.Println("Processing files sequentially...")
		for _, f := range files {
			objPath := deriveMultiFileObjectPath(objectPath, f)
			if err := uploadFile(session, f, bucketName, objPath, contentType, true, fileTimeout); err != nil {
				failed++
			}
		}
	} else {
		LogInfo.Printf("Processing files concurrently with %d workers...", workers)

		jobs := make(chan string, len(files))
		var mu sync.Mutex
		var wg sync.WaitGroup

		for range workers {
			wg.Go(func() {
				for f := range jobs {
					objPath := deriveMultiFileObjectPath(objectPath, f)
					if err := uploadFile(session, f, bucketName, objPath, contentType, true, fileTimeout); err != nil {
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
	LogInfo.Printf("Multi-file upload complete: %d/%d succeeded, %d failed.", succeeded, len(files), failed)

	if failed > 0 {
		LogWarn.Printf("%d file(s) failed to upload. Check warnings above for details.", failed)
	}
	return failed
}

func uploadFile(session *gcsSession, filePath string, bucketName string, objectPath string, contentType string, multiFile bool, fileTimeout int) error {

	ctx := session.ctx
	client := session.client

	if fileTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Second*time.Duration(fileTimeout))
		defer cancel()
	}

	fileRoot, err := os.OpenRoot(filepath.Dir(filePath))
	if err != nil {
		return handleUploadError(multiFile, fmt.Sprintf("Cannot open directory for file %s! (%s)", filePath, err.Error()), err)
	}
	defer fileRoot.Close()

	file, err := fileRoot.Open(filepath.Base(filePath))
	if err != nil {
		return handleUploadError(multiFile, fmt.Sprintf("Cannot open file %s! (%s)", filePath, err.Error()), err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return handleUploadError(multiFile, fmt.Sprintf("Cannot stat file %s! (%s)", filePath, err.Error()), err)
	}
	if !fileInfo.Mode().IsRegular() {
		return handleUploadError(multiFile, fmt.Sprintf("Path %s is not a regular file! Only regular files can be uploaded.", filePath), nil)
	}

	bkt := client.Bucket(bucketName)
	obj := bkt.Object(objectPath)
	compress := !appFlag.NoCompress

	// ExtraChecks pre-flight: verify bucket/object state before reading the file.
	if appFlag.ExtraChecks {
		if !multiFile {
			if err = checkBucketExists(ctx, bkt); err != nil {
				if errors.Is(err, storage.ErrBucketNotExist) {
					LogErr.Fatalln("Bucket does not exist!")
				} else {
					LogErr.Fatalln("Cannot fetch bucket info! (" + err.Error() + ")")
				}
			}
		}

		var objAttrs *storage.ObjectAttrs
		err = withRetry(appFlag.RetryCount, func() error {
			var e error
			objAttrs, e = obj.Attrs(ctx)
			return e
		})
		if err != nil {
			if errors.Is(err, storage.ErrObjectNotExist) {
				LogInfo.Println("Object does not exist, going to create a new one: " + objectPath)
			} else {
				return handleUploadError(multiFile, fmt.Sprintf("Cannot fetch object info for %s! (%s)", objectPath, err.Error()), err)
			}
		} else {
			LogWarn.Printf("Object %s exists, going to override it! (SIZE: %d, CRC32C: %d, GENERATION: %d)", objectPath, objAttrs.Size, objAttrs.CRC32C, objAttrs.Generation)
		}
	}

	// Compute CRC32C after pre-flight so we don't read the entire file when the
	// bucket/object checks would have already aborted.
	localCRC32C, err := computeCRC32C(file)
	if err != nil {
		return handleUploadError(multiFile, fmt.Sprintf("Cannot compute CRC32C for %s! (%s)", filePath, err.Error()), err)
	}

	compressTag := ""
	if compress {
		compressTag = ", gzip-compressed"
	}

	var bytesWritten int64
	err = withRetry(appFlag.RetryCount, func() error {
		if _, seekErr := file.Seek(0, io.SeekStart); seekErr != nil {
			return seekErr
		}

		// Use a per-attempt context so that a failed attempt's in-flight HTTP
		// request is cancelled immediately rather than committing partial data.
		attemptCtx, attemptCancel := context.WithCancel(ctx)
		defer attemptCancel()

		w := obj.NewWriter(attemptCtx)
		if contentType != "" {
			w.ContentType = contentType
		}
		if compress {
			w.ContentEncoding = "gzip"
			w.Metadata = map[string]string{
				"x-original-crc32c": strconv.FormatUint(uint64(localCRC32C), 10),
			}
		} else {
			w.SendCRC32C = true
			w.CRC32C = localCRC32C
		}

		bufReader := bufio.NewReaderSize(file, getBufferSizeBytes())

		if compress {
			gzWriter := gzip.NewWriter(w)
			var copyErr error
			bytesWritten, copyErr = io.Copy(gzWriter, bufReader)
			if copyErr != nil {
				return copyErr
			}
			if closeErr := gzWriter.Close(); closeErr != nil {
				return closeErr
			}
		} else {
			var copyErr error
			bytesWritten, copyErr = io.Copy(w, bufReader)
			if copyErr != nil {
				return copyErr
			}
		}

		return w.Close()
	})
	if err != nil {
		return handleUploadError(multiFile, fmt.Sprintf("Cannot write file %s to bucket! (%s)", filePath, err.Error()), err)
	}

	if appFlag.ExtraChecks {
		var objAttrsNew *storage.ObjectAttrs
		err = withRetry(appFlag.RetryCount, func() error {
			var e error
			objAttrsNew, e = obj.Attrs(ctx)
			return e
		})
		if err != nil {
			LogWarn.Printf("Upload succeeded but cannot verify object info for %s. (%s)", objectPath, err.Error())
		} else {
			LogInfo.Printf("SUCCESS: %s uploaded to %s. (Original Bytes: %d, Object SIZE: %d, Object CRC32C: %d, Local CRC32C: %d, GENERATION: %d%s)", filePath, objectPath, bytesWritten, objAttrsNew.Size, objAttrsNew.CRC32C, localCRC32C, objAttrsNew.Generation, compressTag)
			return nil
		}
	}

	LogInfo.Printf("SUCCESS: %s uploaded to %s. (Original Bytes: %d, Local CRC32C: %d%s)", filePath, objectPath, bytesWritten, localCRC32C, compressTag)
	return nil

}

func downloadFile(session *gcsSession, filePath string, bucketName string, objectPath string) {

	ctx := session.ctx
	client := session.client

	if info, err := os.Stat(filePath); err == nil {
		if info.Mode().IsRegular() {
			LogWarn.Println("File exists, going to override it! (Existing File's SIZE: " + strconv.FormatInt(info.Size(), 10) + ")")
		} else {
			LogWarn.Println("Path exists but not a regular file!")
		}
	}

	bkt := client.Bucket(bucketName)
	obj := bkt.Object(objectPath)

	var objAttrs *storage.ObjectAttrs

	if appFlag.ExtraChecks {
		if err := checkBucketExists(ctx, bkt); err != nil {
			if errors.Is(err, storage.ErrBucketNotExist) {
				LogErr.Fatalln("Bucket does not exist!")
			} else {
				LogErr.Fatalln("Cannot fetch bucket info! (" + err.Error() + ")")
			}
		}

		if err := withRetry(appFlag.RetryCount, func() error {
			var e error
			objAttrs, e = obj.Attrs(ctx)
			return e
		}); err != nil {
			if errors.Is(err, storage.ErrObjectNotExist) {
				LogErr.Fatalln("Object does not exist!")
			} else {
				LogErr.Fatalln("Cannot fetch object info! (" + err.Error() + ")")
			}
		}

		LogInfo.Printf("Object info: SIZE: %d, CRC32C: %d, GENERATION: %d, ContentEncoding: %s", objAttrs.Size, objAttrs.CRC32C, objAttrs.Generation, objAttrs.ContentEncoding)
	}

	fileRoot, err := os.OpenRoot(filepath.Dir(filePath))
	if err != nil {
		LogErr.Fatalln("Cannot open directory for output file! (" + err.Error() + ")")
	}
	defer fileRoot.Close()
	file, err := fileRoot.Create(filepath.Base(filePath))
	if err != nil {
		LogErr.Fatalln("Cannot create requested file! (" + err.Error() + ")")
	}
	defer file.Close()

	crcHash := crc32.New(crc32cTable)
	var bytesWritten int64
	var compressed bool

	err = withRetry(appFlag.RetryCount, func() error {
		// Reset file and hash state at the start of every attempt (no-op on first).
		if _, e := file.Seek(0, io.SeekStart); e != nil {
			return e
		}
		if e := file.Truncate(0); e != nil {
			return e
		}
		crcHash.Reset()

		// Per-attempt context so a failed attempt's HTTP request is aborted cleanly.
		attemptCtx, attemptCancel := context.WithCancel(ctx)
		defer attemptCancel()

		r, e := obj.ReadCompressed(true).NewReader(attemptCtx)
		if e != nil {
			return e
		}

		// Determine compression from object metadata.
		// ReadCompressed(true) fetches the raw (possibly gzip) stream; we decompress
		// manually so that the CRC is computed over the final uncompressed bytes.
		compressed = strings.EqualFold(r.Attrs.ContentEncoding, "gzip")

		var src io.Reader = r
		var gzRdr *gzip.Reader
		if compressed {
			gzRdr, e = gzip.NewReader(r)
			if e != nil {
				closeWithWarn(r, "storage reader")
				return e
			}
			src = gzRdr
		}

		teeReader := io.TeeReader(src, crcHash)
		bufWriter := bufio.NewWriterSize(file, getBufferSizeBytes())

		bytesWritten, e = io.Copy(bufWriter, teeReader)
		if e != nil {
			closeWithWarn(r, "storage reader")
			return e
		}

		if e = bufWriter.Flush(); e != nil {
			closeWithWarn(r, "storage reader")
			return e
		}

		// Close gzRdr before r to maintain proper ownership order.
		if gzRdr != nil {
			if e = gzRdr.Close(); e != nil {
				closeWithWarn(r, "storage reader")
				return e
			}
		}

		return r.Close()
	})
	if err != nil {
		closeWithWarn(file, "output file")
		if removeErr := os.Remove(filePath); removeErr != nil {
			LogWarn.Println("Cannot delete file after error: " + removeErr.Error())
		}
		LogErr.Fatalln("Cannot read object from bucket! (" + err.Error() + ")")
	}

	downloadedCRC32C := crcHash.Sum32()

	if appFlag.ExtraChecks && objAttrs != nil {
		if compressed {
			origStr, ok := objAttrs.Metadata["x-original-crc32c"]
			if ok {
				origVal, parseErr := strconv.ParseUint(origStr, 10, 32)
				if parseErr == nil && uint32(origVal) == downloadedCRC32C {
					LogInfo.Printf("SUCCESS: Object downloaded and decompressed. (Written Bytes: %d, CRC32C verified: %d)", bytesWritten, downloadedCRC32C)
				} else if parseErr == nil {
					closeWithWarn(file, "output file")
					if removeErr := os.Remove(filePath); removeErr != nil {
						LogWarn.Println("Cannot delete file after CRC32C mismatch: " + removeErr.Error())
					}
					LogErr.Fatalf("CRC32C mismatch! File deleted. (Expected: %d, Got: %d, Written Bytes: %d)", uint32(origVal), downloadedCRC32C, bytesWritten)
				} else {
					LogWarn.Printf("Cannot parse original CRC32C from metadata. (Written Bytes: %d, Downloaded CRC32C: %d)", bytesWritten, downloadedCRC32C)
				}
			} else {
				LogInfo.Printf("SUCCESS: Object downloaded and decompressed. (Written Bytes: %d, CRC32C: %d, no original checksum in metadata to validate)", bytesWritten, downloadedCRC32C)
			}
		} else {
			if objAttrs.CRC32C == downloadedCRC32C {
				LogInfo.Printf("SUCCESS: Object downloaded. (Written Bytes: %d, CRC32C verified: %d)", bytesWritten, downloadedCRC32C)
			} else {
				closeWithWarn(file, "output file")
				if removeErr := os.Remove(filePath); removeErr != nil {
					LogWarn.Println("Cannot delete file after CRC32C mismatch: " + removeErr.Error())
				}
				LogErr.Fatalf("CRC32C mismatch! File deleted. (Expected: %d, Got: %d, Written Bytes: %d)", objAttrs.CRC32C, downloadedCRC32C, bytesWritten)
			}
		}
	} else {
		decompTag := ""
		if compressed {
			decompTag = " and decompressed"
		}
		LogInfo.Printf("SUCCESS: Object downloaded%s. (Written Bytes: %d, CRC32C: %d)", decompTag, bytesWritten, downloadedCRC32C)
	}

}
