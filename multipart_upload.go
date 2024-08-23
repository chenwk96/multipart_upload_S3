package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	maxPartSize        = int64(5 * 1024 * 1024)
	maxRetries         = 3
	awsAccessKeyID     = "ak"
	awsSecretAccessKey = "sk"
	awsEndpoint        = "endpoint"
	awsBucketName      = "aaaa"
	awsBucketRegion    = "sh"
	srcFileName        = "file.test"
)

func main() {
	creds := credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, "")

	httpClient := &http.Client{
		Timeout: time.Second * time.Duration(30),
	}

	session, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(awsEndpoint),
		Region:           aws.String(awsBucketRegion),
		Credentials:      creds,
		S3ForcePathStyle: aws.Bool(true),
		HTTPClient:       httpClient,
	})

	if err != nil {
		fmt.Printf("Failed to create AWS session: %v", err)
		return
	}

	s3Client := s3.New(session)

	prefix := "/"
	delimiter := "/"
	output, err := s3Client.ListObjects(&s3.ListObjectsInput{
		Bucket:    aws.String(awsBucketName),
		Prefix:    &prefix,
		Delimiter: &delimiter,
	})

	if err != nil {
		fmt.Printf("Failed to ListObjects: %v", err)
		return
	}

	fmt.Printf("listobject: %v \n", output)

	file, err := os.Open(srcFileName)
	if err != nil {
		fmt.Printf("Failed to open file[%s]: %v", srcFileName, err)
		return
	}
	defer file.Close()

	fileInfo, _ := file.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)
	fileType := http.DetectContentType(buffer)
	file.Read(buffer)

	path := genRandonString(1024)
	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(awsBucketName),
		Key:         aws.String(path),
		ContentType: aws.String(fileType),
	}

	resp, err := s3Client.CreateMultipartUpload(input)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Created multipart upload request")

	var completedParts []*s3.CompletedPart
	partNumber := 1
	for partNumber <= 1401 {
		completedPart, err := uploadPart(s3Client, resp, buffer, partNumber)
		if err != nil {
			fmt.Println(err.Error())
			err := abortMultipartUpload(s3Client, resp)
			if err != nil {
				fmt.Println(err.Error())
			}
			return
		}
		partNumber++
		completedParts = append(completedParts, completedPart)
	}

	completeResponse, err := completeMultipartUpload(s3Client, resp, completedParts)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Printf("Successfully uploaded file: %s\n", completeResponse.String())
}

func completeMultipartUpload(s3Client *s3.S3, resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return s3Client.CompleteMultipartUpload(completeInput)
}

func uploadPart(s3Client *s3.S3, resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber int) (*s3.CompletedPart, error) {
	tryNum := 1
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	for tryNum <= maxRetries {
		uploadResult, err := s3Client.UploadPart(partInput)
		if err != nil {
			if tryNum == maxRetries {
				if aerr, ok := err.(awserr.Error); ok {
					return nil, aerr
				}
				return nil, err
			}
			fmt.Printf("Retrying to upload part #%v\n", partNumber)
			tryNum++
		} else {
			fmt.Printf("Uploaded part #%v\n", partNumber)
			return &s3.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: aws.Int64(int64(partNumber)),
			}, nil
		}
	}
	return nil, nil
}

func abortMultipartUpload(s3Client *s3.S3, resp *s3.CreateMultipartUploadOutput) error {
	fmt.Println("Aborting multipart upload for UploadId#" + *resp.UploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := s3Client.AbortMultipartUpload(abortInput)
	return err
}

func genRandonString(length int) string {
	str := "1234567890qwertyuiopasadfghjklzxcvbnm"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < length; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}

	return string(result)
}
