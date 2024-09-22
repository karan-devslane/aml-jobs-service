import { S3Client, GetObjectCommand, PutObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { appConfiguration } from '../config';
import logger from '../utils/logger';

const { bucketName, presignedUrlExpiry } = appConfiguration;

const s3Client = new S3Client({});

export const getFolderData = async (filePath: string) => {
  const command = new GetObjectCommand({
    Bucket: bucketName,
    Key: filePath,
  });
  const response = await s3Client.send(command);
  logger.info('stream object get from cloud');
  return response.Body;
};

export const uploadMediaFile = async (filesData: any, type: string) => {
  const fileName = filesData.entryName.split('/')[1];
  const command = new PutObjectCommand({
    Bucket: bucketName,
    Key: `media/${type}/${fileName}`,
    Body: filesData.getData(),
  });
  await s3Client.send(command);
  return { fileName: fileName, src: `media/${type}` };
};

export const uploadCsvFile = async (filesData: any, fileName: string) => {
  const command = new PutObjectCommand({
    Bucket: bucketName,
    Key: fileName,
    Body: filesData,
    ContentType: 'text/csv',
  });
  const fileUpload = await s3Client.send(command);
  return fileUpload;
};

export const getQuestionSignedUrl = async (folderName: string, fileName: string) => {
  try {
    const command = new GetObjectCommand({
      Bucket: bucketName,
      Key: `${folderName}/${fileName}`,
    });

    const url = await getSignedUrl(s3Client, command, {
      expiresIn: presignedUrlExpiry,
    });

    return { error: false, url, message: 'success' };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to generate URL for get' : '';
    return { error: true, message: errorMsg };
  }
};

export const getFolderMetaData = async (folderPath: string) => {
  try {
    const command = new ListObjectsV2Command({
      Bucket: bucketName,
      Prefix: folderPath,
    });

    const s3Objects = await s3Client.send(command);
    const { Contents } = s3Objects;
    return { error: false, Contents, message: 'success' };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get all folder' : '';
    return { error: true, message: errorMsg };
  }
};
