/* eslint-disable @typescript-eslint/no-unused-vars */
import logger from '../../utils/logger';
import * as _ from 'lodash';
import { getAllCloudFolder, getQuestionSignedUrl, getTemplateSignedUrl } from '../../services/awsService';
import { getProcessByMetaData, updateProcess } from '../../services/process';
import path from 'path';
import AdmZip from 'adm-zip';
import { appConfiguration } from '../../config';

const { csvFileName } = appConfiguration;
export const scheduleJob = async () => {
  const processInfo = await getProcessByMetaData({ status: 'open' });
  const { getProcess } = processInfo;
  try {
    const validFileNames: string[] = csvFileName;
    for (const process of getProcess) {
      const { process_id, fileName } = process;
      const folderPath = `upload/${process_id}`;
      const s3Objects = await getAllCloudFolder(folderPath);
      if (isFolderEmpty(s3Objects)) {
        await markProcessAsFailed(process_id, 'is_empty', 'The uploaded zip folder is empty, please ensure a valid upload file.');
        continue;
      }
      const validZip = await validateZipFiles(process_id, s3Objects.Contents, folderPath, fileName, validFileNames);
      if (!validZip) {
        continue;
      }
    }
  } catch (error) {
    const code = _.get(error, 'code', 'UPLOAD_JOB_PROCESS');
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation.Re upload file for new process';
    await markProcessAsFailed(getProcess[0].dataValues.process_id, 'failed', 'Error during upload validation.Re upload file for new process');
    logger.error({ errorMsg, code });
  }
};

// Function to check if the folder is empty
const isFolderEmpty = (s3Objects: any): boolean => {
  return !s3Objects.Contents || _.isEmpty(s3Objects.Contents);
};

const validateZipFiles = async (process_id: string, s3Objects: any, folderPath: string, fileName: string, validFileNames: string[]): Promise<boolean> => {
  let isZipFile = true;
  try {
    const fileExt = path.extname(s3Objects[0].Key || '').toLowerCase();
    if (fileExt !== '.zip') {
      await markProcessAsFailed(process_id, 'is_unsupported_format', 'The uploaded file is an unsupported format, please upload all CSV files inside a ZIP file.');
      isZipFile = false;
    } else {
      await updateProcess(process_id, { status: 'progress', updated_by: 1 });
    }

    if (!isZipFile) return false;
    let mediaFiles;
    const questionZipEntries = await fetchAndExtractZipEntries('upload', folderPath, fileName);

    for (const entry of questionZipEntries) {
      if (entry.entryName === 'media/' && entry.isDirectory) {
        mediaFiles = entry;
        continue;
      }
      if (!entry.isDirectory && entry.entryName.includes('media/')) continue;
      if (entry.isDirectory && entry.entryName.includes('.csv')) {
        await markProcessAsFailed(process_id, 'is_unsupported_folder_type', 'The uploaded ZIP folder contains valid files with valid format. Follow the template format.');
        return false;
      }

      if (!validFileNames.includes(entry.entryName)) {
        await markProcessAsFailed(process_id, 'is_unsupported_file_name', `The uploaded file '${entry.entryName}' is not a valid file name.`);
        return false;
      }
      const validCSV = await validateCSVFormat(process_id, entry, fileName, mediaFiles);
      if (!validCSV) return false;
    }
    return true;
  } catch (error) {
    const code = _.get(error, 'code', 'UPLOAD_JOB_PROCESS');
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation,please re upload the zip file for the new process';
    logger.error({ errorMsg, code });
    await markProcessAsFailed(process_id, 'is failed', 'Error during upload validation,please re upload the zip file for the new process');
    return false;
  }
};

const validateCSVFormat = async (process_id: string, entry: any, fileName: string, mediaContent: any): Promise<boolean> => {
  try {
    const templateZipEntries = await fetchAndExtractZipEntries('template', '', fileName);
    const templateFileContent = templateZipEntries
      .find((t) => t.entryName === entry.entryName)
      ?.getData()
      .toString('utf8');

    if (!templateFileContent) {
      await markProcessAsFailed(process_id, 'invalid_template', `Template for '${entry.entryName}' not found.`);
      return false;
    }

    const [templateHeader] = templateFileContent.split('\n').map((row) => row.split(','));

    const questionFileContent = entry.getData().toString('utf8');
    const [Qheader, ...Qrows] = questionFileContent
      .split('\n')
      .map((row: string) => row.split(','))
      .filter((row: string[]) => row.some((cell) => cell.trim() !== ''));
    const checkKey = entry.entryName.split('_')[1];
    switch (checkKey) {
      case 'question.csv':
        validateQuestionCsv(entry.entryName, process_id, Qheader, Qrows, templateHeader, mediaContent);
        break;
      case 'questionSet.csv':
        validateQuestionSetCsv(entry.entryName, process_id, Qheader, Qrows, templateHeader);
        break;
      case 'content.csv':
        validateContentCsv(entry.entryName, process_id, Qheader, Qrows, templateHeader, mediaContent);
        return true;
      default:
        await markProcessAsFailed(process_id, 'unsupported_sheet', `Unsupported sheet in file '${entry.entryName}'.`);
    }
    return true;
  } catch (error) {
    const code = _.get(error, 'code', 'UPLOAD_QUESTION_CRON');
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation,please re upload the zip file for the new process';
    logger.error({ errorMsg, code });
    await markProcessAsFailed(process_id, 'is failed', errorMsg);
    return false;
  }
};

const fetchAndExtractZipEntries = async (folderName: string, folderPath: string, fileName: string): Promise<AdmZip.IZipEntry[]> => {
  try {
    let s3File;
    if (folderName === 'upload') {
      s3File = await getQuestionSignedUrl(folderPath, fileName);
    } else {
      s3File = await getTemplateSignedUrl(fileName);
    }
    if (!s3File.url) {
      throw new Error('Signed URL is missing or invalid');
    }
    const response = await fetch(s3File.url);

    if (!response.ok) {
      throw new Error('Network response was not ok');
    }

    const arrayBuffer = await response.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    const zip = new AdmZip(buffer);

    return zip.getEntries();
  } catch (error) {
    const code = _.get(error, 'code', 'UPLOAD_QUESTION_CRON');
    const errorMsg = error instanceof Error ? error.message : 'Error in the validation process,please re-upload the zip file for the new process';
    logger.error({ errorMsg, code });
    return [];
  }
};

const markProcessAsFailed = async (process_id: string, error_status: string, error_message: string) => {
  await updateProcess(process_id, {
    error_status,
    error_message,
    status: 'failed',
  });
};

function validateQuestionCsv(entryName: any, process_id: string, Qheader: any, Qrows: any, templateHeader: string[], mediaContent: any) {
  throw new Error('Function not implemented.');
}

function validateQuestionSetCsv(entryName: any, process_id: string, Qheader: any, Qrows: any, templateHeader: string[]) {
  throw new Error('Function not implemented.');
}

function validateContentCsv(entryName: any, process_id: string, Qheader: any, Qrows: any, templateHeader: string[], mediaContent: any) {
  throw new Error('Function not implemented.');
}
