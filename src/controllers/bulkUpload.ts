import logger from '../utils/logger';
import * as _ from 'lodash';
import { getAWSFolderMetaData } from '../services/awsService';
import { getProcessMetaData, updateProcess } from '../services/process';
import path from 'path';
import { appConfiguration } from '../config';
import { contentStageMetaData } from '../services/contentStage';
import { questionStageMetaData } from '../services/questionStage';
import { questionSetStageMetaData } from '../services/questionSetStage';
import { fetchAndExtractZipEntries } from '../services/util';
import { handleQuestionCsv, stageDataToQuestion } from './question';
import { handleQuestionSetCsv, stageDataToQuestionSet } from './questionSet';
import { handleContentCsv, stageDataToContent } from './content';

const { csvFileName, fileUploadInterval, reCheckProcessInterval } = appConfiguration;
let FILENAME: string;
let Process_id: string;
let mediaEntries: any[];

export const bulkUploadProcess = async () => {
  await handleFailedProcess();
  const processesInfo = await getProcessMetaData({ status: 'open' });
  if (processesInfo.error) {
    logger.error('Error: An unexpected issue occurred while retrieving the open process.');
    return false;
  }
  const { getAllProcess } = processesInfo;
  try {
    for (const process of getAllProcess) {
      const { process_id, fileName, created_at } = process;
      logger.info(`initiate:: bulk upload job for process id :${process_id}.`);
      Process_id = process_id;
      const IsStaleProcess = await markStaleProcessesAsErrored(created_at);
      if (IsStaleProcess.data.isStale) {
        logger.info(`Stale:: Process ${Process_id} is stale, skipping.`);
        continue;
      }
      FILENAME = fileName;
      const bulkUploadMetadata = await getAWSFolderMetaData(`upload/${process_id}`);
      if (bulkUploadMetadata.error) {
        logger.error('Error: An unexpected problem arose while accessing the folder from the cloud.');
        continue;
      }
      logger.info(`initiate:: bulk upload folder validation for process id :${process_id}.`);
      const validateZip = await validateZipFile(bulkUploadMetadata.Contents);
      const {
        data: { isValid },
      } = validateZip;
      if (!isValid) {
        await updateProcess(Process_id, {
          error_status: validateZip.error.errorStatus,
          error_message: validateZip.error.errMsg,
          status: validateZip.error.status,
        });
        continue;
      }
      const validateCsv = await validateCSVFilesFormatInZip();
      const {
        data: { isValidCsv, csvZipEntries },
      } = validateCsv;
      if (!isValidCsv) {
        await updateProcess(Process_id, {
          error_status: validateCsv.error.errorStatus,
          error_message: validateCsv.error.errMsg,
          status: validateCsv.error.status,
        });
        continue;
      }
      const handleCsv = await handleCSVEntries(csvZipEntries);
      const {
        data: { validData },
      } = handleCsv;
      if (!validData) {
        await updateProcess(Process_id, {
          error_status: handleCsv.error.errorStatus,
          error_message: handleCsv.error.errMsg,
          status: handleCsv.error.status,
        });
        continue;
      }
      await updateProcess(Process_id, { status: 'completed' });
      logger.info(`Completed:: ${Process_id} process validation for bulk upload question ,question set and content successfully inserted`);
    }
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation.Re upload file for new process';
    await updateProcess(Process_id, {
      status: 'errored',
      error_status: 'errored',
      error_message: `Failed to retrieve metadata for process id: ${Process_id}. ${errorMsg}.Re upload file for new process`,
    });
    logger.error(errorMsg);
  }
};

const markStaleProcessesAsErrored = async (created_at: Date): Promise<any> => {
  const timeDifference = Math.floor((Date.now() - created_at.getTime()) / (1000 * 60 * 60));
  if (timeDifference > fileUploadInterval) {
    await updateProcess(Process_id, {
      error_status: 'empty',
      error_message: 'The uploaded zip folder is empty, please ensure a valid upload file.',
      status: 'failed',
    });
    logger.error('Stale process:: The uploaded zip folder is empty, please ensure a valid upload file.');
    return {
      error: 'error',
      result: {
        isStale: true,
      },
    };
  }
  return {
    error: null,
    result: {
      isStale: false,
    },
  };
};

const handleFailedProcess = async () => {
  const processesInfo = await getProcessMetaData({ status: 'progress' });
  const { getAllProcess } = processesInfo;
  for (const process of getAllProcess) {
    await updateProcess(Process_id, { status: 'reopen' });
    const { process_id, fileName, created_at } = process;
    FILENAME = fileName;
    Process_id = process_id;
    logger.info(`process reopened for ${process_id}`);
    const timeDifference = Math.floor((Date.now() - created_at.getTime()) / (1000 * 60 * 60));
    if (timeDifference > reCheckProcessInterval) {
      const isSuccessStageProcess = await checkStagingProcess();
      if (!isSuccessStageProcess) {
        await updateProcess(Process_id, { status: 'errored', error_status: 'errored', error_message: 'The csv Data is invalid format or errored fields.Re upload file for new process' });
        logger.error(`Re-open process:: The csv Data is invalid format or errored fields for process id: ${process_id}`);
      } else {
        await updateProcess(Process_id, { status: 'completed' });
        logger.info(`Re-open process:: ${Process_id} Process completed successfully.`);
      }
    }
  }
};

const checkStagingProcess = async () => {
  const getAllQuestionStage = await questionStageMetaData({ process_id: Process_id });
  if (_.isEmpty(getAllQuestionStage)) {
    logger.info(`Re-open:: ${Process_id} ,the csv Data is invalid format or errored fields`);
    return { error: 'error', result: { validStageData: false, data: null } };
  } else {
    const getAllQuestionSetStage = await questionSetStageMetaData({ status: 'success', process_id: Process_id });
    if (_.isEmpty(getAllQuestionSetStage)) {
      logger.info(`Re-open:: ${Process_id} ,the csv Data is invalid format or errored fields`);
      return { error: 'error', result: { validStageData: false, data: null } };
    } else {
      const getAllContentStage = await contentStageMetaData({ status: 'success', process_id: Process_id });
      if (_.isEmpty(getAllContentStage)) {
        logger.info(`Re-open:: ${Process_id} ,the csv Data is invalid format or errored fields`);
        return { error: 'error', result: { validStageData: false, data: null } };
      }
    }
  }
  await stageDataToQuestion();
  await stageDataToQuestionSet();
  await stageDataToContent();
  return { error: null, result: { validStageData: true, data: null } };
};

const validateZipFile = async (bulkUploadMetadata: any): Promise<any> => {
  const fileExt = path.extname(bulkUploadMetadata[0].Key || '').toLowerCase();
  if (fileExt !== '.zip') {
    await updateProcess(Process_id, {
      error_status: 'unsupported_format',
      error_message: 'The uploaded file is an unsupported format, please upload all CSV files inside a ZIP file.',
      status: 'failed',
    });
    logger.error(`Zip Format:: ${Process_id} Unsupported file format, please upload a ZIP file.`);
    return { error: 'Error', result: { isValidZip: false, data: null } };
  }
  await updateProcess(Process_id, { status: 'progress', updated_by: 1 });
  logger.info(`Zip Format:: ${Process_id} having valid zip file.`);
  return { error: null, result: { isValidZip: true, data: null } };
};

const validateCSVFilesFormatInZip = async (): Promise<any> => {
  try {
    logger.info(`Zip extract:: ${Process_id} initiated to fetch and extract ZIP entries...`);
    const ZipEntries = await fetchAndExtractZipEntries('upload', Process_id, FILENAME);

    logger.info('Zip extract:: Filtering ZIP entries from media entries.');
    mediaEntries = ZipEntries.filter((e) => !e.entryName.endsWith('.csv'));
    const csvZipEntries = ZipEntries.filter((e) => e.entryName.endsWith('.csv'));

    for (const entry of csvZipEntries) {
      if (entry.isDirectory && entry.entryName.includes('.csv')) {
        await updateProcess(Process_id, {
          error_status: 'unsupported_folder_type',
          error_message: `The uploaded '${entry.entryName}' ZIP folder file format is invalid`,
          status: 'failed',
        });
        logger.error(`File Format:: ${Process_id} The uploaded ZIP folder file format is in valid`);
        return { error: 'Error', result: { isValidCsv: false, data: null } };
      }
      if (!csvFileName.includes(entry.entryName)) {
        await updateProcess(Process_id, {
          error_status: 'unsupported_folder_type',
          error_message: `The uploaded file '${entry.entryName}' is not a valid file name.`,
          status: 'failed',
        });
        logger.error(`File Format:: ${Process_id} The uploaded file '${entry.entryName}' is not a valid file name.`);
        return { error: 'Error', result: { isValidCsv: false, data: null } };
      }
    }

    logger.info(`File Format:: ${Process_id} csv files are valid file name and format.`);
    return { error: null, result: { isValidCsv: true, data: csvZipEntries } };
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation,please re upload the zip file for the new process';
    return { error: errorMsg, result: { isValidCsv: false, data: null } };
  }
};

const handleCSVEntries = async (csvFilesEntries: { entryName: string }[]): Promise<any> => {
  try {
    const validData = {
      questions: [] as object[],
      questionSets: [] as object[],
      contents: [] as object[],
    };
    for (const entry of csvFilesEntries) {
      const checkKey = entry?.entryName.split('_')[1];
      switch (checkKey) {
        case 'question.csv':
          validData.questions.push(entry);
          break;

        case 'questionSet.csv':
          validData.questionSets.push(entry);
          break;

        case 'content.csv':
          validData.contents.push(entry);
          break;

        default:
          logger.error(`Unsupported sheet in file '${entry?.entryName}'.`);
          break;
      }
    }
    logger.info(`Question Validate::csv Data validation initiated for questions`);
    const questionCsv = await handleQuestionCsv(validData.questions, mediaEntries, Process_id);
    if (!questionCsv) {
      logger.error('Question:: Error in question csv validation');
      return { error: 'error', result: { validData: false, data: null } };
    }

    logger.info(`Question Set Validate::csv Data validation initiated for question sets`);
    const questionSetCsv = await handleQuestionSetCsv(validData.questionSets, Process_id);
    if (!questionSetCsv) {
      logger.error('Question:: Error in question csv validation');
      return { error: 'error', result: { validData: false, data: null } };
    }

    logger.info(`Content Validate::csv Data validation initiated for contents`);
    const contentCsv = await handleContentCsv(validData.contents, mediaEntries, Process_id);
    if (!contentCsv) {
      logger.error('Question:: Error in question csv validation');
      return { error: 'error', result: { validData: false, data: null } };
    }
    return { error: null, result: { validData: true, data: null } };
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation csv data,please re upload the zip file for the new process';
    return { error: errorMsg, result: { isValidCsv: false, data: null } };
  }
};
