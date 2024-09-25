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
import { Status } from '../enums/status';

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
      if (IsStaleProcess.result.isStale) {
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
      const zipValidation = await validateZipFile(bulkUploadMetadata.Contents);
      const {
        result: { isValidZip },
      } = zipValidation;
      if (!isValidZip) {
        const processUpdate = await updateProcess(Process_id, {
          error_status: zipValidation.error.errStatus,
          error_message: zipValidation.error.errMsg,
          status: Status.FAILED,
        });
        if (processUpdate.error) {
          logger.error(`Error while updating the process id ${Process_id} terminating whole process job`);
          return false;
        }
        continue;
      }
      const csvValidation = await validateCSVFilesFormatInZip();
      if (!csvValidation.result.isValid) {
        const processUpdate = await updateProcess(Process_id, {
          error_status: csvValidation.error.errStatus,
          error_message: csvValidation.error.errMsg,
          status: Status.FAILED,
        });
        if (processUpdate.error) {
          logger.error(`Error while updating the process id ${Process_id} terminating whole process job`);
          return false;
        }
        continue;
      }
      const handleCsv = await handleCSVEntries(csvValidation.result.data);
      if (!handleCsv.result.isValid) {
        const processUpdate = await updateProcess(Process_id, {
          error_status: handleCsv.error.errStatus,
          error_message: handleCsv.error.errMsg,
          status: Status.FAILED,
        });
        if (processUpdate.error) {
          logger.error(`Error while updating the process id ${Process_id} terminating whole process job`);
          return false;
        }
        continue;
      }
      await updateProcess(Process_id, { status: Status.COMPLETED });
      logger.info(`Completed:: ${Process_id} process validation for bulk upload question ,question set and content successfully inserted`);
    }
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation.Re upload file for new process';
    await updateProcess(Process_id, {
      status: Status.ERROR,
      error_status: 'errored',
      error_message: `Failed to retrieve metadata for process id: ${Process_id}. ${errorMsg}.Re upload file for new process`,
    });
    logger.error(errorMsg);
  }
};

const markStaleProcessesAsErrored = (created_at: Date): any => {
  const timeDifference = Math.floor((Date.now() - created_at.getTime()) / (1000 * 60 * 60));
  if (timeDifference > fileUploadInterval) {
    logger.error('Stale process:: The uploaded zip folder is empty, please ensure a valid upload file.');
    return {
      error: { errStatus: 'empty', errMsg: 'The uploaded zip folder is empty, please ensure a valid upload file.' },
      result: {
        isStale: true,
      },
    };
  }
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isStale: false,
    },
  };
};

const handleFailedProcess = async () => {
  const processesInfo = await getProcessMetaData({ status: Status.PROGRESS });
  const { getAllProcess } = processesInfo;
  for (const process of getAllProcess) {
    await updateProcess(Process_id, { status: Status.REOPEN });
    const { process_id, fileName, created_at } = process;
    FILENAME = fileName;
    Process_id = process_id;
    logger.info(`process reopened for ${process_id}`);
    const timeDifference = Math.floor((Date.now() - created_at.getTime()) / (1000 * 60 * 60));
    if (timeDifference > reCheckProcessInterval) {
      const isSuccessStageProcess = await checkStagingProcess();
      if (!isSuccessStageProcess) {
        await updateProcess(Process_id, { status: Status.ERROR, error_status: 'errored', error_message: 'The csv Data is invalid format or errored fields.Re upload file for new process' });
        logger.error(`Re-open process:: The csv Data is invalid format or errored fields for process id: ${process_id}`);
      } else {
        await updateProcess(Process_id, { status: Status.COMPLETED });
        logger.info(`Re-open process:: ${Process_id} Process completed successfully.`);
      }
    }
  }
};

const checkStagingProcess = async () => {
  const getAllQuestionStage = await questionStageMetaData({ process_id: Process_id });
  if (_.isEmpty(getAllQuestionStage)) {
    logger.info(`Re-open:: ${Process_id} ,the csv Data is invalid format or errored fields`);
    return { error: { errStatus: null, errMsg: null }, result: { validStageData: false, data: null } };
  } else {
    const getAllQuestionSetStage = await questionSetStageMetaData({ status: 'success', process_id: Process_id });
    if (_.isEmpty(getAllQuestionSetStage)) {
      logger.info(`Re-open:: ${Process_id} ,the csv Data is invalid format or errored fields`);
      return { error: { errStatus: null, errMsg: null }, result: { validStageData: false, data: null } };
    } else {
      const getAllContentStage = await contentStageMetaData({ status: 'success', process_id: Process_id });
      if (_.isEmpty(getAllContentStage)) {
        logger.info(`Re-open:: ${Process_id} ,the csv Data is invalid format or errored fields`);
        return { error: { errStatus: null, errMsg: null }, result: { validStageData: false, data: null } };
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
    logger.error(`Zip Format:: ${Process_id} Unsupported file format, please upload a ZIP file.`);
    return {
      error: { errStatus: 'unsupported_format', errMsg: 'The uploaded file is an unsupported format, please upload all CSV files inside a ZIP file.' },
      result: { isValidZip: false, data: null },
    };
  }
  await updateProcess(Process_id, { status: Status.PROGRESS, updated_by: 1 });
  logger.info(`Zip Format:: ${Process_id} having valid zip file.`);
  return { error: null, result: { isValidZip: true, data: null } };
};

const validateCSVFilesFormatInZip = async () => {
  try {
    logger.info(`Zip extract:: ${Process_id} initiated to fetch and extract ZIP entries...`);
    const ZipEntries = await fetchAndExtractZipEntries('upload', Process_id, FILENAME);

    if (!ZipEntries.result.isValid) {
      return {
        error: { errStatus: 'invalid Zip', errMsg: `The uploaded  ZIP folder file format is invalid` },
        result: { isValid: false, data: [] },
      };
    }
    const zipEntries = ZipEntries?.result?.data;

    logger.info('Zip extract:: Filtering ZIP entries from media entries.');
    mediaEntries = zipEntries?.filter((e: any) => !e.entryName.endsWith('.csv'));
    const csvZipEntries = zipEntries?.filter((e: any) => e.entryName.endsWith('.csv'));

    for (const entry of csvZipEntries) {
      if (entry.isDirectory && entry.entryName.includes('.csv')) {
        logger.error(`File Format:: ${Process_id} The uploaded ZIP folder file format is in valid`);
        return {
          error: { errStatus: 'unsupported_folder_type', errMsg: `The uploaded '${entry.entryName}' ZIP folder file format is invalid` },
          result: { isValid: false, data: [] },
        };
      }
      if (!csvFileName.includes(entry.entryName)) {
        logger.error(`File Format:: ${Process_id} The uploaded file '${entry.entryName}' is not a valid file name.`);
        return {
          error: { errStatus: 'unsupported_folder_type', errMsg: `The uploaded file '${entry.entryName}' is not a valid file name.` },
          result: { isValid: false, data: [] },
        };
      }
    }

    logger.info(`File Format:: ${Process_id} csv files are valid file name and format.`);
    return { error: { errStatus: null, errMsg: null }, result: { isValid: true, data: csvZipEntries } };
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation,please re upload the zip file for the new process';
    return { error: { errStatus: 'un-expected', errMsg: errorMsg }, result: { isValid: false, data: [] } };
  }
};

const handleCSVEntries = async (csvFilesEntries: { entryName: string }[]) => {
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
    if (!questionCsv.result.isValid) {
      logger.error('Question:: Error in question csv validation');
      return questionCsv;
    }

    logger.info(`Question Set Validate::csv Data validation initiated for question sets`);
    const questionSetCsv = await handleQuestionSetCsv(validData.questionSets, Process_id);
    if (!questionSetCsv) {
      logger.error('Question set:: Error in question set csv validation');
      return questionSetCsv;
    }

    logger.info(`Content Validate::csv Data validation initiated for contents`);
    const contentCsv = await handleContentCsv(validData.contents, mediaEntries, Process_id);
    if (!contentCsv.result.isValid) {
      logger.error('content:: Error in question csv validation');
      return contentCsv;
    }
    return { error: { errStatus: null, errMsg: null }, result: { isValid: true, data: null } };
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation csv data,please re upload the zip file for the new process';
    return { error: { errStatus: 'errored', errMsg: errorMsg }, result: { isValid: false, data: null } };
  }
};
