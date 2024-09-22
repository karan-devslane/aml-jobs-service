import logger from '../../utils/logger';
import * as _ from 'lodash';
import * as uuid from 'uuid';
import { getFolderMetaData, getFolderData, uploadMediaFile, uploadCsvFile } from '../../services/awsService';
import { getProcessByMetaData, updateProcess } from '../../services/process';
import path from 'path';
import AdmZip from 'adm-zip';
import { appConfiguration } from '../../config';
import { contentStageMetaData, createContentSage, getAllStageContent, updateContentStage } from '../../services/contentStage';
import { createQuestionStage, getAllStageQuestion, questionStageMetaData, updateQuestionStage } from '../../services/questionStage';
import { createQuestionSetStage, getAllStageQuestionSet, questionSetStageMetaData, updateQuestionStageSet } from '../../services/questionSetStage';
import { createContent } from '../../services/content ';
import { QuestionStage } from '../../models/questionStage';
import { QuestionSetStage } from '../../models/questionSetStage';
import { ContentStage } from '../../models/contentStage';
import { createQuestion } from '../../services/question';
import { getBoards, getClasses, getRepository, getSkills, getSubSkills, getTenants } from '../../services/service';
import { createQuestionSet } from '../../services/questionSet';
import { Parser } from '@json2csv/plainjs';

const { csvFileName, fileUploadInterval, reCheckProcessInterval, grid1AddFields, grid1DivFields, grid1MultipleFields, grid1SubFields, grid2Fields, mcqFields, fibFields } = appConfiguration;
let FILENAME: string;
let Process_id: string;
let mediaEntries: any[];

export const scheduleJob = async () => {
  await handleFailedProcess();
  const processesInfo = await getProcessByMetaData({ status: 'open' });
  const { getAllProcess } = processesInfo;
  try {
    for (const process of getAllProcess) {
      const { process_id, fileName, created_at } = process;
      logger.info(`initiate:: bulk upload job for process id :${process_id}.`);
      Process_id = process_id;
      const IsStaleProcess = await markStaleProcessesAsErrored(created_at);
      if (IsStaleProcess) {
        logger.info(`Stale:: Process ${Process_id} is stale, skipping.`);
        continue;
      }
      FILENAME = fileName;
      const folderPath = `upload/${process_id}`;
      const bulkUploadMetadata = await getFolderMetaData(folderPath);

      logger.info(`initiate:: bulk upload folder validation for process id :${process_id}.`);
      await validateZipFile(bulkUploadMetadata);
    }
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation.Re upload file for new process';
    await updateProcess(Process_id, {
      status: 'errored',
      error_status: 'errored',
      error_message: `Failed to retrieve metadata for process id: ${Process_id}. ${errorMsg}.Re upload file for new process`,
    });
    logger.error(errorMsg);
    throw new Error(`Failed to retrieve metadata for process id: ${Process_id}. ${errorMsg}`);
  }
};

const markStaleProcessesAsErrored = async (created_at: Date): Promise<boolean> => {
  const timeDifference = Math.floor((Date.now() - created_at.getTime()) / (1000 * 60 * 60));
  if (timeDifference > fileUploadInterval) {
    await updateProcess(Process_id, {
      error_status: 'empty',
      error_message: 'The uploaded zip folder is empty, please ensure a valid upload file.',
      status: 'failed',
    });
    logger.error('Stale process:: The uploaded zip folder is empty, please ensure a valid upload file.');
    return true;
  }
  return false;
};

const handleFailedProcess = async () => {
  const processesInfo = await getProcessByMetaData({ status: 'progress' });
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
    return false;
  } else {
    const getAllQuestionSetStage = await questionSetStageMetaData({ status: 'success', process_id: Process_id });
    if (_.isEmpty(getAllQuestionSetStage)) {
      logger.info(`Re-open:: ${Process_id} ,the csv Data is invalid format or errored fields`);
      return false;
    } else {
      const getAllContentStage = await contentStageMetaData({ status: 'success', process_id: Process_id });
      if (_.isEmpty(getAllContentStage)) {
        logger.info(`Re-open:: ${Process_id} ,the csv Data is invalid format or errored fields`);
        return false;
      }
    }
  }
  await insertStageDataToQuestionTable();
  await insertStageDataToQuestionSetTable();
  await insertStageDataToContentTable();
  return true;
};

const validateZipFile = async (bulkUploadMetadata: any): Promise<any> => {
  const fileExt = path.extname(bulkUploadMetadata.Contents[0].Key || '').toLowerCase();
  if (fileExt !== '.zip') {
    await updateProcess(Process_id, {
      error_status: 'unsupported_format',
      error_message: 'The uploaded file is an unsupported format, please upload all CSV files inside a ZIP file.',
      status: 'failed',
    });
    logger.error(`Zip Format:: ${Process_id} Unsupported file format, please upload a ZIP file.`);
    return false;
  }
  await updateProcess(Process_id, { status: 'open', updated_by: 1 });
  logger.info(`Zip Format:: ${Process_id} Valid ZIP file, moving to next process`);
  await validateCSVFilesFormatInZip();
  return true;
};

const validateCSVFilesFormatInZip = async (): Promise<boolean> => {
  try {
    logger.info(`Zip extract:: ${Process_id} initiated to fetch and extract ZIP entries...`);
    const ZipEntries = await fetchAndExtractZipEntries('upload');

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
        return false;
      }
      if (!csvFileName.includes(entry.entryName)) {
        await updateProcess(Process_id, {
          error_status: 'unsupported_folder_type',
          error_message: `The uploaded file '${entry.entryName}' is not a valid file name.`,
          status: 'failed',
        });
        logger.error(`File Format:: ${Process_id} The uploaded file '${entry.entryName}' is not a valid file name.`);
        return false;
      }
    }

    logger.info(`File Format:: ${Process_id} csv files are valid file name and format`);
    await handleCSVEntries(csvZipEntries);
    return true;
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation,please re upload the zip file for the new process';
    logger.error(errorMsg);
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: errorMsg,
      status: 'errored',
    });
    throw new Error(`Unexpected Error occurred,${errorMsg}`);
  }
};

const handleCSVEntries = async (csvFilesEntries: any): Promise<any> => {
  try {
    for (const entry of csvFilesEntries) {
      const checkKey = entry.entryName.split('_')[1];
      switch (checkKey) {
        case 'question.csv': {
          logger.info(`Handle question csv:: started Data validation for ${entry.entryName}`);
          const isValidQuestionData = await validateCSVQuestionHeaderRow(entry);
          if (!isValidQuestionData) {
            logger.error(`Validation:: failed for ${entry.entryName}`);
            return false;
          }
          break;
        }
        case 'questionSet.csv': {
          logger.info(`Handle question_set csv:: started Data validation for ${entry.entryName}`);
          const isValidQuestionSetData = await validateCSVQuestionSetHeaderRow(entry);
          if (!isValidQuestionSetData) {
            logger.error(`Validation:: failed for ${entry.entryName}`);
            return false;
          }
          break;
        }
        case 'content.csv': {
          logger.info(`Handle content csv:: started Data validation for ${entry.entryName}`);
          const isValidQuestionContentData = await validateCSVContentHeaderRow(entry);
          if (!isValidQuestionContentData) {
            logger.error(`Validation:: failed for ${entry.entryName}`);
            return false;
          }
          break;
        }
        default: {
          await updateProcess(Process_id, {
            error_status: 'unsupported_sheet',
            error_message: `Unsupported sheet in file '${entry.entryName}'.`,
            status: 'failed',
          });
          logger.error(`Unsupported sheet in file '${entry.entryName}'.`);
        }
      }
    }
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation csv data,please re upload the zip file for the new process';
    logger.error(errorMsg);
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: errorMsg,
      status: 'errored',
    });
    throw new Error(`Unexpected Error occurred,${errorMsg}`);
  }
};

const validateCSVQuestionHeaderRow = async (questionEntry: any) => {
  const templateHeader = await getCSVTemplateHeader(questionEntry.entryName);
  const { header, rows } = getCSVHeaderAndRow(questionEntry);
  if (!templateHeader && !header && !rows) {
    logger.error('Question Row/header::Template header, header, or rows are missing');
    return false;
  }

  const isValidHeader = await validHeader(questionEntry.entryName, header, templateHeader);
  if (!isValidHeader) {
    logger.error('Question Row/header::Header validation failed');
    return false;
  }

  logger.info(`Question Row/header::Row and Header mapping process started for ${Process_id} `);
  await questionRowHeaderProcess(rows, header);
  return true;
};

const questionRowHeaderProcess = async (rows: any, header: any) => {
  const processData = processRow(rows, header);
  if (!processData || processData.length === 0) {
    logger.error('Question Row/header:: Row processing failed or returned empty data');
    await updateProcess(Process_id, {
      error_status: 'process_error',
      error_message: 'Question Row/header:: Row processing failed or returned empty data',
      status: 'errored',
    });
    return false;
  }
  logger.info('Question Row/header:: header and row process successfully and process 2 started');

  const updatedProcessData = ProcessStageDataQuestion(processData);
  if (!updatedProcessData || updatedProcessData.length === 0) {
    logger.error('Question Row/header:: Stage 2 data processing failed or returned empty data');
    await updateProcess(Process_id, {
      error_status: 'process_stage_error',
      error_message: 'Question Row/header:: Stage 2 data processing failed or returned empty data',
      status: 'errored',
    });
    return false;
  }

  logger.info('Insert question Stage::Questions Data ready for bulk insert');
  await insertBulkQuestionStage(updatedProcessData);
};

const insertBulkQuestionStage = async (insertData: any) => {
  const stageProcessData = await insertProcessDataToQuestionStaging(insertData);
  if (!stageProcessData) {
    logger.error('Insert question stage:: Failed to insert process data into staging');
    await updateProcess(Process_id, {
      error_status: 'staging_insert_error',
      error_message: 'Failed to insert process data into staging',
      status: 'errored',
    });
    return false;
  }

  logger.info(`Validate question Stage::Staged questions Data ready for validation`);
  await ValidateQuestionsStage();
};

const ValidateQuestionsStage = async () => {
  const stageProcessValidData = await validateQuestionStageData();
  if (!stageProcessValidData) {
    logger.error(`Question:: ${Process_id} staging data are invalid`);
    await updateProcess(Process_id, {
      error_status: 'staging_validation_error',
      error_message: `Question staging data are invalid.correct the error and re upload new csv for fresh process`,
      status: 'errored',
    });
    return false;
  }

  await updateProcess(Process_id, { fileName: 'questions.csv', status: 'validated' });
  logger.info(`Question Media upload:: ${Process_id} question Stage data is ready for upload media `);
  await QuestionMediaProcess();

  // logger.info(`Upload Cloud::Staging Data ready for upload in cloud`);
  // await uploadQuestionsStageData();
};

const uploadQuestionsStageData = async () => {
  const getQuestions = await getAllStageQuestion();
  const uploadQuestion = await convertToCSV(getQuestions, 'questions.csv');
  if (!uploadQuestion) {
    logger.error('Upload Cloud::Unexpected error occurred while upload to cloud');
    return false;
  }
  logger.info('Question Upload Cloud::All the question are validated and uploaded in the cloud for reference');
};

const QuestionMediaProcess = async () => {
  const getQuestions = await getAllStageQuestion();
  for (const question of getQuestions) {
    if (question.media_files?.length > 0) {
      const mediaFiles = await Promise.all(
        question.media_files.map(async (o: string) => {
          const foundMedia = mediaEntries.slice(1).find((media: any) => {
            return media.entryName.split('/')[1] === o;
          });
          if (foundMedia) {
            const mediaData = await uploadMediaFile(foundMedia, 'question');
            return mediaData;
          }
          return null;
        }),
      );
      if (mediaFiles[0] !== null && mediaFiles.length > 0) {
        const updateContent = await updateQuestionStage({ id: question.id }, { media_files: mediaFiles });
        if (updateContent.error) {
          logger.error('Question Media upload:: Media validation or update failed');
          await updateProcess(Process_id, {
            error_status: 'media_validation_error',
            error_message: ' Media validation or update failed',
            status: 'errored',
          });
          return false;
        }
      }
    }
  }
  logger.info('Question Media upload:: Media inserted and updated in the process data');
  logger.info(`Bulk Insert::${Process_id} is Ready for inserting bulk upload to question`);
  await insertQuestionsMain();
};

const insertQuestionsMain = async () => {
  const insertToMainQuestionSet = await insertStageDataToQuestionTable();
  if (!insertToMainQuestionSet) {
    logger.error(`Question Bulk Insert:: ${Process_id} staging data are invalid for main question insert`);
    await updateProcess(Process_id, {
      error_status: 'main_insert_error',
      error_message: `Bulk Insert staging data are invalid to format main question insert`,
      status: 'errored',
    });
    return false;
  }

  logger.info(`Question Bulk insert:: bulk upload completed  for Process ID: ${Process_id}`);
  await updateProcess(Process_id, { status: 'completed' });
  await QuestionStage.truncate({ restartIdentity: true });
  logger.info(`Completed:: ${Process_id} Question csv uploaded successfully`);
  return true;
};

const validateCSVQuestionSetHeaderRow = async (questionSetEntry: any) => {
  const templateHeader = await getCSVTemplateHeader(questionSetEntry.entryName);
  const { header, rows } = getCSVHeaderAndRow(questionSetEntry);
  if (!templateHeader && !header && !rows) {
    logger.error('Question Set Row/header:: Template header, header, or rows are missing');
    return false;
  }

  const isValidHeader = await validHeader(questionSetEntry.entryName, header, templateHeader);
  if (!isValidHeader) {
    logger.error('Question Set Row/header:: Header validation failed');
    return false;
  }

  logger.info(`Question Set Row/header:: Row and Header mapping process started for ${Process_id} `);
  await questionSetRowHeaderProcess(rows, header);
  return true;
};

const questionSetRowHeaderProcess = async (rows: any, header: any) => {
  const processData = processRow(rows, header);
  if (!processData || processData.length === 0) {
    logger.error('Question Set Row/header:: Row processing failed or returned empty data');
    await updateProcess(Process_id, {
      error_status: 'process_error',
      error_message: 'Question Set Row/header::Row processing failed or returned empty data',
      status: 'errored',
    });
    return false;
  }
  logger.info('Insert Question Set Stage::Question sets  Data ready for bulk insert');
  await insertBulkQuestionSetStage(processData);
};

const insertBulkQuestionSetStage = async (questionSetData: any) => {
  const stageProcessData = await insertProcessDataToQuestionSetStaging(questionSetData);
  if (!stageProcessData) {
    logger.error('Insert Question Set Stage::  Failed to insert process data into staging');
    await updateProcess(Process_id, {
      error_status: 'staging_insert_error',
      error_message: 'Insert Question Set Stage:: Failed to insert process data into staging',
      status: 'errored',
    });
    return false;
  }

  logger.info(`Validate question set Stage::question sets Data ready for validation process`);
  await ValidateQuestionSetsStage();
};

const ValidateQuestionSetsStage = async () => {
  const stageProcessValidData = await validateQuestionSetStageData();
  if (!stageProcessValidData) {
    logger.error(`Validate question set Stage:: ${Process_id} staging data are invalid`);
    await updateProcess(Process_id, {
      error_status: 'staging_validation_error',
      error_message: `Validate question set Stage:: ${Process_id} staging data are invalid`,
      status: 'errored',
    });
    return false;
  }

  await updateProcess(Process_id, { status: 'validated' });
  logger.info(`Question set Media upload:: ${Process_id}  Stage data is ready for upload media to cloud`);
  // await questionSetMediaProcess();
  await insertQuestionSetMain();

  // logger.info(`Upload Cloud::Staging Data ready for upload in cloud`);
  // await uploadQuestionSetsStageData();
};

const questionSetMediaProcess = async () => {
  const getQuestionSets = await getAllStageQuestionSet();
  for (const questionSet of getQuestionSets) {
    if (questionSet.media_files?.length > 0) {
      const mediaFiles = await Promise.all(
        questionSet.media_files.map(async (o: string) => {
          const foundMedia = mediaEntries.slice(1).find((media: any) => {
            return media.entryName.split('/')[1] === o;
          });
          if (foundMedia) {
            const mediaData = await uploadMediaFile(foundMedia, 'questionSet');
            return mediaData;
          }
          return null;
        }),
      );
      if (mediaFiles[0] !== null && mediaFiles.length > 0) {
        const updateContent = await updateQuestionStageSet({ id: questionSet.id }, { media_files: mediaFiles });
        if (updateContent.error) {
          logger.error('Question Set:: Media validation or update failed');
          await updateProcess(Process_id, {
            error_status: 'media_validation_error',
            error_message: ' Media validation or update failed',
            status: 'errored',
          });
          return false;
        }
      }
    }
  }
  logger.info('Question set question Set:: Media inserted and updated in the process data');
  logger.info(`question set Bulk Insert::${Process_id} is Ready for inserting bulk upload to question set`);
  await insertQuestionsMain();
};

const uploadQuestionSetsStageData = async () => {
  const questionSets = await getAllStageQuestionSet();
  await convertToCSV(questionSets, 'question_sets.csv');
  await updateProcess(Process_id, { fileName: 'question_sets.csv', status: 'validated' });
  logger.info('Question set:: all the data are validated successfully and uploaded to cloud for reference');

  logger.info('Question:: Media inserted and updated in the process data');
  logger.info(`Question set Bulk Insert::${Process_id} is Ready for inserting bulk upload to question`);
  await insertQuestionSetMain();
};

const insertQuestionSetMain = async () => {
  const insertToMainQuestionSet = await insertStageDataToQuestionSetTable();
  if (!insertToMainQuestionSet) {
    logger.error(`Question set bulk insert:: ${Process_id} staging data are invalid for main question set insert`);
    await updateProcess(Process_id, {
      error_status: 'main_insert_error',
      error_message: `Question set staging data are invalid for main question set insert`,
      status: 'errored',
    });
    return false;
  }

  await updateProcess(Process_id, { status: 'completed' });
  await QuestionSetStage.truncate({ restartIdentity: true });
  logger.info(`Question set bulk upload:: completed successfully and question_sets.csv file upload to cloud for Process ID: ${Process_id}`);
  return true;
};

const validateCSVContentHeaderRow = async (contentEntry: any) => {
  const templateHeader = await getCSVTemplateHeader(contentEntry.entryName);
  const { header, rows } = getCSVHeaderAndRow(contentEntry);
  if (!templateHeader && !header && !rows) {
    logger.error('Content Row/Header:: Template header, header, or rows are missing');
    return false;
  }

  const isValidHeader = await validHeader(contentEntry.entryName, header, templateHeader);
  if (!isValidHeader) {
    logger.error('Content Row/Header:: Header validation failed');
    return false;
  }

  logger.info(`content Row/Header:: Row and Header mapping process started for ${Process_id} `);
  await contentRowHeaderProcess(rows, header);
  return true;
};

const contentRowHeaderProcess = async (rows: any, header: any) => {
  const processData = processRow(rows, header);
  if (!processData || processData.length === 0) {
    logger.error('Content Row/Header:: Row processing failed or returned empty data');
    await updateProcess(Process_id, {
      error_status: 'process_error',
      error_message: 'Content Row/Header:: Row processing failed or returned empty data',
      status: 'errored',
    });
    return false;
  }
  logger.info('Insert content Stage:: Data ready for bulk insert to staging.');
  await insertBulkContentStage(processData);
};

const insertBulkContentStage = async (insertData: object[]) => {
  const stageProcessData = await insertProcessDataToContentStaging(insertData);
  if (!stageProcessData) {
    logger.error('Insert content Stage:: Failed to insert process data into staging');
    await updateProcess(Process_id, {
      error_status: 'staging_insert_error',
      error_message: 'Content Failed to insert process data into staging',
      status: 'errored',
    });
    return false;
  }

  logger.info(`Validate Content Stage::Staged contents Data ready for validation`);
  await ValidateContentsStage();
};

const ValidateContentsStage = async () => {
  const stageProcessValidData = await validateContentStageData();
  if (!stageProcessValidData) {
    logger.error(`Validate Content Stage:: ${Process_id} staging data are invalid`);
    await updateProcess(Process_id, {
      error_status: 'staging_validation_error',
      error_message: `Content staging data are invalid`,
      status: 'errored',
    });
    return false;
  }

  await updateProcess(Process_id, { fileName: 'contents.csv', status: 'validated' });
  logger.info(`Content Media upload:: ${Process_id} content Stage data is ready for upload media to cloud`);
  await contentsMediaProcess();

  //  logger.info(`Upload Cloud::Staging Data ready for upload in cloud`);
  // await uploadContentsStageData();
};

const contentsMediaProcess = async () => {
  const getContents = await getAllStageContent();
  for (const content of getContents) {
    if (content.media_files?.length > 0) {
      const mediaFiles = await Promise.all(
        content.media_files.map(async (o: string) => {
          const foundMedia = mediaEntries.slice(1).find((media: any) => {
            return media.entryName.split('/')[1] === o;
          });
          if (foundMedia) {
            const mediaData = await uploadMediaFile(foundMedia, 'content');
            return mediaData;
          }
          return null;
        }),
      );
      if (mediaFiles[0] !== null && mediaFiles.length > 0) {
        const updateContent = await updateContentStage({ id: content.id }, { media_files: mediaFiles });
        if (updateContent.error) {
          logger.error('Content Media upload:: Media validation or update failed');
          await updateProcess(Process_id, {
            error_status: 'media_validation_error',
            error_message: 'Content Media validation or update failed',
            status: 'errored',
          });
          return false;
        }
      }
    }
  }
  logger.info('Content Media upload:: Media inserted and updated in the stage table');
  logger.info(`Content Main Insert::${Process_id} is Ready for inserting bulk upload to question`);
  await insertContentsMain();
  return true;
};

const uploadContentsStageData = async () => {
  const getContents = await getAllStageContent();
  await convertToCSV(getContents, 'contents');
  logger.info('Content csv upload:: all the data are validated successfully and uploaded to cloud for reference');
};

const insertContentsMain = async () => {
  const insertToMainContent = await insertStageDataToContentTable();
  if (!insertToMainContent) {
    logger.error(`Content Main Insert::${Process_id} staging data are invalid for main question insert`);
    await updateProcess(Process_id, {
      error_status: 'main_insert_error',
      error_message: `Content staging data are invalid for main question insert`,
      status: 'errored',
    });
    return false;
  }

  logger.info(`Content Main insert:: bulk upload completed  for Process ID: ${Process_id}`);
  await updateProcess(Process_id, { status: 'completed' });
  await ContentStage.truncate({ restartIdentity: true });
  logger.info(`Completed:: ${Process_id} Content csv uploaded successfully`);
  return true;
};

const insertProcessDataToQuestionStaging = async (insertData: object[]) => {
  const contentStageProcessData = await createQuestionStage(insertData);
  if (!contentStageProcessData) {
    logger.error(`Insert Staging:: ${Process_id} question bulk data error in inserting`);
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: ' question bulk data error in inserting',
      status: 'errored',
    });
    return false;
  }
  logger.info(`Insert Question Staging:: ${Process_id} question bulk data inserted successfully to staging table`);
  return true;
};

const insertProcessDataToQuestionSetStaging = async (insertData: object[]) => {
  const contentStageProcessData = await createQuestionSetStage(insertData);
  if (!contentStageProcessData) {
    logger.error(`Insert Question SetStaging:: ${Process_id} question set  bulk data error in inserting`);
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: ' question set bulk data error in inserting',
      status: 'errored',
    });
    return false;
  }
  logger.info(`Insert Question Set Staging:: ${Process_id} question set bulk data inserted successfully to staging table `);
  return true;
};

const insertProcessDataToContentStaging = async (insertData: object[]) => {
  const contentStageProcessData = await createContentSage(insertData);
  if (!contentStageProcessData) {
    logger.error(`Insert Content Staging:: ${Process_id} content bulk data error in inserting`);
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: ' content bulk data error in inserting',
      status: 'errored',
    });
    return false;
  }
  logger.info(`Insert Content Staging:: ${Process_id} content bulk data inserted successfully to staging table `);
  return true;
};

const validateQuestionStageData = async () => {
  const getAllQuestionStage = await questionStageMetaData({ process_id: Process_id });
  let isUnique, isValid;
  if (_.isEmpty(getAllQuestionStage)) {
    logger.error(`Validate Question Stage:: ${Process_id} ,the csv Data is invalid format or errored fields`);
    return false;
  }
  for (const question of getAllQuestionStage) {
    const { id, question_id, question_set_id, question_type, L1_skill, body } = question;
    const checkRecord = await questionStageMetaData({ question_id, question_set_id, L1_skill, question_type });
    if (checkRecord.length > 1) {
      await updateQuestionStage(
        { id },
        {
          status: 'errored',
          error_info: 'Duplicate question and question_set_id combination found.',
        },
      );
      isUnique = false;
    } else {
      isUnique = true;
    }
    let requiredFields: string[] = [];
    const caseKey = question_type === 'Grid-1' ? `${question_type}_${L1_skill}` : question_type;
    switch (caseKey) {
      case `Grid-1_add`:
        requiredFields = grid1AddFields;
        break;
      case `Grid-1_sub`:
        requiredFields = grid1SubFields;
        break;
      case `Grid-1_multiple`:
        requiredFields = grid1MultipleFields;
        break;
      case `Grid-1_division`:
        requiredFields = grid1DivFields;
        break;
      case `Grid-2`:
        requiredFields = grid2Fields;
        break;
      case `mcq`:
        requiredFields = mcqFields;
        break;
      case `fib`:
        requiredFields = fibFields;
        break;
      default:
        requiredFields = [];
        break;
    }
    if (!requiredFields.every((field) => body[field] !== undefined && body[field] !== null && body[field] !== '')) {
      await updateQuestionStage(
        { id },
        {
          status: 'errored',
          error_info: `Missing required data for type ${question_type},fields are  ${requiredFields.join(', ')}`,
        },
      );
      isValid = false;
    } else {
      isValid = true;
    }
  }
  logger.info(`Validate Question Stage::${Process_id} , everything in the Question stage Data valid.`);
  return isUnique && isValid;
};

const validateQuestionSetStageData = async () => {
  const getAllQuestionSetStage = await questionSetStageMetaData({ process_id: Process_id });
  let isValid = true;
  if (_.isEmpty(getAllQuestionSetStage)) {
    logger.info(`Validate Question set Stage:: ${Process_id} ,the staging Data is empty invalid format or errored fields`);
    return false;
  }
  for (const questionSet of getAllQuestionSetStage) {
    const { id, question_set_id, L1_skill } = questionSet;
    const checkRecord = await questionSetStageMetaData({ question_set_id, L1_skill });
    if (checkRecord.length > 1) {
      await updateQuestionStage(
        { id },
        {
          status: 'errored',
          error_info: 'Duplicate question_set_id found.',
        },
      );

      return false;
    }
    isValid = true;
  }
  logger.info(`Validate Question set Stage:: ${Process_id} , the staging Data question set is valid`);
  return isValid;
};

const validateContentStageData = async () => {
  const getAllContentStage = await contentStageMetaData({ process_id: Process_id });
  let isValid = true;
  if (_.isEmpty(getAllContentStage)) {
    logger.error(`Validate Content Stage:: ${Process_id} ,the csv Data is invalid format or errored fields`);
    return false;
  }
  for (const content of getAllContentStage) {
    const { id, content_id, L1_skill } = content;
    const checkRecord = await contentStageMetaData({ content_id, L1_skill });
    if (checkRecord.length > 1) {
      await updateQuestionStage(
        { id },
        {
          status: 'errored',
          error_info: 'Duplicate content_id found.',
        },
      );
      return false;
    }
    isValid = true;
  }

  logger.info(`Validate Content Stage:: ${Process_id} , the staging Data content is valid`);
  return isValid;
};

const insertStageDataToQuestionTable = async () => {
  const getAllQuestionStage = await questionStageMetaData({ process_id: Process_id });
  const insertData = await formatQuestionStageData(getAllQuestionStage);
  if (!insertData) {
    await updateProcess(Process_id, {
      error_status: 'process_stage_data',
      error_message: ' Error in formatting staging data to min table.',
      status: 'errored',
    });
    return false;
  }
  const questionInsert = await createQuestion(insertData);
  if (questionInsert) {
    logger.info(`Insert Question main:: ${Process_id} question bulk data inserted successfully to main table `);
    return true;
  }
  logger.error(`Insert Question main:: ${Process_id} question bulk data error in inserting to main table`);
  await updateProcess(Process_id, {
    error_status: 'errored',
    error_message: 'Question question bulk data error in inserting',
    status: 'errored',
  });
  return false;
};

const insertStageDataToQuestionSetTable = async () => {
  const getAllQuestionSetStage = await questionSetStageMetaData({ process_id: Process_id });
  const insertData = await formatQuestionSetStageData(getAllQuestionSetStage);
  const questionSetInsert = await createQuestionSet(insertData);
  if (questionSetInsert) {
    logger.info(`Insert Question set main:: ${Process_id} question set bulk data inserted successfully to main table `);
    return true;
  }
  logger.error(`Insert Question set main:: ${Process_id} question set data error in inserting to main table`);
  await updateProcess(Process_id, {
    error_status: 'errored',
    error_message: ' content bulk data error in inserting',
    status: 'errored',
  });
  return false;
};

const insertStageDataToContentTable = async () => {
  const getAllContentStage = await contentStageMetaData({ process_id: Process_id });
  const insertData = await formatContentStageData(getAllContentStage);
  const contentInsert = await createContent(insertData);
  if (contentInsert) {
    logger.info(`Insert Content main:: ${Process_id} content bulk data inserted successfully to main table `);
    return true;
  }
  logger.error(`Insert Content main:: ${Process_id} content bulk data error in inserting to main table`);
  await updateProcess(Process_id, {
    error_status: 'errored',
    error_message: ' content bulk data error in inserting',
    status: 'errored',
  });
  return false;
};

const getCSVTemplateHeader = async (entryName: string) => {
  const templateZipEntries = await fetchAndExtractZipEntries('template');
  const templateFileContent = templateZipEntries
    .find((t) => t.entryName === entryName)
    ?.getData()
    .toString('utf8');
  if (!templateFileContent) {
    await updateProcess(Process_id, { error_status: 'invalid_template', error_message: `Template for '${entryName}' not found.`, status: 'failed' });
    logger.error(`Template:: The file '${entryName}' does not match the expected CSV format.`);
    return [];
  }
  const [templateHeader] = templateFileContent.split('\n').map((row) => row.split(','));
  logger.info('Template:: template header extracted.');
  return templateHeader;
};

const getCSVHeaderAndRow = (csvEntries: any) => {
  const [header, ...rows] = csvEntries
    .getData()
    .toString('utf8')
    .split('\n')
    .map((row: string) => row.split(','))
    .filter((row: string[]) => row.some((cell) => cell.trim() !== ''));
  logger.info('Row/Header:: header and rows are extracted');
  return { header, rows };
};

const validHeader = async (entryName: string, header: any, templateHeader: any): Promise<boolean> => {
  if (header.length !== templateHeader.length) {
    await updateProcess(Process_id, { error_status: 'invalid_header_length', error_message: `CSV file contains more/less fields compared to the template.`, status: 'failed' });
    logger.error(`Header Validate:: CSV file contains more/less fields compared to the template.`);
    return false;
  }

  const validHeader = templateHeader.every((col: any, i: number) => col === header[i]);
  if (!validHeader) {
    await updateProcess(Process_id, { error_status: 'invalid_column_name', error_message: `The file '${entryName}' does not match the expected CSV format.`, status: 'failed' });
    logger.error(`Header validate:: The file '${entryName}' does not match the expected CSV format.`);
    return false;
  }
  logger.info(`Header validate:: ${entryName} contain valid header`);
  return true;
};

const processRow = (rows: string[][], header: string[]) => {
  return rows.map((row) =>
    row.reduce(
      (acc, cell, index) => {
        const headerName = header[index].replace(/\r/g, '');
        const cellValue = cell.includes('#') ? cell.split('#').map((v: string) => v.trim()) : cell.replace(/\r/g, '');
        if (headerName.startsWith('mcq') || headerName.startsWith('fib') || headerName.startsWith('grid') || headerName.includes('n1') || headerName.includes('n2')) {
          acc.body = acc.body || {};
          acc.body[headerName] = cellValue;
        } else if (headerName.includes('L2_skill') || headerName.includes('L3_skill') || headerName.includes('sub_skill')) {
          acc[headerName] = typeof cellValue === 'string' ? [cellValue] : cellValue;
        } else if (headerName.includes('media')) {
          acc.media_files = acc.media_files || [];
          if (cellValue) acc.media_files.push(cellValue);
        } else if (headerName.includes('QSID')) {
          acc['question_set_id'] = cellValue;
        } else if (headerName.includes('QID')) {
          acc['question_id'] = cellValue;
        } else if (headerName.includes('sequence') || headerName.includes('benchmark_time')) {
          acc[headerName] = Number(cellValue);
        } else if (headerName.includes('sub_skill_x+x')) {
          acc['sub_skill_xx'] = cellValue;
        } else if (headerName.includes('sub_skill_x+0')) {
          acc['sub_skill_x0'] = cellValue;
        } else if (headerName.includes('is_atomic')) {
          acc['is_atomic'] = cellValue.toLocaleString().toLowerCase() === 'true';
        } else if (headerName.includes('instruction_media')) {
          acc['is_atomic'] = cellValue;
        } else if (headerName.includes('instruction_text')) {
          acc['is_atomic'] = cellValue;
        } else {
          acc[headerName] = cellValue;
        }
        acc.process_id = Process_id;
        return acc;
      },
      {} as Record<string, any>,
    ),
  );
};

const ProcessStageDataQuestion = (questionsData: any) => {
  const fieldMapping: any = {
    'Grid-1_add': [...grid1AddFields, 'grid1_pre_fills_top', 'grid1_pre_fills_result'],
    'Grid-1_sub': [...grid1SubFields, 'grid1_pre_fills_top', 'grid1_pre_fills_result'],
    'Grid-1_multiple': [...grid1MultipleFields, 'grid1_multiply_intermediate_steps_prefills', 'grid1_pre_fills_result'],
    'Grid-1_division': [...grid1DivFields, 'grid1_pre_fills_remainder', 'grid1_pre_fills_quotient', 'grid1_div_intermediate_steps_prefills'],
    'Grid-2': [...grid2Fields, 'grid2_pre_fills_n1', 'grid2_pre_fills_n2'],
    mcq: mcqFields,
    fib: fibFields,
  };
  questionsData.forEach((question: any) => {
    const questionType = question.question_type === 'Grid-1' ? `${question.question_type}_${question.L1_skill}` : question.question_type;
    const relevantFields = fieldMapping[questionType];
    const filteredBody: any = {};
    relevantFields.forEach((field: any) => {
      if (question.body[field] !== undefined) {
        filteredBody[field] = question.body[field];
      }
    });
    question.body = filteredBody;
  });
  return questionsData;
};

const formatQuestionSetStageData = async (stageData: any[]) => {
  const { boards, classes, skills, tenants, subSkills, repositories } = await preloadData();
  subSkills;
  const transformedData = stageData.map((obj) => {
    const transferData = {
      identifier: uuid.v4(),
      question_set_id: obj.question_set_id,
      content_id: obj.content_id ?? '',
      instruction_text: obj.instruction_text ?? '',
      sequence: obj.sequence,
      title: { en: obj.title || obj.question_text },
      description: { en: obj.description },
      tenant: tenants.find((tenant: any) => tenant.name.en === 'Ekstep'),
      repository: repositories.find((repository: any) => repository.name === obj.repository_name),
      taxonomy: {
        board: boards.find((board: any) => board.name.en === obj.board),
        class: classes.find((Class: any) => Class.name.en === obj.class),
        l1_skill: skills.find((skill: any) => skill.type == obj.L1_skill),
        l2_skill: obj.L2_skill.map((skill: string) => skills.find((Skill: any) => Skill.type === skill)),
        l3_skill: obj.L3_skill.map((skill: string) => skills.find((Skill: any) => Skill.type === skill)),
      },
      sub_skills: obj.sub_skills,
      purpose: obj.purpose,
      is_atomic: obj.is_atomic,
      gradient: obj.gradient,
      group_name: obj.group_name,
      status: 'draft',
      created_by: 1,
      is_active: true,
    };
    return Object.fromEntries(Object.entries(transferData).filter(([_, v]) => v !== undefined));
  });
  logger.info('Data transfer:: staging Data transferred as per original format');
  return transformedData;
};

const formatContentStageData = async (stageData: any[]) => {
  const { boards, classes, skills, tenants, subSkills, repositories } = await preloadData();

  subSkills;
  const transformedData = stageData.map((obj) => {
    const transferData = {
      identifier: uuid.v4(),
      content_id: obj.content_id,
      name: { en: obj.title || obj.question_text },
      description: { en: obj.description },
      tenant: tenants.find((tenant: any) => tenant.name.en === 'Ekstep'),
      repository: repositories.find((repository: any) => repository.name.en === obj.repository_name),
      taxonomy: {
        board: boards.find((board: any) => board.name.en === obj.board),
        class: classes.find((Class: any) => Class.name.en === obj.class),
        l1_skill: skills.find((skill: any) => skill.type == obj.L1_skill),
        l2_skill: obj.L2_skill.map((skill: string) => skills.find((Skill: any) => Skill.type === skill)),
        l3_skill: obj.L3_skill.map((skill: string) => skills.find((Skill: any) => Skill.type === skill)),
      },
      sub_skills: obj.sub_skills,
      gradient: obj.gradient,
      status: 'draft',
      media: obj.media_files,
      created_by: 1,
      is_active: true,
    };
    return Object.fromEntries(Object.entries(transferData).filter(([_, v]) => v !== undefined));
  });
  logger.info('Data transfer:: staging Data transferred as per original format');
  return transformedData;
};

const formatQuestionStageData = async (stageData: any[]) => {
  const { boards, classes, skills, tenants, subSkills, repositories } = await preloadData();
  subSkills;
  const transformedData = stageData.map((obj) => {
    const {
      grid_fib_n1 = null,
      grid_fib_n2 = null,
      mcq_option_1 = null,
      mcq_option_2 = null,
      mcq_option_3 = null,
      mcq_option_4 = null,
      mcq_option_5 = null,
      mcq_option_6 = null,
      mcq_correct_options = null,
      sub_skill_carry = null,
      sub_skill_procedural = null,
      sub_skill_xx = null,
      sub_skill_x0 = null,
    } = obj.body || {};
    const transferData = {
      identifier: uuid.v4(),
      question_id: obj.question_id,
      question_set_id: obj.question_set_id,
      question_type: obj.question_type,
      operation: obj.L1_skill,
      hints: obj.hint,
      sequence: obj.sequence,
      name: { en: obj.title || obj.question_text },
      description: { en: obj.description },
      tenant: tenants.find((tenant: any) => tenant.name.en === 'Ekstep'),
      repository: repositories.find((repository: any) => repository.name.en === obj.repository_name),
      taxonomy: {
        board: boards.find((board: any) => board.name.en === obj.board),
        class: classes.find((Class: any) => Class.name.en === obj.class),
        l1_skill: skills.find((skill: any) => skill.type == obj.L1_skill),
        l2_skill: obj.L2_skill.map((skill: string) => skills.find((Skill: any) => Skill.type === skill)),
        l3_skill: obj.L3_skill.map((skill: string) => skills.find((Skill: any) => Skill.type === skill)),
      },
      sub_skills: obj.sub_skills,
      question_body: {
        numbers: [grid_fib_n1, grid_fib_n2],
        options: obj.type === 'mcq' ? [mcq_option_1, mcq_option_2, mcq_option_3, mcq_option_4, mcq_option_5, mcq_option_6] : undefined,
        correct_option: obj.type === 'mcq' ? mcq_correct_options : undefined,
        answers: getAnswer(obj.L1_skill, grid_fib_n1, grid_fib_n2, obj.question_type),
        wrong_answer: convertWrongAnswerSubSkills({ sub_skill_carry, sub_skill_procedural, sub_skill_xx, sub_skill_x0 }),
      },
      benchmark_time: obj.benchmark_time,
      status: 'draft',
      media: obj.media_files,
      created_by: 1,
      is_active: true,
    };
    return Object.fromEntries(Object.entries(transferData).filter(([_, v]) => v !== undefined));
  });
  logger.info('Data transfer:: staging Data transferred as per original format');
  return transformedData;
};

const getAnswer = (skill: string, num1: string, num2: string, type: string) => {
  switch (skill) {
    case 'multiple':
      return multiplyWithSteps(num1, num2, type);
    case 'division':
      return divideWithSteps(Number(num2), Number(num1), type);
    case 'add':
      logger.info('Add:: got a value for addition  numbers');
      return Number(num1) + Number(num2);
    case 'sub':
      logger.info('sub:: got a value for subtraction  numbers');
      return Number(num1) - Number(num2);
    default:
      return undefined;
  }
};

const convertWrongAnswerSubSkills = (inputData: any) => {
  const wrongAnswers = [];

  for (const [key, value] of Object.entries(inputData)) {
    if (_.isEmpty(value)) {
      continue;
    }
    const numbers = (value as number[]).map(Number).filter((n: any) => !isNaN(n) && n !== 0);
    if (numbers.length > 0) {
      wrongAnswers.push({
        value: numbers,
        sub_skill_id: 1,
        subskillname: key,
      });
    }
  }
  logger.info('Wrong answer:: wrong answer mapped to sub skills');
  return wrongAnswers;
};

const multiplyWithSteps = (num1: string, num2: string, type: string) => {
  const n1 = Number(num1);
  const n2 = Number(num2);
  if (type === 'Grid-1') {
    const num2Str = num2.toString();
    const num2Length = num2Str.length;
    let intermediateStep = '';
    let runningTotal = 0;
    for (let i = 0; i < num2Length; i++) {
      const placeValue = parseInt(num2Str[num2Length - 1 - i]) * Math.pow(10, i);
      const product = n1 * placeValue;
      intermediateStep += product;
      runningTotal += product;
    }
    return {
      intermediateStep: intermediateStep,
      result: runningTotal,
    };
  }

  return { answer: n1 * n2 };
};

const divideWithSteps = (dividend: number, divisor: number, type: string) => {
  if (type == 'Grid-1') {
    if (divisor === 0) {
      throw new Error('Division by zero is not allowed.');
    }

    const steps = [];
    const quotient = Math.floor(dividend / divisor);
    let remainder = dividend;
    while (remainder >= divisor) {
      const currentStep = Math.floor(remainder / divisor) * divisor;
      steps.push(currentStep);
      remainder -= currentStep;
    }
    return {
      steps: steps,
      quotient: quotient,
      remainder: remainder,
    };
  }
  return { answer: dividend / divisor };
};

const preloadData = async () => {
  const [boards, classes, skills, subSkills, tenants, repositories] = await Promise.all([getBoards(), getClasses(), getSkills(), getSubSkills(), getTenants(), getRepository()]);
  logger.info('Preloaded:: pre loading metadata from table.');
  return {
    boards,
    classes,
    skills,
    tenants,
    subSkills,
    repositories,
  };
};

const convertToCSV = async (jsonData: any, fileName: string) => {
  const json2csvParser = new Parser();
  const csv = json2csvParser.parse(jsonData);
  const uploadMediaFile = await uploadCsvFile(csv, `${Process_id}/${fileName}.csv`);
  logger.info(`CSV:: csv file created from staging data for ${fileName}`);
  return uploadMediaFile;
};

const fetchAndExtractZipEntries = async (folderName: string): Promise<AdmZip.IZipEntry[]> => {
  try {
    let bulkUploadFolder;
    if (folderName === 'upload') {
      bulkUploadFolder = await getFolderData(`upload/${Process_id}/${FILENAME}`);
    } else {
      bulkUploadFolder = await getFolderData(`template/${FILENAME}`);
    }
    const buffer = (await streamToBuffer(bulkUploadFolder)) as Buffer;
    const zip = new AdmZip(buffer);
    logger.info('Cloud Process:: converted stream to zip entries');
    return zip.getEntries();
  } catch (error) {
    const code = _.get(error, 'code', 'UPLOAD_QUESTION_CRON');
    const errorMsg = error instanceof Error ? error.message : 'Error in the validation process,please re-upload the zip file for the new process';
    logger.error({ errorMsg, code });
    return [];
  }
};

const streamToBuffer = (stream: any) => {
  return new Promise((resolve, reject) => {
    const chunks: any = [];
    stream.on('data', (chunk: any) => chunks.push(chunk));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', reject);
  });
};
