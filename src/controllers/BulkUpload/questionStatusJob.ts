import logger from '../../utils/logger';
import * as _ from 'lodash';
import { getFolderMetaData, getFolderData, uploadFile } from '../../services/awsService';
import { getProcessByMetaData, updateProcess } from '../../services/process';
import path from 'path';
import AdmZip from 'adm-zip';
import { appConfiguration } from '../../config';
import { contentStageMetaData, createContentSage } from '../../services/contentStage';
import { createQuestionStage, questionStageMetaData, updateQuestionStage } from '../../services/questionStage';
import { createQuestionSetStage, questionSetStageMetaData } from '../../services/questionSetStage';
import { createContent } from '../../services/content ';
import { QuestionStage } from '../../models/questionStage';
import { QuestionSetStage } from '../../models/questionSetStage';
import { ContentStage } from '../../models/contentStage';
import { createQuestion } from '../../services/question';

const { csvFileName, fileUploadInterval, reCheckProcessInterval, grid1AddFields, grid1DivFields, grid1MultipleFields, grid1SubFields, grid2Fields, mcqFields, fibFields } = appConfiguration;
let FILENAME: string;
let Process_id: string;

export const scheduleJob = async () => {
  await handleFailedProcess();
  const processesInfo = await getProcessByMetaData({ status: 'open' });
  const { getAllProcess } = processesInfo;
  try {
    for (const process of getAllProcess) {
      const { process_id, fileName, created_at } = process;
      logger.info(`Starting bulk upload job for process id :${process_id}.`);
      Process_id = process_id;
      const IsStaleProcess = await markStaleProcessesAsErrored(created_at);
      if (IsStaleProcess) continue;
      FILENAME = fileName;
      const folderPath = `upload/${process_id}`;
      const bulkUploadMetadata = await getFolderMetaData(folderPath);
      logger.info(`Starting bulk upload folder validation for process id :${process_id}.`);
      await validateZipFile(bulkUploadMetadata);
    }
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation.Re upload file for new process';
    await updateProcess(Process_id, { status: 'errored', error_status: 'errored', error_message: 'Error during upload validation.Re upload file for new process' });
    logger.error(errorMsg);
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
    logger.error('The uploaded zip folder is empty, please ensure a valid upload file.');
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
    logger.info({ message: `process reopened for ${process_id}` });
    const timeDifference = Math.floor((Date.now() - created_at.getTime()) / (1000 * 60 * 60));
    if (timeDifference > reCheckProcessInterval) {
      const isSuccessStageProcess = await checkStagingProcess();
      if (!isSuccessStageProcess) {
        await updateProcess(Process_id, { status: 'errored', error_status: 'errored', error_message: 'The csv Data is invalid format or errored fields.Re upload file for new process' });
        logger.info({ message: `The csv Data is invalid format or errored fields for process id: ${process_id}` });
      } else {
        await updateProcess(Process_id, { status: 'completed' });
        logger.info({ message: `${Process_id} Process completed successfully.` });
      }
    }
  }
};

const checkStagingProcess = async () => {
  const getAllQuestionStage = await questionStageMetaData({ process_id: Process_id });
  if (_.isEmpty(getAllQuestionStage.questions)) {
    logger.info(`process id : ${Process_id} ,the csv Data is invalid format or errored fields`);
    return false;
  } else {
    const getAllQuestionSetStage = await questionSetStageMetaData({ status: 'success', process_id: Process_id });
    if (_.isEmpty(getAllQuestionSetStage.questions)) {
      logger.info(`process id : ${Process_id} ,the csv Data is invalid format or errored fields`);
      return false;
    } else {
      const getAllContentStage = await contentStageMetaData({ status: 'success', process_id: Process_id });
      if (_.isEmpty(getAllContentStage.questions)) {
        logger.info(`process id : ${Process_id} ,the csv Data is invalid format or errored fields`);
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
    logger.error('The uploaded file is an unsupported format, please upload all CSV files inside a ZIP file.');
  } else {
    await updateProcess(Process_id, { status: 'progress', updated_by: 1 });
    logger.info(`Bulk upload folder found and valid zip for process id:${Process_id}`);
    await validateCSVFilesFormatInZip();
  }
};

const validateCSVFilesFormatInZip = async (): Promise<boolean> => {
  try {
    const ZipEntries = await fetchAndExtractZipEntries('upload');
    const mediaZipEntries = ZipEntries.filter((e) => !e.entryName.endsWith('.csv'));
    const csvZipEntries = ZipEntries.filter((e) => e.entryName.endsWith('.csv'));
    let isValidType,
      isValidName = true;
    for (const entry of csvZipEntries) {
      if (entry.isDirectory && entry.entryName.includes('.csv')) {
        await updateProcess(Process_id, {
          error_status: 'unsupported_folder_type',
          error_message: 'The uploaded ZIP folder contains valid files with valid format. Follow the template format.',
          status: 'failed',
        });
        logger.error('The uploaded ZIP folder contains valid files with valid format. Follow the template format.');
        isValidType = false;
      }
      if (!csvFileName.includes(entry.entryName)) {
        await updateProcess(Process_id, {
          error_status: 'unsupported_folder_type',
          error_message: `The uploaded file '${entry.entryName}' is not a valid file name.`,
          status: 'failed',
        });
        logger.error(`The uploaded file '${entry.entryName}' is not a valid file name.`);
        isValidName = false;
      }
    }
    logger.info('every csv file have valid file name');
    if (isValidName && isValidType) {
      const validCSV = await handleCSVEntries(csvZipEntries, mediaZipEntries);
      return validCSV;
    }
    return false;
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation,please re upload the zip file for the new process';
    logger.error(errorMsg);
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: errorMsg,
      status: 'errored',
    });
    return false;
  }
};

const handleCSVEntries = async (csvFilesEntries: any, mediaEntires: any): Promise<any> => {
  try {
    for (const entry of csvFilesEntries) {
      const checkKey = entry.entryName.split('_')[1];
      switch (checkKey) {
        case 'question.csv': {
          const isValidQuestionData = await validateQuestionCsvData(entry, mediaEntires);
          if (!isValidQuestionData) continue;
          break;
        }
        case 'questionSet.csv': {
          const isValidQuestionSetData = await validateQuestionSetCsvData(entry);
          if (!isValidQuestionSetData) continue;
          break;
        }
        case 'content.csv': {
          const isValidQuestionContentData = await validateContentCsvData(entry, mediaEntires);
          if (!isValidQuestionContentData) continue;
          break;
        }
        default: {
          await updateProcess(Process_id, {
            error_status: 'unsupported_sheet',
            error_message: `Unsupported sheet in file '${entry.entryName}'.`,
            status: 'failed',
          });
          logger.error(`Unsupported sheet in file '${entry.entryName}'.`);
          continue;
        }
      }
    }
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation,please re upload the zip file for the new process';
    logger.error(errorMsg);
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: errorMsg,
      status: 'errored',
    });
    return false;
  }
};

const validateQuestionCsvData = async (questionEntry: any, mediaEntries: any) => {
  try {
    const templateHeader = await getCSVTemplateHeader(questionEntry.entryName);
    const { header, rows } = getCSVHeaderAndRow(questionEntry);
    if (!templateHeader && !header && !rows) {
      logger.error('Question::  Template header, header, or rows are missing');
      return false;
    }

    const isValidHeader = await validHeader(questionEntry.entryName, header, templateHeader);
    if (!isValidHeader) {
      logger.error('Question ::  Header validation failed');
      return false;
    }

    const processData = processRow(rows, header);
    if (!processData || processData.length === 0) {
      logger.error('Question ::  Row processing failed or returned empty data');
      await updateProcess(Process_id, {
        error_status: 'process_error',
        error_message: 'Question :: Row processing failed or returned empty data',
        status: 'errored',
      });
      return false;
    }
    logger.info('Question :: header and row process successfully');

    const processData2 = ProcessStageDataQuestion(processData);
    if (!processData2 || processData2.length === 0) {
      logger.error('Question ::  Stage 2 data processing failed or returned empty data');
      await updateProcess(Process_id, {
        error_status: 'process_stage_error',
        error_message: 'Question :: Stage 2 data processing failed or returned empty data',
        status: 'errored',
      });
      return false;
    }
    logger.info('Question :: header and row process-2 successfully');

    const updatedProcessData = await validMedia(processData2, mediaEntries, 'question');
    if (!updatedProcessData || updatedProcessData.length === 0) {
      logger.error('Question ::  Media validation or update failed');
      await updateProcess(Process_id, {
        error_status: 'media_validation_error',
        error_message: 'Question :: Media validation or update failed',
        status: 'errored',
      });
      return false;
    }
    logger.info('Question :: Media inserted and updated in the process data');

    const stageProcessData = await insertProcessDataToQuestionStaging(updatedProcessData);
    if (!stageProcessData) {
      logger.error('Question:: Failed to insert process data into staging');
      await updateProcess(Process_id, {
        error_status: 'staging_insert_error',
        error_message: 'Question::Failed to insert process data into staging',
        status: 'errored',
      });
      return false;
    }

    const stageProcessValidData = await validateQuestionStageData();
    if (!stageProcessValidData) {
      logger.error(`Question:: ${Process_id} staging data are invalid`);
      await updateProcess(Process_id, {
        error_status: 'staging_validation_error',
        error_message: `Question::${Process_id} staging data are invalid`,
        status: 'errored',
      });
      return false;
    }

    const insertToMainQuestionSet = await insertStageDataToQuestionTable();
    if (!insertToMainQuestionSet) {
      logger.error(`${Process_id} staging data are invalid for main question insert`);
      await updateProcess(Process_id, {
        error_status: 'main_insert_error',
        error_message: `Question::${Process_id} staging data are invalid for main question insert`,
        status: 'errored',
      });
      return false;
    }

    await updateProcess(Process_id, { status: 'completed' });
    await QuestionStage.truncate({ restartIdentity: true });
    logger.info(`Question bulk upload completed successfully for Process ID: ${Process_id}`);
    return true;
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation,please re upload the zip file for the new process';
    logger.error(errorMsg);
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: errorMsg,
      status: 'errored',
    });
    return false;
  }
};

const validateQuestionSetCsvData = async (questionSetEntry: any) => {
  try {
    const templateHeader = await getCSVTemplateHeader(questionSetEntry.entryName);
    const { header, rows } = getCSVHeaderAndRow(questionSetEntry);
    if (!templateHeader && !header && !rows) {
      logger.error('Question set:: Template header, header, or rows are missing');
      return false;
    }

    const isValidHeader = await validHeader(questionSetEntry.entryName, header, templateHeader);
    if (!isValidHeader) {
      logger.error('Question set:: Header validation failed');
      return false;
    }

    const processData = processRow(rows, header);
    if (!processData || processData.length === 0) {
      logger.error('Question set:: Row processing failed or returned empty data');
      await updateProcess(Process_id, {
        error_status: 'process_error',
        error_message: 'Question set::Row processing failed or returned empty data',
        status: 'errored',
      });
      return false;
    }
    logger.info('Question set::header and row process successfully');

    const stageProcessData = await insertProcessDataToQuestionSetStaging(processData);
    if (!stageProcessData) {
      logger.error('Question set::  Failed to insert process data into staging');
      await updateProcess(Process_id, {
        error_status: 'staging_insert_error',
        error_message: 'Question set:: Failed to insert process data into staging',
        status: 'errored',
      });
      return false;
    }

    const stageProcessValidData = await validateQuestionSetStageData();
    if (!stageProcessValidData) {
      logger.error(`Question set:: ${Process_id} staging data are invalid`);
      await updateProcess(Process_id, {
        error_status: 'staging_validation_error',
        error_message: `Question set::${Process_id} staging data are invalid`,
        status: 'errored',
      });
      return false;
    }

    const insertToMainQuestionSet = await insertStageDataToQuestionSetTable();
    if (!insertToMainQuestionSet) {
      logger.error(`${Process_id} staging data are invalid for main question set insert`);
      await updateProcess(Process_id, {
        error_status: 'main_insert_error',
        error_message: `Question::${Process_id} staging data are invalid for main question set insert`,
        status: 'errored',
      });
      return false;
    }

    await updateProcess(Process_id, { status: 'completed' });
    await QuestionSetStage.truncate({ restartIdentity: true });
    logger.info(`Question set bulk upload completed successfully for Process ID: ${Process_id}`);
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation,please re upload the zip file for the new process';
    logger.error(errorMsg);
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: errorMsg,
      status: 'errored',
    });
  }
};

const validateContentCsvData = async (contentEntry: any, mediaEntries: any) => {
  try {
    const templateHeader = await getCSVTemplateHeader(contentEntry.entryName);
    const { header, rows } = getCSVHeaderAndRow(contentEntry);
    if (!templateHeader && !header && !rows) {
      logger.error('Content::  Template header, header, or rows are missing');
      return false;
    }

    const isValidHeader = await validHeader(contentEntry.entryName, header, templateHeader);
    if (!isValidHeader) {
      logger.error('Content::  Header validation failed');
      return false;
    }

    const processData = processRow(rows, header);
    if (!processData || processData.length === 0) {
      logger.error('Content::  Row processing failed or returned empty data');
      await updateProcess(Process_id, {
        error_status: 'process_error',
        error_message: 'Content:: Row processing failed or returned empty data',
        status: 'errored',
      });
      return false;
    }
    logger.info('Content:: header and row process successfully');

    const updatedProcessData = await validMedia(processData, mediaEntries, 'content');
    if (!updatedProcessData || updatedProcessData.length === 0) {
      logger.error('Content::  Media validation or update failed');
      await updateProcess(Process_id, {
        error_status: 'media_validation_error',
        error_message: 'Content:: Media validation or update failed',
        status: 'errored',
      });
      return false;
    }
    logger.info('Content:: Media inserted and updated in the process data');

    const stageProcessData = await insertProcessDataToContentStaging(updatedProcessData);
    if (!stageProcessData) {
      logger.error('Content:: Failed to insert process data into staging');
      await updateProcess(Process_id, {
        error_status: 'staging_insert_error',
        error_message: 'Content::Failed to insert process data into staging',
        status: 'errored',
      });
      return false;
    }

    const stageProcessValidData = await validateContentStageData();
    if (!stageProcessValidData) {
      logger.error(`Content:: ${Process_id} staging data are invalid`);
      await updateProcess(Process_id, {
        error_status: 'staging_validation_error',
        error_message: `Content::${Process_id} staging data are invalid`,
        status: 'errored',
      });
      return false;
    }

    const insertToMainQuestionSet = await insertStageDataToContentTable();
    if (!insertToMainQuestionSet) {
      logger.error(`${Process_id} staging data are invalid for main question insert`);
      await updateProcess(Process_id, {
        error_status: 'main_insert_error',
        error_message: `Content::${Process_id} staging data are invalid for main question insert`,
        status: 'errored',
      });
      return false;
    }

    await updateProcess(Process_id, { status: 'completed' });
    await ContentStage.truncate({ restartIdentity: true });
    logger.info(`Content::bulk upload completed successfully for Process ID: ${Process_id}`);
    return true;
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : 'Error during upload validation,please re upload the zip file for the new process';
    logger.error(errorMsg);
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: errorMsg,
      status: 'errored',
    });
  }
};

const insertProcessDataToQuestionStaging = async (insertData: object[]) => {
  const contentStageProcessData = await createQuestionStage(insertData);
  if (contentStageProcessData) {
    logger.info({ message: `${Process_id} question bulk data inserted successfully to staging table` });
    return true;
  } else {
    logger.error({ message: `${Process_id} question bulk data error in inserting` });
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: ' question bulk data error in inserting',
      status: 'errored',
    });
    return false;
  }
};

const insertProcessDataToQuestionSetStaging = async (insertData: object[]) => {
  const contentStageProcessData = await createQuestionSetStage(insertData);
  if (contentStageProcessData) {
    logger.info({ message: `${Process_id} question set bulk data inserted successfully to staging table ` });
    return true;
  } else {
    logger.error({ message: `${Process_id} question set  bulk data error in inserting` });
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: ' question set bulk data error in inserting',
      status: 'errored',
    });
    return false;
  }
};

const insertProcessDataToContentStaging = async (insertData: object[]) => {
  const contentStageProcessData = await createContentSage(insertData);
  if (contentStageProcessData) {
    logger.info({ message: `${Process_id} content bulk data inserted successfully to staging table ` });
    return true;
  } else {
    logger.error({ message: `${Process_id} content bulk data error in inserting` });
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: ' content bulk data error in inserting',
      status: 'errored',
    });
    return false;
  }
};

const validateQuestionStageData = async () => {
  const getAllQuestionStage = await questionStageMetaData({ process_id: Process_id });
  let isUnique, isValid;
  if (_.isEmpty(getAllQuestionStage.questions)) {
    logger.info(`process id : ${Process_id} ,the csv Data is invalid format or errored fields`);
    return false;
  } else {
    for (const question of getAllQuestionStage.questions) {
      const {
        dataValues: { id, question_id, question_set_id, question_type, L1_skill, body },
      } = question;
      const checkRecord = await questionStageMetaData({ question_id, question_set_id, L1_skill, question_type });
      if (checkRecord.question.length > 1) {
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
            error_info: `Missing required data: ${requiredFields.join(', ')}`,
          },
        );
        isValid = false;
      } else {
        isValid = true;
      }
    }
    logger.info(`process id : ${Process_id} , the staging Data question is valid`);
    return isUnique && isValid;
  }
};

const validateQuestionSetStageData = async () => {
  const getAllQuestionSetStage = await questionSetStageMetaData({ process_id: Process_id });
  let isValid;
  if (_.isEmpty(getAllQuestionSetStage.questionSet)) {
    logger.info(`process id : ${Process_id} ,the staging Data is empty invalid format or errored fields`);
    return false;
  } else {
    for (const question of getAllQuestionSetStage.questionSet) {
      const {
        dataValues: { id, question_set_id, L1_skill },
      } = question;
      const checkRecord = await questionSetStageMetaData({ question_set_id, L1_skill });
      if (checkRecord.questionSet.length > 1) {
        await updateQuestionStage(
          { id },
          {
            status: 'errored',
            error_info: 'Duplicate question_set_id found.',
          },
        );
        isValid = false;
      } else {
        isValid = true;
      }
    }
    logger.info(`process id : ${Process_id} , the staging Data question set is valid`);
    return isValid;
  }
};

const validateContentStageData = async () => {
  const getAllContentStage = await contentStageMetaData({ process_id: Process_id });
  let isValid;
  if (_.isEmpty(getAllContentStage.contents)) {
    logger.info(`process id : ${Process_id} ,the csv Data is invalid format or errored fields`);
    return false;
  } else {
    for (const question of getAllContentStage.contents) {
      const {
        dataValues: { id, content_id, L1_skill },
      } = question;
      const checkRecord = await contentStageMetaData({ content_id, L1_skill });
      if (checkRecord.contents.length > 1) {
        await updateQuestionStage(
          { id },
          {
            status: 'errored',
            error_info: 'Duplicate content_id found.',
          },
        );
        isValid = false;
      } else {
        isValid = true;
      }
    }
    logger.info(`process id : ${Process_id} , the staging Data content is valid`);
    return isValid;
  }
};

const insertStageDataToQuestionTable = async () => {
  const getAllQuestionStage = await questionStageMetaData({ process_id: Process_id });
  const insertData = getAllQuestionStage.questions;
  const contentInsert = await createQuestion(insertData);
  if (contentInsert) {
    logger.info({ message: `${Process_id} content bulk data inserted successfully to main table ` });
    return true;
  } else {
    logger.error({ message: `${Process_id} content bulk data error in inserting to main table` });
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: ' content bulk data error in inserting', //check
      status: 'errored',
    });
    return false;
  }
};

const insertStageDataToQuestionSetTable = async () => {
  const getAllQuestionSetStage = await questionSetStageMetaData({ process_id: Process_id });
  const insertData = getAllQuestionSetStage.questionSets;
  const contentInsert = await createContent(insertData);
  if (contentInsert) {
    logger.info({ message: `${Process_id} question set bulk data inserted successfully to main table ` });
    return true;
  } else {
    logger.error({ message: `${Process_id} question set data error in inserting to main table` });
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: ' content bulk data error in inserting', //check
      status: 'errored',
    });
    return false;
  }
};

const insertStageDataToContentTable = async () => {
  const getAllContentStage = await contentStageMetaData({ process_id: Process_id });
  const insertData = getAllContentStage.contents;
  const contentInsert = await createContent(insertData);
  if (contentInsert) {
    logger.info({ message: `${Process_id} content bulk data inserted successfully to main table ` });
    return true;
  } else {
    logger.error({ message: `${Process_id} content bulk data error in inserting to main table` });
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: ' content bulk data error in inserting', //check
      status: 'errored',
    });
    return false;
  }
};

const processRow = (rows: string[][], header: string[]) => {
  return rows.map((row) =>
    row.reduce(
      (acc, cell, index) => {
        const headerName = header[index].replace(/\r/g, '');
        const cellValue = cell.includes('#') ? cell.split('#').map((v: string) => v.trim()) : cell.replace(/\r/g, '');
        if (headerName.startsWith('mcq') || headerName.startsWith('fib') || headerName.startsWith('grid')) {
          acc.body = acc.body || {};
          acc.body[headerName] = cellValue;
        } else if (headerName.includes('L2_skill') || headerName.includes('L3_skill') || headerName.includes('sub_skill')) {
          acc[headerName] = typeof cellValue === 'string' ? [cellValue] : cellValue;
        } else if (headerName.includes('media')) {
          acc.media_files = acc.media_files || [];
          if (cellValue) acc.media_files.push(cellValue);
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
    'Grid-1_add': grid1AddFields.push('grid1_pre_fills_top', 'grid1_pre_fills_result'),
    'Gid-1_sub': grid1SubFields.push('grid1_pre_fills_top', 'grid1_pre_fills_result'),
    'Grid-1_multiple': grid1MultipleFields.push('grid1_multiply_intermediate_steps_prefills', 'grid1_pre_fills_result'),
    'Grid-1_division': grid1DivFields.push('grid1_pre_fills_remainder', 'grid1_pre_fills_quotient', 'grid1_div_intermediate_steps_prefills'),
    'Grid-2': grid2Fields.push('grid2_pre_fills_n1', 'grid2_pre_fills_n2'),
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

const validMedia = async (processedCSVData: any, mediaEntries: any[], type: string) => {
  const mediaCsvData = await Promise.all(
    processedCSVData.map(async (p: any) => {
      const mediaFiles = await Promise.all(
        p.media_files.map(async (o: string) => {
          const foundMedia = mediaEntries.slice(1).find((media: any) => {
            return media.entryName.split('/')[1] === o;
          });
          if (foundMedia) {
            const mediaData = await uploadFile(foundMedia, type);
            return mediaData;
          }
          return null;
        }),
      );
      p.media_files = mediaFiles.filter((media: any) => media !== null);
      return p;
    }),
  );
  return mediaCsvData;
};

const getCSVTemplateHeader = async (entryName: string) => {
  const templateZipEntries = await fetchAndExtractZipEntries('template');
  const templateFileContent = templateZipEntries
    .find((t) => t.entryName === entryName)
    ?.getData()
    .toString('utf8');
  if (!templateFileContent) {
    await updateProcess(Process_id, { error_status: 'invalid_template', error_message: `Template for '${entryName}' not found.`, status: 'failed' });
    logger.error(`The file '${entryName}' does not match the expected CSV format.`);
    return [];
  }
  const [templateHeader] = templateFileContent.split('\n').map((row) => row.split(','));
  logger.info('template header extracted');
  return templateHeader;
};

const getCSVHeaderAndRow = (csvEntries: any) => {
  const [header, ...rows] = csvEntries
    .getData()
    .toString('utf8')
    .split('\n')
    .map((row: string) => row.split(','))
    .filter((row: string[]) => row.some((cell) => cell.trim() !== ''));
  logger.info('header and rows are extracted');
  return { header, rows };
};

const validHeader = async (entryName: string, header: any, templateHeader: any): Promise<boolean> => {
  if (header.length !== templateHeader.length) {
    await updateProcess(Process_id, { error_status: 'invalid_header_length', error_message: `CSV file contains more/less fields compared to the template.`, status: 'failed' });
    logger.error(`CSV file contains more/less fields compared to the template.`);
    return false;
  }

  const validHeader = templateHeader.every((col: any, i: number) => col === header[i]);
  if (!validHeader) {
    await updateProcess(Process_id, { error_status: 'invalid_column_name', error_message: `The file '${entryName}' does not match the expected CSV format.`, status: 'failed' });
    logger.error(`The file '${entryName}' does not match the expected CSV format.`);
    return false;
  }
  logger.info(`${entryName} contain valid header`);
  return true;
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
    logger.info('converted stream to zip entries');
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
