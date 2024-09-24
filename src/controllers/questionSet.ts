import logger from '../utils/logger';
import * as _ from 'lodash';
import * as uuid from 'uuid';
import { updateProcess } from '../services/process';
import { createQuestionSetStage, getAllStageQuestionSet, questionSetStageMetaData, updateQuestionStageSet } from '../services/questionSetStage';
import { QuestionSetStage } from '../models/questionSetStage';
import { createQuestionSet } from '../services/questionSet';
import { getCSVTemplateHeader, getCSVHeaderAndRow, validHeader, processRow, convertToCSV, preloadData } from '../services/util';

const tenantName = 'Ekstep';
let Process_id: string;

export const handleQuestionSetCsv = async (questionSetsCsv: object[], process_id: string) => {
  Process_id = process_id;
  let questionSetsData: object[] = [];
  if (questionSetsCsv.length === 0) {
    logger.error(`${Process_id} Question set data validation resulted in empty data.`);
    return false;
  }
  for (const questionSet of questionSetsCsv) {
    const validAddQuestionData = await validateCSVQuestionSetHeaderRow(questionSet);
    if (!validAddQuestionData) {
      logger.error('error while progressing data');
      return false;
    }
    questionSetsData = questionSetsData.concat(validAddQuestionData);
    if (questionSetsData.length === 0) {
      logger.error('Error while processing the question set csv data');
      return false;
    }
  }
  logger.info('Insert question Set Stage::Questions set Data ready for bulk insert');
  await insertBulkQuestionSetStage(questionSetsData);
  return true;
};
const validateCSVQuestionSetHeaderRow = async (questionSetEntry: any) => {
  const templateHeader = await getCSVTemplateHeader(questionSetEntry.entryName);
  const { header, rows } = getCSVHeaderAndRow(questionSetEntry);
  if (!templateHeader && !header && !rows) {
    logger.error('Question Set Row/header:: Template header, header, or rows are missing');
    return [];
  }

  const isValidHeader = validHeader(questionSetEntry.entryName, header, templateHeader);
  if (!isValidHeader) {
    logger.error('Question Set Row/header:: Header validation failed');
    return [];
  }

  logger.info(`Question Set Row/header:: Row and Header mapping process started for ${Process_id} `);
  const validData = await questionSetRowHeaderProcess(rows, header);
  return validData;
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
    return [];
  }
  logger.info('Insert Question Set Stage::Question sets  Data ready for bulk insert');
  return processData;
};

const insertBulkQuestionSetStage = async (questionSetData: any) => {
  const stageProcessData = await insertQuestionSetStage(questionSetData);
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
  await validateQuestionSetStage();
};

const validateQuestionSetStage = async () => {
  const stageProcessValidData = await validateQuestionSetStageData();
  if (!stageProcessValidData) {
    logger.error(`Validate question set Stage:: ${Process_id} staging data are invalid`);
    await updateProcess(Process_id, {
      error_status: 'staging_validation_error',
      error_message: `Validate question set Stage:: ${Process_id} staging data are invalid`,
      status: 'errored',
    });
  }
  logger.info(`Upload Cloud::Staging Data ready for upload in cloud`);
  await uploadQuestionSetStage(stageProcessValidData);
};

const uploadQuestionSetStage = async (isValid: boolean) => {
  const processStatus = isValid ? 'validated' : 'errored';
  const questionSets = await getAllStageQuestionSet();
  if (questionSets.error) {
    logger.error('unexpected error occurred while get all stage data');
    await updateProcess(Process_id, {
      error_status: 'unexpected_error',
      error_message: `unexpected error occurred while get all stage data`,
      status: 'errored',
    });
    return false;
  }
  await updateProcess(Process_id, { fileName: 'questionSet.csv', status: processStatus });
  const uploadQuestionSet = await convertToCSV(questionSets, 'questionSets');
  if (!uploadQuestionSet) {
    logger.error('Upload Cloud::Unexpected error occurred while upload to cloud');
    return false;
  }
  logger.info('Question set:: all the data are validated successfully and uploaded to cloud for reference');
  if (!isValid) return false;

  logger.info(`Question set Bulk Insert::${Process_id} is Ready for inserting bulk upload to question`);
  await insertQuestionSetMain();
};

const insertQuestionSetMain = async () => {
  const insertToMainQuestionSet = await stageDataToQuestionSet();
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

const insertQuestionSetStage = async (insertData: object[]) => {
  const questionSetStage = await createQuestionSetStage(insertData);
  if (questionSetStage.error) {
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

const validateQuestionSetStageData = async () => {
  const getAllQuestionSetStage = await questionSetStageMetaData({ process_id: Process_id });
  if (getAllQuestionSetStage.error) {
    logger.error(`Validate Question Set Stage:: ${Process_id} ,th unexpected error .`);
    return false;
  }
  let isValid = true;
  if (_.isEmpty(getAllQuestionSetStage)) {
    logger.info(`Validate Question set Stage:: ${Process_id} ,the staging Data is empty invalid format or errored fields`);
    return false;
  }
  for (const questionSet of getAllQuestionSetStage) {
    const { id, question_set_id, L1_skill } = questionSet;
    const checkRecord = await questionSetStageMetaData({ question_set_id, L1_skill });
    if (checkRecord.error) {
      logger.error(`Validate Question Set Stage:: ${Process_id} ,th unexpected error .`);
      return false;
    }
    if (checkRecord.length > 1) {
      await updateQuestionStageSet(
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
  logger.info(`Validate Question set Stage:: ${Process_id} , the staging Data question set is valid`);
  return isValid;
};

export const stageDataToQuestionSet = async () => {
  const getAllQuestionSetStage = await questionSetStageMetaData({ process_id: Process_id });
  if (getAllQuestionSetStage.error) {
    logger.error(`Insert Question set main:: ${Process_id} ,the unexpected error .`);
    return false;
  }
  const insertData = await formatQuestionSetStageData(getAllQuestionSetStage);
  if (!insertData) {
    await updateProcess(Process_id, {
      error_status: 'process_stage_data',
      error_message: ' Error in formatting staging data to main table.',
      status: 'errored',
    });
    return false;
  }
  const questionSetInsert = await createQuestionSet(insertData);
  if (questionSetInsert.error) {
    logger.error(`Insert Question set main:: ${Process_id} question set data error in inserting to main table`);
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: ' content bulk data error in inserting',
      status: 'errored',
    });
    return false;
  }

  return true;
};

const formatQuestionSetStageData = async (stageData: any[]) => {
  const preload = await preloadData();
  const boards = preload?.boards || [];
  const classes = preload?.classes || [];
  const skills = preload?.skills || [];
  const tenants = preload?.tenants || [];
  const subSkills = preload?.subSkills || [];
  const repositories = preload?.repositories || [];
  const transformedData = stageData.map((obj) => {
    const transferData = {
      identifier: uuid.v4(),
      question_set_id: obj.question_set_id,
      content_id: obj.content_id ?? '',
      instruction_text: obj.instruction_text ?? '',
      sequence: obj.sequence,
      title: { en: obj.title || obj.question_text },
      description: { en: obj.description },
      tenant: tenants.find((tenant: any) => tenant.name === tenantName),
      repository: repositories.find((repository: any) => repository.name === obj.repository_name),
      taxonomy: {
        board: boards.find((board: any) => board.name.en === obj.board),
        class: classes.find((Class: any) => Class.name.en === obj.class),
        l1_skill: skills.find((skill: any) => skill.type == obj.L1_skill),
        l2_skill: obj.L2_skill.map((skill: string) => skills.find((Skill: any) => Skill.type === skill)),
        l3_skill: obj.L3_skill.map((skill: string) => skills.find((Skill: any) => Skill.type === skill)),
      },
      sub_skills: obj.sub_skills.map((subSkill: string) => subSkills.find((sub: any) => sub.name.en === subSkill)),
      purpose: obj.purpose,
      is_atomic: obj.is_atomic,
      gradient: obj.gradient,
      group_name: obj.group_name,
      status: 'draft',
      created_by: 1,
      is_active: true,
    };
    return transferData;
  });
  logger.info('Data transfer:: staging Data transferred as per original format');
  return transformedData;
};
