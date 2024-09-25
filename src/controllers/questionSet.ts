import logger from '../utils/logger';
import * as _ from 'lodash';
import * as uuid from 'uuid';
import { updateProcess } from '../services/process';
import { createQuestionSetStage, getAllStageQuestionSet, questionSetStageMetaData, updateQuestionStageSet } from '../services/questionSetStage';
import { QuestionSetStage } from '../models/questionSetStage';
import { createQuestionSet } from '../services/questionSet';
import { getCSVTemplateHeader, getCSVHeaderAndRow, validHeader, processRow, convertToCSV, preloadData } from '../services/util';
import { Status } from '../enums/status';

let Process_id: string;

export const handleQuestionSetCsv = async (questionSetsCsv: object[], process_id: string) => {
  Process_id = process_id;
  let questionSetsData: object[] = [];
  if (questionSetsCsv.length === 0) {
    logger.error(`${Process_id} Question set data validation resulted in empty data.`);
    return {
      error: { errStatus: 'Empty', errMsg: 'empty question set data found' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  for (const questionSet of questionSetsCsv) {
    const validQuestionSetData = await validateCSVQuestionSetHeaderRow(questionSet);
    if (!validQuestionSetData.result.isValid) {
      logger.error('error while progressing data');
      return validQuestionSetData;
    }
    const {
      result: { data },
    } = validQuestionSetData;
    const validData = questionSetRowHeaderProcess(data?.rows, data?.header);
    if (!validData.result.isValid) {
      logger.error('error while processing data');
      return {
        error: { errStatus: 'in valid row', errMsg: 'in valid row are header found for question' },
        result: {
          isValid: false,
          data: null,
        },
      };
    }
    const { result } = validData;
    questionSetsData = questionSetsData.concat(result.data);
    if (questionSetsData.length === 0) {
      logger.error('Error while processing the question set csv data');
      return {
        error: { errStatus: 'Empty', errMsg: 'Error question set csv data' },
        result: {
          isValid: false,
          data: null,
        },
      };
    }
  }
  logger.info('Insert question Set Stage::Questions set Data ready for bulk insert');
  const createQuestionSetStage = await insertBulkQuestionSetStage(questionSetsData);
  if (!createQuestionSetStage.result.isValid) {
    logger.error('Error while creating stage question table');
    return createQuestionSetStage;
  }
  const validateQuestionSet = await validateQuestionSetStage();
  if (!validateQuestionSet.result.isValid) {
    logger.error('Error while validating stage question set data');
    const uploadQuestionSet = await uploadQuestionSetStage();
    if (!uploadQuestionSet.result.isValid) return uploadQuestionSet;
    return validateQuestionSet;
  }
  const mainQuestionSet = await insertQuestionSetMain();
  return mainQuestionSet;
};

const validateCSVQuestionSetHeaderRow = async (questionSetEntry: any) => {
  const templateHeader = await getCSVTemplateHeader(questionSetEntry.entryName);
  if (!templateHeader.result.isValid) {
    return {
      error: { errStatus: 'Template missing', errMsg: 'template missing' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  const questionSetRowHeader = getCSVHeaderAndRow(questionSetEntry);
  if (!questionSetRowHeader.result.isValid) {
    logger.error('Question Set Row/header:: Template header, header, or rows are missing');
    return {
      error: { errStatus: 'Missing row/header', errMsg: 'Question Row/header::Template header, or rows are missing' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }

  const {
    result: {
      data: { header },
    },
  } = questionSetRowHeader;
  const isValidHeader = validHeader(questionSetEntry.entryName, header, templateHeader.result.data);
  if (!isValidHeader.result.isValid) {
    logger.error('Question Set Row/header:: Header validation failed');
    return isValidHeader;
  }

  logger.info(`Question Set Row/header:: Row and Header mapping process started for ${Process_id} `);
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: questionSetRowHeader.result.data,
    },
  };
};

const questionSetRowHeaderProcess = (rows: any, header: any) => {
  const processData = processRow(rows, header);
  if (!processData || processData.length === 0) {
    logger.error('Question Set Row/header:: Row processing failed or returned empty data');
    return {
      error: { errStatus: 'process_error', errMsg: 'Row processing failed or returned empty data' },
      result: {
        isValid: false,
        data: processData,
      },
    };
  }
  logger.info('Insert Question Set Stage::Question sets  Data ready for bulk insert');
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: processData,
    },
  };
};

const insertBulkQuestionSetStage = async (questionSetData: any) => {
  const stageProcessData = await insertQuestionSetStage(questionSetData);
  if (!stageProcessData.result.isValid) {
    logger.error('Insert Question Set Stage::  Failed to insert process data into staging');
    return stageProcessData;
  }

  logger.info(`Validate question set Stage::question sets Data ready for validation process`);
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: null,
    },
  };
};

const validateQuestionSetStage = async () => {
  const validQuestionSetStage = await validateQuestionSetStageData();
  if (!validQuestionSetStage.result.isValid) {
    logger.error(`Validate question set Stage:: ${Process_id} staging data are invalid`);
  }
  logger.info(`Upload Cloud::Staging Data ready for upload in cloud`);
  return validQuestionSetStage;
};

const uploadQuestionSetStage = async () => {
  const questionSets = await getAllStageQuestionSet();
  if (questionSets.error) {
    logger.error('unexpected error occurred while get all stage data');
    return {
      error: { errStatus: 'unexpected_error', errMsg: 'unexpected error occurred while get all stage data' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  await updateProcess(Process_id, { question_set_error_file_name: 'questionSet.csv', status: Status.ERROR });
  const uploadQuestionSet = await convertToCSV(questionSets, 'questionSets');
  if (!uploadQuestionSet) {
    logger.error('Upload Cloud::Unexpected error occurred while upload to cloud');
    return {
      error: { errStatus: 'unexpected_error', errMsg: 'unexpected error occurred while upload to cloud' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  logger.info('Question set:: all the data are validated successfully and uploaded to cloud for reference');

  logger.info(`Question set Bulk Insert::${Process_id} is Ready for inserting bulk upload to question`);
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: null,
    },
  };
};

const insertQuestionSetMain = async () => {
  const insertToMainQuestionSet = await stageDataToQuestionSet();
  if (!insertToMainQuestionSet.result.isValid) {
    logger.error(`Question set bulk insert:: ${Process_id} staging data are invalid for main question set insert`);
    return insertToMainQuestionSet;
  }

  await QuestionSetStage.truncate({ restartIdentity: true });
  logger.info(`Question set bulk upload:: completed successfully and question_sets.csv file upload to cloud for Process ID: ${Process_id}`);
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: null,
    },
  };
};

const insertQuestionSetStage = async (insertData: object[]) => {
  const questionSetStage = await createQuestionSetStage(insertData);
  if (questionSetStage.error) {
    logger.error(`Insert Question SetStaging:: ${Process_id} question set  bulk data error in inserting`);
    return {
      error: { errStatus: 'errored', errMsg: 'question set bulk data error in inserting' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  logger.info(`Insert Question Set Staging:: ${Process_id} question set bulk data inserted successfully to staging table `);
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: null,
    },
  };
};

const validateQuestionSetStageData = async () => {
  const getAllQuestionSetStage = await questionSetStageMetaData({ process_id: Process_id });
  if (getAllQuestionSetStage.error) {
    logger.error(`Validate Question Set Stage:: ${Process_id} unexpected error  .`);
    return {
      error: { errStatus: 'error', errMsg: `question Set Stage data  unexpected error .` },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  if (_.isEmpty(getAllQuestionSetStage)) {
    logger.info(`Validate Question set Stage:: ${Process_id} ,staging Data is empty invalid format or errored fields`);
    return {
      error: { errStatus: 'error', errMsg: `question Set Stage data unexpected error .` },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  let isValid = true;
  for (const questionSet of getAllQuestionSetStage) {
    const { id, question_set_id, L1_skill } = questionSet;
    const checkRecord = await questionSetStageMetaData({ question_set_id, L1_skill });
    if (checkRecord.error) {
      logger.error(`Validate Question Set Stage:: ${Process_id} ,unexpected error .`);
      return {
        error: { errStatus: 'error', errMsg: `question Set Stage data unexpected error .` },
        result: {
          isValid: false,
          data: null,
        },
      };
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
    }
  }
  logger.info(`Validate Question set Stage:: ${Process_id} , the staging Data question set is valid`);
  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: isValid,
      data: null,
    },
  };
};

export const stageDataToQuestionSet = async () => {
  const getAllQuestionSetStage = await questionSetStageMetaData({ process_id: Process_id });
  if (getAllQuestionSetStage.error) {
    logger.error(`Insert Question set main:: ${Process_id} ,the unexpected error .`);
    return {
      error: { errStatus: 'errored', errMsg: 'question set bulk data error in inserting' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  const insertData = await formatQuestionSetStageData(getAllQuestionSetStage);
  if (insertData.length === 0) {
    return {
      error: { errStatus: 'process_stage_data', errMsg: 'Error in formatting staging data question set to main table.' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  const questionSetInsert = await createQuestionSet(insertData);
  if (questionSetInsert.error) {
    logger.error(`Insert Question set main:: ${Process_id} question set data error in inserting to main table`);
    return {
      error: { errStatus: 'errored', errMsg: ' question set bulk data error in inserting' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }

  return {
    error: { errStatus: null, errMsg: null },
    result: {
      isValid: true,
      data: null,
    },
  };
};

const formatQuestionSetStageData = async (stageData: any[]) => {
  const { boards, classes, skills, subSkills, repositories } = await preloadData();

  const transformedData = stageData.map((obj) => {
    const transferData = {
      identifier: uuid.v4(),
      question_set_id: obj.question_set_id,
      content_id: [obj.content_id ?? ''],
      instruction_text: obj.instruction_text ?? '',
      sequence: obj.sequence,
      title: { en: obj.title || obj.question_text },
      description: { en: obj.description },
      tenant: '',
      repository: repositories.find((repository: any) => repository.name === obj.repository_name),
      taxonomy: {
        board: boards.find((board: any) => board.name.en === obj.board),
        class: classes.find((Class: any) => Class.name.en === obj.class),
        l1_skill: skills.find((skill: any) => skill.name.en == obj.L1_skill),
        l2_skill: obj.L2_skill.map((skill: string) => skills.find((Skill: any) => Skill.name.en === skill)),
        l3_skill: obj.L3_skill.map((skill: string) => skills.find((Skill: any) => Skill.name.en === skill)),
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
