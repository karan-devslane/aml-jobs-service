import logger from '../utils/logger';
import * as _ from 'lodash';
import * as uuid from 'uuid';
import { uploadMediaFile } from '../services/awsService';
import { updateProcess } from '../services/process';
import { createQuestionStage, getAllStageQuestion, questionStageMetaData, updateQuestionStage } from '../services/questionStage';
import { QuestionStage } from '../models/questionStage';
import { appConfiguration } from '../config';
import { createQuestion } from '../services/question';
import { getCSVTemplateHeader, getCSVHeaderAndRow, validHeader, processRow, convertToCSV, preloadData } from '../services/util';

const tenantName = 'Ekstep';
let mediaEntries: any[];
let Process_id: string;

const { grid1AddFields, grid1DivFields, grid1MultipleFields, grid1SubFields, grid2Fields, mcqFields, fibFields } = appConfiguration;

export const handleQuestionCsv = async (questionsCsv: object[], media: any, process_id: string) => {
  Process_id = process_id;
  mediaEntries = media;
  let questionData: object[] = [];
  if (questionsCsv.length === 0) {
    logger.error(`${Process_id} Question data validation resulted in empty data.`);
    return {
      error: { errStatus: 'Empty', errMsg: 'empty question data found' },
      result: {
        isValid: false,
      },
    };
  }
  for (const questions of questionsCsv) {
    const validQuestionData = await validateCSVQuestionHeaderRow(questions);
    if (!validQuestionData.result.isValid) {
      logger.error('error while progressing data');
      return {
        error: { errStatus: 'in valid row', errMsg: 'in valid row are header found for question' },
        result: {
          isValid: false,
        },
      };
    }
    const {
      result: { data },
    } = validQuestionData;
    const validData = await questionRowHeaderProcess(data?.rows, data?.header);
    if (!validData.result.isValid) {
      logger.error('error while processing data');
      return {
        error: { errStatus: 'in valid row', errMsg: 'in valid row are header found for question' },
        result: {
          isValid: false,
        },
      };
    }
    const { result } = validData;
    questionData = questionData.concat(result.data);
    if (questionData.length === 0) {
      logger.error('Error while processing the question csv data');
      return {
        error: { errStatus: 'Empty', errMsg: 'empty question data found' },
        result: {
          isValid: false,
        },
      };
    }
  }
  logger.info('Insert question Stage::Questions Data ready for bulk insert');
  const createQuestionStage = await insertBulkQuestionStage(questionData);
  if (!createQuestionStage.result.isValid) {
    logger.error('Error while creating stage question table');
    return createQuestionStage;
  }

  const validateQuestion = await validateQuestionStage();
  await uploadQuestionStage(validateQuestion.result.isValid);
  if (!validateQuestion.result.isValid) {
    logger.error('Error while validating stage question table');
    return {
      error: { errStatus: 'error', errMsg: 'error while validating stage data for question table' },
      result: {
        isValid: false,
      },
    };
  }

  const questionMedia = await questionMediaProcess();
  if (!questionMedia.result.isValid) {
    logger.error('Error while validating stage question table');
    return {
      error: { errStatus: 'error', errMsg: 'error while validating stage data for question table' },
      result: {
        isValid: false,
      },
    };
  }
  const mainQuestion = await insertQuestionMain();
  return mainQuestion;
};
const validateCSVQuestionHeaderRow = async (questionEntry: any) => {
  const templateHeader = await getCSVTemplateHeader(questionEntry.entryName);
  if (!templateHeader.result.isValid) {
    return {
      error: { errStatus: 'Template missing', errMsg: 'template missing' },
      result: {
        isValid: false,
      },
    };
  }
  const questionRowHeader = getCSVHeaderAndRow(questionEntry);
  if (!questionRowHeader.result.isValid) {
    logger.error('Question Row/header::Template header, header, or rows are missing');
    return {
      error: { errStatus: 'Missing row/header', errMsg: 'Question Row/header::Template header, header, or rows are missing' },
      result: {
        isValid: false,
      },
    };
  }
  const {
    result: {
      data: { header },
    },
  } = questionRowHeader;
  const isValidHeader = validHeader(questionEntry.entryName, header, templateHeader.result.data);
  if (!isValidHeader.result.isValid) {
    logger.error('Question Row/header::Header validation failed');
    return {
      error: { errStatus: 'Missing row/header', errMsg: 'Question Row/header::Template header, header, or rows are missing' },
      result: {
        isValid: false,
      },
    };
  }
  logger.info(`Question Row/header::Row and Header mapping process started for ${Process_id} `);
  return {
    error: null,
    result: {
      isValid: true,
      data: questionRowHeader.result.data,
    },
  };
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
    return {
      error: { errStatus: 'process_error', errMsg: 'Question Row/header:: Row processing failed or returned empty data' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  logger.info('Question Row/header:: header and row process successfully and process 2 started');
  const updatedProcessData = processQuestionStage(processData);
  if (!updatedProcessData || updatedProcessData.length === 0) {
    logger.error('Question Row/header:: Stage 2 data processing failed or returned empty data');
    await updateProcess(Process_id, {
      error_status: 'process_stage_error',
      error_message: 'Data processing failed or returned empty data',
      status: 'errored',
    });
    return {
      error: { errStatus: 'process_stage_error', errMsg: 'Data processing failed or returned empty data' },
      result: {
        isValid: false,
      },
    };
  }
  logger.info('Insert question Stage::Questions Data ready for bulk insert');
  return {
    error: null,
    result: {
      isValid: true,
      data: updatedProcessData,
    },
  };
};

const insertBulkQuestionStage = async (insertData: any) => {
  const questionStage = await insertQuestionStage(insertData);
  if (!questionStage.result.isValid) {
    logger.error('Insert question stage:: Failed to insert process data into staging.');
    await updateProcess(Process_id, {
      error_status: 'staging_insert_error',
      error_message: 'Failed to insert process data into staging',
      status: 'errored',
    });
    return {
      error: { errStatus: 'staging_insert_error', errMsg: 'Failed to insert process data into staging' },
      result: {
        isValid: false,
      },
    };
  }

  logger.info(`Validate question Stage::Staged questions Data ready for validation`);
  return {
    error: null,
    result: {
      isValid: true,
      data: null,
    },
  };
};

const validateQuestionStage = async () => {
  const stageProcessValidData = await validateQuestionStageData();
  if (!stageProcessValidData.result.isValid) {
    logger.error(`Question:: ${Process_id} staging data are invalid`);
    await updateProcess(Process_id, {
      error_status: 'staging_validation_error',
      error_message: `Question staging data are invalid.correct the error and re upload new csv for fresh process`,
      status: 'errored',
    });
  }

  logger.info(`Upload Cloud::Staging Data ready for upload in cloud`);
  return stageProcessValidData;
};

const uploadQuestionStage = async (isValid: boolean) => {
  const processStatus = isValid ? 'validated' : 'errored';
  const getQuestions = await getAllStageQuestion();
  if (getQuestions.error) {
    logger.error('unexpected error occurred while get all stage data');
    await updateProcess(Process_id, {
      error_status: 'unexpected_error',
      error_message: `unexpected error occurred while get all stage data`,
      status: 'errored',
    });
    return {
      error: { errStatus: 'unexpected_error', errMsg: 'unexpected error occurred while get all stage data' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  await updateProcess(Process_id, { fileName: 'questions.csv', status: processStatus });
  const uploadQuestion = await convertToCSV(getQuestions, 'questions');
  if (!uploadQuestion) {
    logger.error('Upload Cloud::Unexpected error occurred while upload to cloud');
    return {
      error: { errStatus: 'unexpected_error', errMsg: 'unexpected error occurred while upload to cloud' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  logger.info('Question Upload Cloud::All the question are validated and uploaded in the cloud for reference');
  logger.info(`Question Media upload:: ${Process_id} question Stage data is ready for upload media `);
  return {
    error: null,
    result: {
      isValid: true,
      data: null,
    },
  };
};

const questionMediaProcess = async () => {
  try {
    const getQuestions = await getAllStageQuestion();
    if (getQuestions.error) {
      logger.error('unexpected error occurred while get all stage data');
      await updateProcess(Process_id, {
        error_status: 'unexpected_error',
        error_message: `unexpected error occurred while get all stage data`,
        status: 'errored',
      });
      return {
        error: { errStatus: 'unexpected_error', errMsg: 'unexpected error occurred while get all stage data' },
        result: {
          isValid: false,
          data: null,
        },
      };
    }
    for (const question of getQuestions) {
      if (question.media_files?.length > 0) {
        const mediaFiles = await Promise.all(
          question.media_files.map(async (o: string) => {
            const foundMedia = mediaEntries.slice(1).find((media: any) => {
              return media.entryName.split('/')[1] === o;
            });
            if (foundMedia) {
              const mediaData = await uploadMediaFile(foundMedia, 'question');
              if (!mediaData) {
                logger.error(`Media upload failed for ${o}`);
                return null;
              }
              return mediaData;
            }
            return null;
          }),
        );
        if (mediaFiles.every((file: any) => file === null)) {
          logger.warn(`No valid media files found for question ID: ${question.id}`);
          continue;
        }

        const validMediaFiles = mediaFiles.filter((file) => file !== null);
        if (validMediaFiles.length === 0) {
          return {
            error: { errStatus: 'Empty', errMsg: 'No media found for the question' },
            result: {
              isValid: false,
              data: null,
            },
          };
        }
        const updateContent = await updateQuestionStage({ id: question.id }, { media_files: validMediaFiles });
        if (updateContent.error) {
          logger.error('Question Media upload:: Media validation failed');
          await updateProcess(Process_id, {
            error_status: 'media_validation_error',
            error_message: 'Media validation failed',
            status: 'errored',
          });
          throw new Error('error while updating media');
        }
      }
    }

    logger.info('Question Media upload::inserted and updated in the process data');
    logger.info(`Bulk Insert::${Process_id} is Ready for inserting bulk upload to question table`);
    return {
      error: null,
      result: {
        isValid: true,
        data: null,
      },
    };
  } catch (error: any) {
    logger.error(`An error occurred in questionMediaProcess: ${error.message}`);
    await updateProcess(Process_id, {
      error_status: 'process_error',
      error_message: error.message,
      status: 'errored',
    });
    return {
      error: null,
      result: {
        isValid: false,
        data: null,
      },
    };
  }
};

const insertQuestionMain = async () => {
  const insertToMainQuestion = await stageDataToQuestion();
  if (!insertToMainQuestion.result.isValid) {
    logger.error(`Question Bulk Insert:: ${Process_id} staging data are invalid for main question insert`);
    await updateProcess(Process_id, {
      error_status: 'main_insert_error',
      error_message: `Bulk Insert staging data are invalid to format main question insert`,
      status: 'errored',
    });
    return {
      error: { errStatus: 'main_insert_error', errMsg: `Bulk Insert staging data are invalid to format main question insert` },
      result: {
        isValid: false,
        data: null,
      },
    };
  }

  logger.info(`Question Bulk insert:: bulk upload completed  for Process ID: ${Process_id}`);
  await QuestionStage.truncate({ restartIdentity: true });
  logger.info(`Completed:: ${Process_id} Question csv uploaded successfully`);
  return {
    error: null,
    result: {
      isValid: true,
      data: null,
    },
  };
};

const insertQuestionStage = async (insertData: object[]) => {
  const questionStage = await createQuestionStage(insertData);
  if (questionStage.error) {
    logger.error(`Insert Staging:: ${Process_id} question bulk data error in inserting`);
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: 'question bulk data error in inserting',
      status: 'errored',
    });
    return {
      error: { errStatus: 'errored', errMsg: 'question bulk data error in inserting' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  logger.info(`Insert Question Staging:: ${Process_id} question bulk data inserted successfully to staging table`);
  return {
    error: null,
    result: {
      isValid: true,
      data: null,
    },
  };
};

const validateQuestionStageData = async () => {
  const getAllQuestionStage = await questionStageMetaData({ process_id: Process_id });
  if (getAllQuestionStage.error) {
    logger.error(`Validate Question Stage:: ${Process_id} ,th unexpected error .`);
    return {
      error: { errStatus: 'error', errMsg: `Validate Question Stage:: ${Process_id} ,th unexpected error .` },
      result: {
        isValid: false,
      },
    };
  }
  let isUnique = true;
  let isValid = true;
  if (_.isEmpty(getAllQuestionStage)) {
    logger.error(`Validate Question Stage:: ${Process_id} ,the csv Data is invalid format or errored fields`);
    return {
      error: { errStatus: 'error', errMsg: `The csv Data is invalid format or errored fields` },
      result: {
        isValid: false,
      },
    };
  }
  for (const question of getAllQuestionStage) {
    const { id, question_id, question_set_id, question_type, L1_skill, body } = question;
    const checkRecord = await questionStageMetaData({ question_id, question_set_id, L1_skill, question_type });
    if (checkRecord.error) {
      logger.error(`Validate Question Stage:: ${Process_id} ,th unexpected error .`);
      return {
        error: { errStatus: 'error', errMsg: ` The csv Data is invalid format or errored fields` },
        result: {
          isValid: false,
        },
      };
    }
    if (checkRecord.length > 1) {
      await updateQuestionStage(
        { id },
        {
          status: 'errored',
          error_info: 'Duplicate question and question_set_id combination found.',
        },
      );
      isUnique = false;
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
    if (!requiredFields.map((field) => body[field] !== undefined && body[field] !== null)) {
      await updateQuestionStage(
        { id },
        {
          status: 'errored',
          error_info: `Missing required data for type ${question_type},fields are  ${requiredFields.join(', ')}`,
        },
      );
      isValid = false;
    }
  }
  logger.info(`Validate Question Stage::${Process_id} , everything in the Question stage Data valid.`);
  return {
    error: null,
    result: {
      isValid: isUnique && isValid,
    },
  };
};

export const stageDataToQuestion = async () => {
  const getAllQuestionStage = await questionStageMetaData({ process_id: Process_id });
  if (getAllQuestionStage.error) {
    logger.error(`Validate Question Stage:: ${Process_id} ,th unexpected error .`);
    return {
      error: { errStatus: 'errored', errMsg: 'question bulk data error in inserting' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  const insertData = await formatQuestionStageData(getAllQuestionStage);
  if (insertData.length === 0) {
    await updateProcess(Process_id, {
      error_status: 'process_stage_data',
      error_message: ' Error in formatting staging data to main table.',
      status: 'errored',
    });
    return {
      error: { errStatus: 'process_stage_data', errMsg: 'Error in formatting staging data to main table.' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }
  const questionInsert = await createQuestion(insertData);
  if (questionInsert.error) {
    logger.error(`Insert Question main:: ${Process_id} question bulk data error in inserting to main table`);
    await updateProcess(Process_id, {
      error_status: 'errored',
      error_message: 'Question question bulk data error in inserting',
      status: 'errored',
    });
    return {
      error: { errStatus: 'errored', errMsg: 'Question bulk data error in inserting' },
      result: {
        isValid: false,
        data: null,
      },
    };
  }

  return {
    error: null,
    result: {
      isValid: true,
      data: null,
    },
  };
};

const processQuestionStage = (questionsData: any) => {
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

const formatQuestionStageData = async (stageData: any[]) => {
  const { boards, classes, skills, tenants, subSkills, repositories } = await preloadData();
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
      tenant: tenants.find((tenant: any) => tenant.name.en === tenantName),
      repository: repositories.find((repository: any) => repository.name.en === obj.repository_name),
      taxonomy: {
        board: boards.find((board: any) => board.name.en === obj.board),
        class: classes.find((Class: any) => Class.name.en === obj.class),
        l1_skill: skills.find((skill: any) => skill.type == obj.L1_skill),
        l2_skill: obj.L2_skill.map((skill: string) => skills.find((Skill: any) => Skill.type === skill)),
        l3_skill: obj.L3_skill.map((skill: string) => skills.find((Skill: any) => Skill.type === skill)),
      },
      sub_skills: obj.sub_skills.map((subSkill: string) => subSkills.find((sub: any) => sub.name.en === subSkill)),
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
    return transferData;
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
